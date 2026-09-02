# povomatic - distributed POV-Ray rendering on Kubernetes
# Copyright (C) 2026 Jakob Flierl
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from flask import Flask, render_template, send_from_directory, jsonify
from flask_socketio import SocketIO
import psycopg2
import psycopg2.extensions
import os
import shutil
import logging
import select
import signal
import threading
import time

logging.basicConfig(
    level=os.environ.get('LOG_LEVEL', 'INFO').upper(),
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
)
logger = logging.getLogger('povomatic.dashboard')

app = Flask(__name__)
# Threading rather than gevent: every query goes through psycopg2, which is not
# cooperative without psycogreen, so a gevent hub would block on each one.
# The client uses the polling transport, which any threaded WSGI server serves.
CORS_ORIGINS = os.environ.get('CORS_ALLOWED_ORIGINS', '*')
socketio = SocketIO(app, async_mode='threading',
                    cors_allowed_origins=CORS_ORIGINS.split(',') if CORS_ORIGINS != '*' else '*')

PORT = int(os.environ.get('PORT', '5000'))
SERVER_THREADS = int(os.environ.get('SERVER_THREADS', '16'))
# Readiness fails if the stats pipeline has not produced a payload this recently.
READY_MAX_AGE = float(os.environ.get('READY_MAX_AGE', '30'))
DB_HOST = os.environ.get('DB_HOST', 'postgres')
DB_NAME = os.environ.get('DB_NAME', 'povomatic')
DB_USER = os.environ.get('DB_USER', 'povomatic')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'password')
DB_CONNECT_TIMEOUT = int(os.environ.get('DB_CONNECT_TIMEOUT', '5'))
if DB_PASSWORD == 'password':
    logger.warning("DB_PASSWORD is the built-in default; set it from a Secret in production")
# Liveness fails once a stats rebuild has not succeeded in this long, so a pod
# whose background threads have wedged is restarted rather than serving a frozen
# page forever. Much laxer than READY_MAX_AGE, which only pulls it from the Service.
LIVE_MAX_AGE = float(os.environ.get('LIVE_MAX_AGE', '300'))

# Assume /app/output is mounted in the dashboard container too
OUTPUT_DIR = '/app/output'

# How often the browser may be pushed to, and how often the poller rebuilds
# stats. Both were 1s and 2s, which was as fast as the dashboard could go while
# every rebuild opened a fresh connection and rescanned NFS.
PUSH_MIN_INTERVAL = float(os.environ.get('PUSH_MIN_INTERVAL', '0.25'))
POLL_SECONDS = float(os.environ.get('POLL_SECONDS', '0.5'))
# Pause before retrying after a database failure.
RETRY_SECONDS = float(os.environ.get('RETRY_SECONDS', '2.0'))
# How far back the ETA looks when measuring how fast an animation is going.
ETA_WINDOW = float(os.environ.get('ETA_WINDOW', '180'))
# Directory listings are reused for this long, which decouples how often the
# preview image changes from how often the numbers do. Listing a 2540 frame
# directory costs a few milliseconds on an idle NFS server but around 400ms
# while two dozen workers are writing frames to it, so refreshing previews at
# the stats rate would put the scans back in charge of the refresh rate.
DIR_CACHE_TTL = float(os.environ.get('DIR_CACHE_TTL', '3.0'))
# How often the refresher wakes to re-read directories that went stale.
DIR_REFRESH_SECONDS = float(os.environ.get('DIR_REFRESH_SECONDS', '0.5'))

_local = threading.local()

def stats_connection():
    """Connection reused per thread.

    fetch_stats opened a new one on every call, and a connect costs about
    240ms against this cluster, which was the single largest fixed cost in a
    push and put a floor under the refresh rate.
    """
    conn = getattr(_local, 'conn', None)
    if conn is not None and not conn.closed:
        return conn
    conn = get_db_connection()
    _local.conn = conn
    return conn

def drop_stats_connection():
    conn = getattr(_local, 'conn', None)
    _local.conn = None
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass

# Progress samples per animation, for the ETA. Per-frame render time cannot
# serve this: the frame rows are deleted as they succeed, so the average the
# ETA used to read was always empty, and it would ignore how many workers are
# on the job in any case. Measuring how fast frames actually land covers both,
# and reflects the queue being shared between animations.
_eta_history = {}
_eta_lock = threading.Lock()

def eta_seconds(parent_id, done, total):
    """Seconds until an animation finishes at the rate it is currently going."""
    if not total or done >= total:
        return None
    now = time.time()
    with _eta_lock:
        samples = _eta_history.setdefault(parent_id, [])
        # Only a change in count is a sample: stats rebuild several times a
        # second, and identical readings say nothing about the rate.
        if not samples or samples[-1][1] != done:
            samples.append((now, done))
        while len(samples) > 2 and now - samples[0][0] > ETA_WINDOW:
            samples.pop(0)
        if len(samples) < 2:
            return None
        elapsed = now - samples[0][0]
        progressed = done - samples[0][1]
    if elapsed <= 0 or progressed <= 0:
        return None
    return (total - done) * elapsed / progressed

def aggregate_frame_rate(parent_ids):
    """Frames per second summed over every running animation, read from the
    same progress samples the per-job ETA uses. Shared workers move between
    animations, so the whole queue's throughput is the sum of the parts, and a
    combined ETA divides the frames still outstanding by it."""
    now = time.time()
    rate = 0.0
    with _eta_lock:
        for parent_id in parent_ids:
            samples = _eta_history.get(parent_id)
            if not samples or len(samples) < 2:
                continue
            elapsed = now - samples[0][0]
            progressed = samples[-1][1] - samples[0][1]
            if elapsed > 0 and progressed > 0:
                rate += progressed / elapsed
    return rate

_dir_cache = {}
_dir_cache_lock = threading.Lock()
# Job ids a caller wanted a listing for and did not get a fresh one.
_dir_wanted = set()
# When each job's listing was last asked for, used only for pruning.
_dir_seen = {}
# Entries not asked for in this long are dropped, so finished jobs do not
# accumulate in the cache forever.
DIR_CACHE_IDLE = 300

def list_job_dir(job_id):
    """Reads one directory. Blocking, and only ever called by the refresher.

    Sizes are collected for the artifacts only. Frames are the bulk of a job
    directory and nothing needs their size, and stat'ing all of them is what
    made a listing cost a second under write load.
    """
    names, sizes = [], {}
    try:
        for entry in os.scandir(job_dir(job_id)):
            if not entry.is_file():
                continue
            names.append(entry.name)
            if not entry.name.endswith('.png'):
                try:
                    sizes[entry.name] = entry.stat().st_size
                except OSError:
                    sizes[entry.name] = 0
    except (OSError, ValueError):
        return [], {}
    return sorted(names), sizes

def scan_job_dir(job_id):
    """Cached file names for a job. Never touches the filesystem.

    Listing the directory of the animation currently being written to takes
    upwards of a second while two dozen workers write frames into it. Doing
    that on the thread building the stats meant that whenever the cache
    expired, the push it was serving stalled for that long, which is where
    the occasional multi-second gap between updates came from. Callers now
    only ever read the cache and ask the refresher for anything stale.
    """
    now = time.time()
    with _dir_cache_lock:
        _dir_seen[job_id] = now
        hit = _dir_cache.get(job_id)
        if hit is None or now - hit[0] >= DIR_CACHE_TTL:
            _dir_wanted.add(job_id)
        return hit[1] if hit else []

def artifact_sizes(job_id):
    """Cached sizes of a job's non-frame files. Never touches the filesystem."""
    with _dir_cache_lock:
        hit = _dir_cache.get(job_id)
        return hit[2] if hit else {}

def dir_refresh_thread():
    """Keeps the directory cache warm off the push path."""
    while True:
      try:
        with _dir_cache_lock:
            wanted = set(_dir_wanted)
            _dir_wanted.clear()
        for job_id in wanted:
            names, sizes = list_job_dir(job_id)
            with _dir_cache_lock:
                _dir_cache[job_id] = (time.time(), names, sizes)
        # Drop entries nothing has asked about for a while.
        cutoff = time.time() - DIR_CACHE_IDLE
        with _dir_cache_lock:
            for key in [k for k, seen in _dir_seen.items() if seen < cutoff]:
                _dir_seen.pop(key, None)
                _dir_cache.pop(key, None)
        socketio.sleep(DIR_REFRESH_SECONDS)
      except Exception as e:
        logger.error(f"Directory refresh failed, retrying: {e}")
        socketio.sleep(RETRY_SECONDS)

def get_db_connection():
    return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER,
                            password=DB_PASSWORD, connect_timeout=DB_CONNECT_TIMEOUT)

@app.route('/output/<path:filename>')
def serve_output(filename):
    # Frames, gifs and videos never change once written, so let the browser keep
    # them: the gif in particular is re-requested on every stats push otherwise.
    return send_from_directory(OUTPUT_DIR, filename, max_age=86400)

def latest_frame_url(parent_id):
    """URL of the furthest-along frame for a job, or None."""
    frames = [n for n in scan_job_dir(parent_id) if n.endswith('.png')]
    if not frames:
        return None
    # Highest frame number rather than newest mtime: frames are numbered in
    # sequence and finish roughly in order, and this needs no stat call.
    return f"/output/job_{int(parent_id)}/{max(frames)}"

def job_dir(job_id):
    """Output directory for a job. The id is an int, so it cannot escape OUTPUT_DIR."""
    return os.path.join(OUTPUT_DIR, f"job_{int(job_id)}")

def artifact_url(job_id, suffix):
    """URL of the first file with this suffix in the job directory, or None."""
    sizes = artifact_sizes(job_id)
    for name in scan_job_dir(job_id):
        # Size guard: an encode killed part way through leaves a zero byte file
        # behind, which would render as a broken image.
        if name.endswith(suffix) and sizes.get(name, 0) > 0:
            return f"/output/job_{int(job_id)}/{name}"
    return None

def artifact_owner(job):
    """Job whose directory holds the artifacts.

    An encode and its frames write into the parent animation's directory, and
    both carry the parent id in scene_file rather than a scene name.
    """
    if job['type'] in ('ffmpeg', 'animation-frame'):
        try:
            return int(job['scene'])
        except (TypeError, ValueError):
            pass
    return job['id']

def _output_rel(path):
    """A /output URL for an absolute path under OUTPUT_DIR, or None."""
    if not path:
        return None
    rel = os.path.relpath(path, OUTPUT_DIR)
    return None if rel.startswith('..') else f"/output/{rel}"

def encoded_artifact(job, suffix):
    """URL of a finished animation's artifact, from the mp4 path the encoder
    writes into output_path when it completes. This is the moment the encode is
    done, with none of the up-to-a-minute lag of a directory scan waiting out
    the NFS attribute cache. The gif sits beside the mp4."""
    op = job.get('output_path') or ''
    if job['type'] != 'animation' or not op.endswith('.mp4'):
        return None
    return _output_rel(op[:-4] + suffix)

def preview_url(job):
    """Preview for a finished job: its own output for a still, the gif otherwise."""
    if job['type'] == 'still' and job['output_path']:
        return _output_rel(job['output_path'])
    owner = artifact_owner(job)
    # The gif is the whole animation in one image, so it beats a single frame.
    return encoded_artifact(job, '.gif') or artifact_url(owner, '.gif') or latest_frame_url(owner)

def download_url(job):
    """What clicking the preview should fetch: the video for an animation."""
    if job['type'] == 'still':
        return preview_url(job)
    return encoded_artifact(job, '.mp4') or artifact_url(artifact_owner(job), '.mp4') or preview_url(job)

last_stats = None
# When a stats rebuild last succeeded, which is what readiness reports on: it
# exercises the database, the queries and the payload build, rather than just
# proving the process is running.
last_success = 0.0
last_stats_lock = threading.Lock()
last_push_time = 0
push_lock = threading.Lock()

def emit_stats(stats):
    """Emits stats to all connected clients, rate-limited to PUSH_MIN_INTERVAL."""
    global last_push_time
    with push_lock:
        now = time.time()
        if now - last_push_time >= PUSH_MIN_INTERVAL:
            socketio.emit('stats', stats)
            last_push_time = now
            return True
    return False

def fetch_stats():
    """Rebuilds the stats payload, retrying once if the pooled connection died."""
    for attempt in (1, 2):
        try:
            return _fetch_stats(stats_connection())
        except psycopg2.Error:
            drop_stats_connection()
            if attempt == 2:
                raise

def _fetch_stats(conn):
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*), sum(frames) FROM jobs WHERE status = 'pending'")
                queued, total = cur.fetchone()
                # Encodes in flight, listed rather than only counted: an encode
                # of a long animation runs for minutes and was invisible on the
                # page, since its animation has already left the active groups.
                cur.execute("""
                    SELECT e.id, e.scene_file, e.progress, e.node_name, p.scene_file, e.stage
                    FROM jobs e LEFT JOIN jobs p ON p.id = CAST(e.scene_file AS INTEGER)
                    WHERE e.type = 'ffmpeg' AND e.status IN ('pending', 'rendering')
                    ORDER BY e.id
                """)
                encodes = [{'id': r[0], 'parent': r[1], 'progress': r[2],
                            'node': r[3], 'scene': r[4], 'stage': r[5]} for r in cur.fetchall()]
                encoding = len(encodes)
                # Fetch performance stats
                # povray_time on an animation is the render cost summed over every
                # frame, so averaging it across jobs compares whole animations and
                # reads in hours. Dividing by the frame count gives the per frame
                # figure the label promises, and leaves a still unchanged since it
                # has one frame. ffmpeg_time stays per job: an encode is one piece
                # of work, not one per frame.
                cur.execute("""
                    SELECT
                        AVG(per_frame), MIN(per_frame), MAX(per_frame),
                        AVG(ffmpeg_time), MIN(ffmpeg_time), MAX(ffmpeg_time)
                    FROM (
                        SELECT povray_time / NULLIF(frames, 0) AS per_frame, ffmpeg_time
                        FROM jobs
                        WHERE status = 'completed'
                        ORDER BY created_at DESC LIMIT 100
                    ) AS recent_jobs
                """)
                stats = cur.fetchone()
                # Fetch all active (rendering) jobs AND parent 'animation' jobs
                # UNION rather than OR: the OR form could not use an index and
                # scanned the whole table, 215ms against 36k rows, where each
                # branch on its own is an index lookup. Same rows, 1ms.
                cur.execute("""
                    SELECT id, scene_file, type, progress, status, output_path, error_log, frames, frames_rendered, current_frame, povray_time, node_name, stage
                    FROM jobs
                    WHERE status = 'rendering'
                    UNION
                    SELECT id, scene_file, type, progress, status, output_path, error_log, frames, frames_rendered, current_frame, povray_time, node_name, stage
                    FROM jobs
                    WHERE type = 'animation'
                      AND id IN (SELECT DISTINCT CAST(scene_file AS INTEGER) FROM jobs WHERE status = 'rendering' AND type = 'animation-frame')
                """)
                active_jobs = cur.fetchall()
                
                
                # Fetch recent history (completed, failed, canceled, or pending) - last 20
                # animation-frame rows are internal bookkeeping, and a queued
                # animation creates thousands of them at once, which pushed
                # every finished job out of the twenty most recent rows and left
                # the completed panel empty for the whole render.
                cur.execute("SELECT id, scene_file, type, progress, status, output_path, error_log, frames, frames_rendered, current_frame, povray_time, node_name, stage FROM jobs WHERE status != 'rendering' AND type != 'animation-frame' ORDER BY created_at DESC LIMIT 20")
                history_jobs = cur.fetchall()

        def map_job(j):
            return {
                'id': j[0], 'scene': j[1], 'type': j[2], 'progress': j[3], 
                'status': j[4], 'output_path': j[5], 'error_log': j[6], 
                'frames': j[7], 'frames_rendered': j[8], 'current_frame': j[9], 
                'povray_time': float(j[10]) if j[10] is not None else 0.0,
                'node': j[11], 'stage': j[12]
            }

        all_active = [map_job(j) for j in active_jobs]
        history = [map_job(j) for j in history_jobs]
        
        # Group rendering animation-frame jobs by parent ID
        animation_groups = {}
        
        for j in all_active:
            if j['type'] == 'animation-frame':
                parent_id = j['scene']
                if parent_id not in animation_groups:
                    animation_groups[parent_id] = {'frames_rendered': 0, 'total_frames': 0, 'active_frames': []}
                animation_groups[parent_id]['active_frames'].append(j)
            elif j['type'] == 'animation':
                parent_id = str(j['id'])
                if parent_id not in animation_groups:
                    animation_groups[parent_id] = {'frames_rendered': 0, 'total_frames': 0, 'active_frames': []}
                animation_groups[parent_id]['frames_rendered'] = j['frames_rendered']
                animation_groups[parent_id]['total_frames'] = j['frames']
                animation_groups[parent_id]['scene_file'] = j['scene']

        # Live preview of the most recent frame each animation has produced.
        for parent_id, group in animation_groups.items():
            group['preview'] = latest_frame_url(parent_id)
            group['eta_seconds'] = eta_seconds(parent_id, group['frames_rendered'], group['total_frames'])

        # Queue-wide throughput and a single ETA for when everything running
        # now is done: remaining frames over the summed frame rate.
        frame_rate = aggregate_frame_rate(animation_groups)
        remaining_frames = sum(max(0, g['total_frames'] - g['frames_rendered'])
                               for g in animation_groups.values())
        overall_eta = (remaining_frames / frame_rate
                       if frame_rate > 0 and remaining_frames > 0 else None)

        # Forget animations that are no longer running, so the samples do not
        # accumulate for jobs that finished or were deleted.
        with _eta_lock:
            for stale in [k for k in _eta_history if k not in animation_groups]:
                del _eta_history[stale]

        # An encode and its animation describe the same render and resolve to
        # the same gif and mp4, so listing both showed one job twice. The
        # animation row is the one to keep; an encode whose animation is not in
        # the window stays, so the artifacts are still reachable.
        animation_ids = {j['id'] for j in history if j['type'] == 'animation'}
        history = [j for j in history
                   if not (j['type'] == 'ffmpeg' and artifact_owner(j) in animation_ids)]

        for j in history:
            if j['status'] == 'completed':
                j['preview'] = preview_url(j)
                j['download'] = download_url(j)
            else:
                j['preview'] = None
                j['download'] = None

        # Rendering stills have progress to show even though they have no
        # frames to group; surface them alongside the animation groups.
        active_stills = [j for j in all_active if j['type'] == 'still']

        return {
            'queued_jobs': queued or 0,
            'total_frames': total or 0,
            'rendering_workers': len(all_active),
            'encoding_jobs': encoding or 0,
            'encodes': encodes,
            'performance': {
                'povray': {'avg': float(stats[0] or 0), 'min': float(stats[1] or 0), 'max': float(stats[2] or 0)},
                'ffmpeg': {'avg': float(stats[3] or 0), 'min': float(stats[4] or 0), 'max': float(stats[5] or 0)}
            },
            'throughput': {
                'frames_per_min': frame_rate * 60,
                'remaining_frames': remaining_frames,
                'overall_eta_seconds': overall_eta,
            },
            'jobs': all_active + history,
            'animation_groups': animation_groups,
            'active_stills': active_stills
        }
    finally:
        # Deliberately not closed: the connection is reused across rebuilds.
        pass

def listener_thread():
    """Listens for PostgreSQL notifications, reconnecting if the server goes away."""
    global last_stats
    while True:
        try:
            _listen_forever()
        except Exception as e:
            logger.error(f"Notification listener lost its connection, reconnecting: {e}")
            socketio.sleep(RETRY_SECONDS)

def _listen_forever():
    global last_stats
    conn = get_db_connection()
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("LISTEN notify_job_changes")
    logger.info("Listening for database notifications...")
    
    last_notify_time = 0
    while True:
        if select.select([conn], [], [], 5) == ([], [], []):
            continue
        conn.poll()
        
        # Drain all currently pending notifications
        if conn.notifies:
            # Rate limit: only process one batch of notifications every 1 second
            now = time.time()
            if now - last_notify_time < PUSH_MIN_INTERVAL:
                # Clear pending notifications without processing
                while conn.notifies:
                    conn.notifies.pop(0)
                continue
            
            last_notify_time = now
            while conn.notifies:
                notify = conn.notifies.pop(0)
                logger.debug("notification: %s", notify.payload)
            
            current_stats = fetch_stats()
            with last_stats_lock:
                emit_stats(current_stats)
                last_stats = current_stats

def background_thread():
    """Broadcasts stats to clients whenever they change.

    Every failure is caught: an unhandled one kills the thread outright, and
    the dashboard then never updates again until the pod is restarted. A
    database restart used to do exactly that.
    """
    global last_stats
    while True:
        socketio.sleep(POLL_SECONDS)
        try:
            current_stats = fetch_stats()
        except Exception as e:
            logger.error(f"Stats rebuild failed, retrying: {e}")
            drop_stats_connection()
            socketio.sleep(RETRY_SECONDS)
            continue
        global last_success
        last_success = time.time()
        with last_stats_lock:
            if current_stats != last_stats:
                emit_stats(current_stats)
                last_stats = current_stats

@socketio.on('connect')
def handle_connect(auth):
    """Send a client whatever stats we have so the page is never blank on load.

    A failed rebuild here must not reject the connection: the background thread
    will push a fresh payload within a second or two anyway.
    """
    global last_stats
    try:
        current_stats = fetch_stats()
        emit_stats(current_stats)
        with last_stats_lock:
            last_stats = current_stats
    except Exception as e:
        logger.warning("initial stats for new client failed, sending last known: %s", e)
        with last_stats_lock:
            if last_stats is not None:
                socketio.emit('stats', last_stats)

PURGE_CHECK_SECONDS = 5
PURGE_STABLE_CHECKS = 3

def purge_when_idle(job_id, timeout=180):
    """Removes a canceled job's output, and keeps removing it until it stays gone.

    A worker only notices a cancel when it next polls, so frames keep landing
    for some seconds after the click. There is no database signal to wait on:
    cancelling sets those rows to canceled, so nothing is left in the rendering
    state to watch, and a single delete just gets undone by the stragglers.
    Deleting repeatedly until the directory has stayed absent across several
    checks covers the drain without needing to know when it ends.
    """
    target = job_dir(job_id)
    deadline = time.time() + timeout
    stable = 0
    removed = 0
    try:
        while time.time() < deadline and stable < PURGE_STABLE_CHECKS:
            time.sleep(PURGE_CHECK_SECONDS)
            if os.path.isdir(target):
                shutil.rmtree(target, ignore_errors=True)
                removed += 1
                stable = 0
            else:
                stable += 1

        if stable >= PURGE_STABLE_CHECKS:
            logger.info(f"Purged output of canceled job {job_id} after {removed} pass(es): {target}")
        else:
            logger.warning(f"Job {job_id} output kept reappearing for {timeout}s, giving up on {target}")
    except Exception as e:
        logger.error(f"Error purging canceled job {job_id}: {e}")

@app.route('/jobs/<int:job_id>/cancel', methods=['POST'])
def cancel_job(job_id):
    """Cancels a job and, for an animation, its frames and pending encode.

    Workers poll the status of the row they are rendering, so marking the
    children is what actually stops the povray processes already running. The
    output is then purged in the background, once those processes have stopped.
    """
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE jobs SET status = 'canceled' WHERE id = %s AND status IN ('pending', 'rendering')", (job_id,))
                canceled = cur.rowcount
                cur.execute("UPDATE jobs SET status = 'canceled' WHERE scene_file = %s AND type IN ('animation-frame', 'ffmpeg') AND status IN ('pending', 'rendering')", (str(job_id),))
                canceled += cur.rowcount
        logger.info(f"Canceled job {job_id} and its children ({canceled} rows).")
        # Returns immediately: the wait for renders to stop must not block the
        # request, or the dashboard would hang for as long as a frame takes.
        socketio.start_background_task(purge_when_idle, job_id)
        return jsonify({'canceled': canceled, 'purge': 'scheduled'})
    except Exception as e:
        logger.error(f"Error canceling job {job_id}: {e}")
        return jsonify({'error': 'cancel failed'}), 500
    finally:
        conn.close()

@app.route('/jobs/<int:job_id>/delete', methods=['POST'])
def delete_job(job_id):
    """Removes a job, its children, and everything rendered for it."""
    target = job_dir(job_id)
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM jobs WHERE id = %s OR scene_file = %s", (job_id, str(job_id)))
                removed = cur.rowcount
        # Only after the rows are gone, so a failure here cannot leave the job
        # pointing at output that no longer exists.
        if os.path.isdir(target):
            shutil.rmtree(target)
            logger.info(f"Deleted job {job_id}: {removed} rows and {target}")
        else:
            logger.info(f"Deleted job {job_id}: {removed} rows, no output directory")
        return jsonify({'deleted': removed})
    except Exception as e:
        logger.error(f"Error deleting job {job_id}: {e}")
        return jsonify({'error': 'delete failed'}), 500
    finally:
        conn.close()

@app.route('/jobs/<int:job_id>/log')
def job_log(job_id):
    """The tail of what povray is printing for one job, right now.

    Fetched only while a frame tile is hovered, rather than carried in every
    stats push: a thousand characters for each of thirty odd running frames,
    two or three times a second, would be a lot of payload for something
    nobody is usually looking at.
    """
    try:
        conn = stats_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT log_tail, status, progress FROM jobs WHERE id = %s", (job_id,))
            row = cur.fetchone()
        conn.commit()
    except psycopg2.Error as e:
        drop_stats_connection()
        logger.error(f"Error reading log for {job_id}: {e}")
        return jsonify({'error': 'unavailable'}), 503
    if not row:
        return jsonify({'error': 'not found'}), 404
    return jsonify({'log': row[0] or '', 'status': row[1], 'progress': row[2]})

@app.route('/healthz')
def healthz():
    """Liveness: a stats rebuild has succeeded within LIVE_MAX_AGE.

    Fails only when the background threads have been stuck for minutes, which
    they should never be once every loop catches and reconnects; this is the
    backstop that gets such a pod restarted instead of serving a frozen page.
    The grace at startup, before the first rebuild, is LIVE_MAX_AGE itself.
    """
    age = time.time() - last_success if last_success else None
    alive = age is None or age < LIVE_MAX_AGE
    body = {'alive': alive, 'stats_age_seconds': round(age, 1) if age is not None else None}
    return (jsonify(body), 200) if alive else (jsonify(body), 503)

@app.route('/readyz')
def readyz():
    """Readiness: the stats pipeline has produced a payload recently."""
    age = time.time() - last_success if last_success else None
    ready = age is not None and age < READY_MAX_AGE
    body = {'ready': ready, 'stats_age_seconds': round(age, 1) if age is not None else None}
    return (jsonify(body), 200) if ready else (jsonify(body), 503)

@app.errorhandler(Exception)
def handle_unexpected(e):
    """Any unhandled error returns JSON, never a stack trace."""
    from werkzeug.exceptions import HTTPException
    if isinstance(e, HTTPException):
        return jsonify({'error': e.name}), e.code
    logger.exception("unhandled error serving a request")
    return jsonify({'error': 'internal error'}), 500

@app.route('/')
def index():
    return render_template('index.html')

def start_background_tasks():
    # Daemon threads, not socketio.start_background_task: the latter's threads
    # are non-daemon and loop forever, which is what kept the process alive
    # (until SIGKILL) after the server had stopped. As daemons they die with
    # the main thread. They still use socketio.sleep / socketio.emit, both of
    # which are just time.sleep / a thread-safe queue put in threading mode.
    for target in (listener_thread, background_thread, dir_refresh_thread):
        threading.Thread(target=target, name=target.__name__, daemon=True).start()
    logger.info("background tasks started: notifications, stats, directory refresh")

def _handle_sigterm(signum, _frame):
    logger.info("received signal %s, exiting", signum)
    os._exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGTERM, _handle_sigterm)
    start_background_tasks()
    # waitress rather than socketio.run, which falls back to werkzeug's
    # development server and has to be forced past its own refusal to serve in
    # production. flask_socketio wraps app.wsgi_app, so a plain WSGI server
    # handles the polling transport the client uses.
    from waitress import serve
    logger.info("serving on 0.0.0.0:%s with %s threads", PORT, SERVER_THREADS)
    serve(app, host='0.0.0.0', port=PORT, threads=SERVER_THREADS,
          # A socket.io poll is held open until there is something to send.
          channel_timeout=120, ident='povomatic')
    # Reached if waitress returns on its own (SIGINT, or a SIGTERM it handles
    # before our handler). Exit cleanly rather than fall off the end.
    logger.info("server stopped, exiting")
    os._exit(0)
