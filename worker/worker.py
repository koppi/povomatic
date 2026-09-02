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

import os
import time
import psycopg2
import shutil
import subprocess
import re
import logging
import signal
import select
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

current_job_id = None

def handle_term_signal(signum, frame):
    logger.info(f"Received signal {signum}. Cleaning up...")
    global current_job_id
    if current_job_id is not None:
        try:
            conn = get_db_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE jobs SET status = 'pending', progress = 0 WHERE id = %s AND status = 'rendering'", (current_job_id,))
                    conn.commit()
            conn.close()
            logger.info(f"Job {current_job_id} set back to pending.")
        except Exception as e:
            logger.error(f"Error resetting job {current_job_id}: {e}")
    sys.exit(0)

signal.signal(signal.SIGTERM, handle_term_signal)
signal.signal(signal.SIGINT, handle_term_signal)

DB_HOST = os.environ.get('DB_HOST', 'postgres')
DB_NAME = os.environ.get('DB_NAME', 'povray')
DB_USER = os.environ.get('DB_USER', 'povray')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'password')
INPUT_PATH = os.environ.get('INPUT_PATH', '/app/input')
OUTPUT_PATH = os.environ.get('OUTPUT_PATH', '/app/output')
ASSETS_PATH = os.environ.get('ASSETS_PATH', '/app/assets')
# Supplied by the downward API. An encode is pinned to the node that rendered
# the animation's last frame, so its frames are already in that node's cache.
NODE_NAME = os.environ.get('NODE_NAME', '')
# Render threads per povray. Deliberately not lowered when several workers
# share a node: each frame spends roughly half its time in single threaded
# parse and png encoding, and letting the processes oversubscribe is what
# fills those gaps with another frame's parallel render.
POVRAY_THREADS = os.environ.get('POVRAY_THREADS', '4')

# povray resolves #include and image_map paths against the working directory
# and its library paths, not against the scene file. Both the scene directory
# and the shared asset library have to be passed explicitly.
LIBRARY_ARGS = [f"+L{INPUT_PATH}", f"+L{ASSETS_PATH}"]

# A worker refreshes its claim every HEARTBEAT_SECONDS while rendering. If a
# claim goes unrefreshed for CLAIM_TIMEOUT the worker is presumed dead and
# another may take the job over, so the gap between them is the number of
# heartbeats that must be missed before a live render is stolen.
HEARTBEAT_SECONDS = 30
CLAIM_TIMEOUT = '10 minutes'
# How long a render may stay silent before the loop wakes anyway. Also bounds
# how long a cancel takes to stop a running povray.
POLL_INTERVAL = 5
# How often a running job's output tail is written back, and how much is kept.
# Every line would be a write per line of povray progress from every worker.
LOG_PUSH_SECONDS = 2.0
LOG_TAIL_CHARS = 1200
# How long a worker reuses its answer to "which animation needs workers".
FAIRNESS_TTL = 2.0
# Target length of the preview gif, whatever the animation's frame count.
GIF_SECONDS = 10
# Share of an encode's progress bar given to the video pass.
MP4_SHARE = 80
# AMD render node for hardware H.264 encoding. Absent unless a GPU is attached
# to the pod; the mp4 pass falls back to libx264 when it is.
VAAPI_DEVICE = os.environ.get("VAAPI_DEVICE", "/dev/dri/renderD128")
# An encode worker claims only 'ffmpeg' jobs. It runs in a small deployment that
# holds an amd.com/gpu so the H.264 pass can use the APU's fixed function
# encoder, while the render fleet stays GPU free and keeps two povray workers a
# node. A render worker (the default) never claims an encode.
ENCODE_ONLY = os.environ.get("ENCODE_ONLY", "").lower() in ("1", "true", "yes")

# Regex to find progress
POV_PROGRESS_RE = re.compile(r'Frame\s+(\d+)\s+of\s+(\d+)')
POV_PERCENT_RE = re.compile(r'(\d+)%')
# povray announces its phases but reports a percentage only while rendering, so
# a frame that is parsing sits at a flat 0 with nothing to say why. There is no
# parse percentage to be had: even a scene with megabytes of includes prints
# this banner and nothing else until the first pixels are done.
POV_PARSING_RE = re.compile(r'\[Parsing')
# Radiosity is never announced. povray prints no pretrace marker at all, with
# or without +V, and its options block says nothing either: the only trace of
# it is that the pixel counter runs the image more than once, coarse pretrace
# passes first and then the real render. A scene with radiosity counts to the
# end and starts again, which is also why the bar appeared to go backwards.
POV_PIXELS_RE = re.compile(r'Rendered (\d+) of (\d+) pixels')
FFMPEG_PROGRESS_RE = re.compile(r'frame=\s*(\d+)')

def ensure_povray_config():
    """Creates the user config povray expects, so it stops warning it is absent.

    povray reads $HOME/.povray/<version>/povray.conf and prints a warning on
    every single render when it is missing. HOME is /tmp here, which is empty
    on each new pod, so the file cannot simply be baked into the image. The
    file is left with only a comment in it: its presence is what silences the
    warning, and no setting is overridden, so povray's defaults still apply.

    The version in the path has to match the binary, and it differs between
    povray builds, so it is read from the warning povray itself prints rather
    than hardcoded.
    """
    home = os.environ.get('HOME') or '/tmp'
    try:
        out = subprocess.run(["povray", "--version"], capture_output=True, text=True, timeout=30)
        text = (out.stderr or "") + (out.stdout or "")
        match = re.search(r'\.povray/([0-9]+\.[0-9]+)/povray\.conf', text)
        if not match:
            match = re.search(r'Version\s+([0-9]+\.[0-9]+)', text)
        if not match:
            logger.info("Could not determine the povray version; leaving its config alone.")
            return
        path = os.path.join(home, '.povray', match.group(1))
        os.makedirs(path, exist_ok=True)
        conf = os.path.join(path, 'povray.conf')
        if not os.path.exists(conf):
            with open(conf, 'w') as f:
                f.write("; Created by povomatic so povray does not warn on every render.\n"
                        "; Intentionally empty: povray's built-in defaults apply.\n")
            logger.info(f"Wrote {conf} to silence povray's missing config warning.")
    except Exception as e:
        logger.warning(f"Could not create the povray config, renders will still work: {e}")

def get_db_connection():
    return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)

def is_canceled(conn, job_id):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
            res = cur.fetchone()
            return res and res[0] == 'canceled'
    except Exception as e:
        logger.error(f"Error checking cancellation: {e}")
        return False

_fairness = {'at': 0.0, 'parent': None}

def least_loaded_animation(cur):
    """Parent id of the queued animation with the fewest workers on it.

    Ordering the queue by frame number alone is not enough: an animation that
    is already part rendered has no low numbered frames left, so it would be
    passed over until the others caught up to it. Steering each worker to
    whichever animation currently has the fewest frames rendering spreads the
    fleet evenly no matter how far along each one is.
    """
    # Grouping the whole frame queue costs about 100ms once it holds tens of
    # thousands of rows, and it only steers fairness, so a slightly stale
    # answer is harmless. Running it per claim from every worker was a large
    # part of what saturated the database.
    now = time.time()
    if now - _fairness['at'] < FAIRNESS_TTL:
        return _fairness['parent']
    try:
        cur.execute("""
            SELECT scene_file,
                   max(priority) AS priority,
                   count(*) FILTER (WHERE status = 'pending') AS waiting,
                   count(*) FILTER (WHERE status = 'rendering') AS running
            FROM jobs
            WHERE type = 'animation-frame'
            GROUP BY scene_file
        """)
        queued = [(-priority, running, parent)
                  for parent, priority, waiting, running in cur.fetchall() if waiting]
        parent = min(queued)[2] if queued else None
    except Exception as e:
        logger.error(f"Error picking least loaded animation: {e}")
        parent = None
    _fairness['at'] = now
    _fairness['parent'] = parent
    return parent

JOB_COLUMNS = ("id, scene_file, type, frames, povray_args, ffmpeg_args, "
               "current_frame, clock_initial, clock_final, stage")

def claim_job(cur):
    """Claims one job, as three indexed lookups rather than one sort.

    Ordering by an expression such as (scene_file = preferred) cannot use an
    index, so the single query this replaced sorted the entire queue on every
    claim: a sequential scan of 35k rows taking 177ms, run by two dozen workers
    at once. That saturated postgres and was what stalled the dashboard.

    The order of the steps is the priority order: an abandoned job first, since
    nothing else reclaims it and it blocks its animation from finishing, then a
    still which must never wait behind an animation, then frames of the animation
    with the fewest workers on it, then anything else. An encode worker skips all
    of that and claims only ffmpeg jobs.
    """
    # An encode worker only ever claims 'ffmpeg' jobs. Storage is shared NFS so
    # the node the frames were rendered on does not matter; the pin is ignored
    # rather than waited out. This also covers encodes whose worker died.
    if ENCODE_ONLY:
        cur.execute(f"""
            SELECT {JOB_COLUMNS} FROM jobs
            WHERE type = 'ffmpeg'
              AND (status = 'pending'
                   OR (status = 'rendering'
                       AND COALESCE(claimed_at, created_at) < now() - interval '{CLAIM_TIMEOUT}'))
            ORDER BY priority DESC, id
            LIMIT 1 FOR UPDATE SKIP LOCKED
        """)
        return cur.fetchone()

    # Expired leases first. These are jobs whose worker died, and nothing else
    # will ever pick them up: while any frame is queued the pending lookup below
    # always succeeds, so a step that only ran after it would never be reached,
    # and a stranded frame keeps its animation from completing forever. The
    # index on rendering rows keeps this to a handful of rows per claim. Encodes
    # are left to the encode workers.
    cur.execute(f"""
        SELECT {JOB_COLUMNS} FROM jobs
        WHERE status = 'rendering' AND type NOT IN ('animation', 'ffmpeg')
          AND COALESCE(claimed_at, created_at) < now() - interval '{CLAIM_TIMEOUT}'
        ORDER BY id
        LIMIT 1 FOR UPDATE SKIP LOCKED
    """)
    job = cur.fetchone()
    if job:
        logger.info(f"Reclaiming abandoned job {job[0]}.")
        return job

    cur.execute(f"""
        SELECT {JOB_COLUMNS} FROM jobs
        WHERE status = 'pending' AND type = 'still'
        ORDER BY priority DESC, id
        LIMIT 1 FOR UPDATE SKIP LOCKED
    """)
    job = cur.fetchone()
    if job:
        return job

    preferred = least_loaded_animation(cur)
    if preferred is not None:
        cur.execute(f"""
            SELECT {JOB_COLUMNS} FROM jobs
            WHERE status = 'pending' AND type = 'animation-frame' AND scene_file = %s
            ORDER BY current_frame
            LIMIT 1 FOR UPDATE SKIP LOCKED
        """, (preferred,))
        job = cur.fetchone()
        if job:
            return job

    # Only reached once the preferred animation is drained, or for a row whose
    # lease expired, so the wider scan here is rare rather than per claim.
    # Encodes are excluded: the encode workers own them.
    cur.execute(f"""
        SELECT {JOB_COLUMNS} FROM jobs
        WHERE type NOT IN ('animation', 'ffmpeg')
          AND (status = 'pending'
               OR (status = 'rendering'
                   AND COALESCE(claimed_at, created_at) < now() - interval '{CLAIM_TIMEOUT}'))
        ORDER BY priority DESC, current_frame ASC NULLS FIRST, id ASC
        LIMIT 1 FOR UPDATE SKIP LOCKED
    """)
    return cur.fetchone()

def touch_claim(conn, job_id):
    """Refreshes the lease so the fleet does not treat this job as abandoned."""
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE jobs SET claimed_at = now() WHERE id = %s", (job_id,))
            conn.commit()
    except Exception as e:
        logger.error(f"Error refreshing claim: {e}")

def push_log_tail(conn, job_id, lines):
    """Stores the tail of a job's output so the dashboard can show it live."""
    try:
        tail = "".join(lines)[-LOG_TAIL_CHARS:]
        with conn.cursor() as cur:
            cur.execute("UPDATE jobs SET log_tail = %s WHERE id = %s", (tail, job_id))
            conn.commit()
    except Exception as e:
        logger.error(f"Error storing log tail: {e}")

def set_stage(conn, job_id, stage):
    """Records which ffmpeg pass an encode is on, for the dashboard."""
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE jobs SET stage = %s WHERE id = %s", (stage, job_id))
            conn.commit()
    except Exception as e:
        logger.error(f"Error setting stage: {e}")

def update_progress(conn, job_id, progress):
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE jobs SET progress = %s WHERE id = %s", (min(100, progress), job_id))
            conn.commit()
    except Exception as e:
        logger.error(f"Error updating progress: {e}")

def run_process_with_progress(f, conn, job_id, cmd, progress_re, total_frames, is_percent=False,
                              progress_range=(0, 100), track_phase=False):
    """Runs a command, reporting its progress into a slice of the job's bar.

    An encode runs ffmpeg twice, and both passes used to report into the full
    range, so the bar reached 100% when the video finished and sat there for
    the whole gif pass while real work continued.
    """
    proc = subprocess.Popen(cmd, stdout=f, stderr=subprocess.PIPE, text=True, bufsize=1)
    stderr_output = []
    last_reported_percent = 0
    last_heartbeat = time.time()

    last_cancel_check = time.time()
    last_log_push = 0.0
    phase = None
    pixels_max = 0
    render_pass = 0

    # Waiting on the pipe with a timeout rather than blocking in readline: a
    # render can go minutes without writing a line, and the cancel check and
    # heartbeat have to keep running through that silence. Blocking here is
    # what let a canceled job keep rendering to completion.
    while True:
        ready, _, _ = select.select([proc.stderr], [], [], POLL_INTERVAL)

        if ready:
            line = proc.stderr.readline()
            if line:
                stderr_output.append(line)
                # Two transitions a frame, so writing them straight through
                # rather than on the throttle costs nothing.
                if track_phase:
                    pixels = POV_PIXELS_RE.search(line)
                    if pixels:
                        count = int(pixels.group(1))
                        # povray runs the whole frame more than once only for
                        # radiosity: a coarse pretrace pass over the image before
                        # the real render, which resets the pixel counter to
                        # near zero. A plain render counts up once. It does not
                        # count up cleanly, though: with +WT threads finish
                        # blocks slightly out of order, so the count dips by a
                        # few thousand pixels constantly. Treating any dip as a
                        # new pass flipped every threaded render to 'radiosity'
                        # on its first blip; a real restart drops the count to a
                        # small fraction of how far the pass had got, well below
                        # that jitter.
                        if pixels_max and count * 4 < pixels_max:
                            render_pass += 1
                            pixels_max = 0
                            last_reported_percent = 0
                            if phase != 'radiosity':
                                phase = 'radiosity'
                                set_stage(conn, job_id, phase)
                        pixels_max = max(pixels_max, count)
                    if phase is None and POV_PARSING_RE.search(line):
                        phase = 'parsing'
                        set_stage(conn, job_id, phase)
                    elif phase is None or (phase == 'parsing' and pixels):
                        if pixels:
                            phase = 'rendering'
                            set_stage(conn, job_id, phase)
                match = progress_re.search(line)
                if match:
                    if is_percent:
                        percent = int(match.group(1))
                    else:
                        current = int(match.group(1))
                        percent = int((current / total_frames) * 100) if total_frames > 0 else 0

                    # Report every 5 points rather than 10, so the bar moves
                    # twice as often on a long frame or encode pass.
                    if percent >= last_reported_percent + 5 or percent == 100:
                        low, high = progress_range
                        update_progress(conn, job_id, int(low + percent * (high - low) / 100))
                        last_reported_percent = percent
            elif proc.poll() is not None:
                break

        now = time.time()

        # Refreshed on a timer rather than with progress, so a render that is
        # slow to report a percentage still holds its claim.
        if now - last_heartbeat >= HEARTBEAT_SECONDS:
            touch_claim(conn, job_id)
            last_heartbeat = now

        # Throttled for the same reason as the cancel check below: povray
        # prints a great many progress lines, and a write per line from every
        # worker would be a lot of writes for a tooltip.
        if stderr_output and now - last_log_push >= LOG_PUSH_SECONDS:
            last_log_push = now
            push_log_tail(conn, job_id, stderr_output)

        # Throttled: this used to run once per output line, which was a query
        # per line of povray progress.
        if now - last_cancel_check >= POLL_INTERVAL:
            last_cancel_check = now
            if is_canceled(conn, job_id):
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                return False, "canceled", "".join(stderr_output)

        if not ready and proc.poll() is not None:
            break

    proc.stderr.close()
    proc.wait()

    # The end of this pass, which is not necessarily the end of the job.
    update_progress(conn, job_id, progress_range[1])
    
    return proc.returncode == 0, "finished", "".join(stderr_output)

# --- Modular Task Handlers ---

def handle_animation_frame(conn, job_id, scene, frames, p_args, f_args, output_dir, abs_scene_path, current_frame, parent_scene, clock_initial, clock_final):
    start_time = time.time()
    scene_name = os.path.splitext(os.path.basename(parent_scene))[0]
    output_filename = f"{scene_name}-{current_frame:05d}.png"
    output_path = os.path.join(output_dir, output_filename)

    tmp_output_path = f"/tmp/{output_filename}"
    final_output_path = os.path.join(output_dir, output_filename)

    cmd = [
        "povray", f"+I{abs_scene_path}", f"+O-",
        "-D",
        "+FN10",
        "-J",
        f"+WT{POVRAY_THREADS}",
        f"+KFI{1}", f"+KFF{frames}",
        f"+KI{clock_initial}", f"+KF{clock_final}",
        f"+SF{current_frame}", f"+EF{current_frame}"
    ]
    cmd.extend(LIBRARY_ARGS)
    if p_args: cmd.extend(p_args.split())
    logger.info(f"DEBUG: povray command: {cmd}")
    with open(tmp_output_path, 'wb') as f:
        success, msg, stderr = run_process_with_progress(f, conn, job_id, cmd, POV_PERCENT_RE, 0, is_percent=True, track_phase=True)

    # Only a successful render is published. povray creates the output file
    # before it parses, so a scene that fails to parse still leaves a zero byte
    # file behind: moving that into place fills the job directory with empty
    # frames that then look rendered to anything checking a frame exists.
    if success:
        if os.path.exists(tmp_output_path):
            shutil.move(tmp_output_path, output_path)
    elif os.path.exists(tmp_output_path):
        os.remove(tmp_output_path)

    povray_time = time.time() - start_time

    return success, msg, stderr, output_path, povray_time, 0

def handle_ffmpeg(conn, job_id, scene, frames, p_args, f_args, output_dir, abs_scene_path, stage=None):
    """Runs one encode pass over a finished frame sequence.

    An animation queues two of these, one per output. They read the same frames
    and write different files, so nothing orders them: the encoder pool runs
    them at once, on whichever nodes are free. A row's stage says which pass it
    is; an older row without one does both in turn, as it used to.
    """
    start_time = time.time()
    scene_name = os.path.splitext(os.path.basename(scene))[0]
    pattern = os.path.join(output_dir, f"{scene_name}-%05d.png")
    mp4_path = os.path.join(output_dir, f"{scene_name}.mp4")
    gif_path = os.path.join(output_dir, f"{scene_name}.gif")

    # Playback rate. An -r or -framerate in the job's ffmpeg_args sets it; that
    # value belongs before -i as the rate the frames are read at, not appended
    # after -c:v where it would retime a 25 fps stream to 60 instead.
    fps, extra_args = "25", []
    fa = (f_args or "").split()
    i = 0
    while i < len(fa):
        if fa[i] in ("-r", "-framerate") and i + 1 < len(fa):
            fps = fa[i + 1]
            i += 2
        else:
            extra_args.append(fa[i])
            i += 1

    ffmpeg_in = ["ffmpeg", "-y", "-framerate", fps, "-start_number", "1", "-i", pattern]

    def run_mp4(progress_range):
        # Encode workers carry a GPU, so /dev/dri is present and h264_vaapi
        # offloads the H.264 pass to the APU's fixed-function encoder. A missing
        # device or a runtime failure falls back to libx264 rather than losing a
        # good sequence.
        def _mp4_cmd(hw):
            if hw:
                cmd = ffmpeg_in + [
                    "-vaapi_device", VAAPI_DEVICE,
                    "-vf", "format=nv12,hwupload",
                    "-c:v", "h264_vaapi", "-movflags", "+faststart",
                ]
            else:
                cmd = ffmpeg_in + [
                    "-c:v", "libx264", "-pix_fmt", "yuv420p", "-movflags", "+faststart",
                ]
            cmd.extend(extra_args)
            cmd.append(mp4_path)
            return cmd

        use_hw = os.path.exists(VAAPI_DEVICE)
        cmd = _mp4_cmd(use_hw)
        logger.info(f"DEBUG: ffmpeg mp4 command: {cmd}")
        ok, msg, err = run_process_with_progress(
            subprocess.DEVNULL, conn, job_id, cmd, FFMPEG_PROGRESS_RE, frames,
            progress_range=progress_range)
        if not ok and msg != "canceled" and use_hw:
            logger.warning(f"Job {job_id}: h264_vaapi encode failed, retrying with libx264: {msg}")
            ok, msg, err = run_process_with_progress(
                subprocess.DEVNULL, conn, job_id, _mp4_cmd(False), FFMPEG_PROGRESS_RE, frames,
                progress_range=progress_range)
        return ok, msg, err

    def run_gif(progress_range):
        # The gif is a thumbnail, so it covers the whole animation in a fixed
        # number of seconds rather than running at playback speed. A 3000 frame
        # render at 12fps is a four minute, 48MB gif, which is useless to a
        # dashboard. Reading the frames faster and decimating to 12fps samples
        # across the entire sequence instead of truncating it.
        gif_input_fps = max(12, round(frames / GIF_SECONDS)) if frames else 12
        # Single pass palette: generating and applying it in one graph avoids
        # writing a palette file to the shared volume.
        gif_filter = ("fps=12,scale=320:-1:flags=lanczos,split[a][b];"
                      "[a]palettegen[p];[b][p]paletteuse")
        cmd = [
            "ffmpeg", "-y", "-framerate", str(gif_input_fps), "-start_number", "1", "-i", pattern,
            "-vf", gif_filter, "-loop", "0", gif_path,
        ]
        logger.info(f"DEBUG: ffmpeg gif command: {cmd}")
        return run_process_with_progress(
            subprocess.DEVNULL, conn, job_id, cmd, FFMPEG_PROGRESS_RE, frames,
            progress_range=progress_range)

    if stage == 'mp4':
        ok, msg, err = run_mp4((0, 100))
        return ok, msg, err, mp4_path, 0, time.time() - start_time

    if stage == 'gif':
        ok, msg, err = run_gif((0, 100))
        if not ok and msg != "canceled":
            # The mp4 is the deliverable and is a separate job now, so a failed
            # gif fails only itself.
            logger.error(f"Job {job_id}: gif encode failed: {msg}")
        return ok, msg, err, gif_path, 0, time.time() - start_time

    # No stage: a row queued before the encode was split. Both passes in turn,
    # sharing the bar as they did then.
    set_stage(conn, job_id, 'mp4')
    ok, msg, err = run_mp4((0, MP4_SHARE))
    if not ok:
        set_stage(conn, job_id, None)
        return False, msg, err, mp4_path, 0, time.time() - start_time

    set_stage(conn, job_id, 'gif')
    gif_ok, gif_msg, gif_err = run_gif((MP4_SHARE, 100))
    ffmpeg_time = time.time() - start_time
    set_stage(conn, job_id, None)
    if gif_msg == "canceled":
        return False, "canceled", gif_err, mp4_path, 0, ffmpeg_time
    if not gif_ok:
        logger.error(f"Job {job_id}: gif encode failed: {gif_msg}")
        return True, "finished", gif_err, mp4_path, 0, ffmpeg_time
    return True, "finished", err, mp4_path, 0, ffmpeg_time

def handle_still(conn, job_id, scene, frames, p_args, f_args, output_dir, abs_scene_path):
    start_time = time.time()
    final_path = f"{output_dir}/output.png"
    cmd = ["povray", f"+I{abs_scene_path}", "+O-", f"+WT{POVRAY_THREADS}"]
    cmd.extend(LIBRARY_ARGS)
    if p_args: cmd.extend(p_args.split())
    logger.info(f"DEBUG: povray command: {cmd}")
    # Rendered to a temporary file and moved into place only on success, for the
    # same reason as a frame: povray creates its output before parsing, so a
    # scene that fails to parse would otherwise leave a zero byte output.png
    # sitting where a finished still belongs.
    tmp_path = f"/tmp/still-{job_id}.png"
    with open(tmp_path, 'wb') as f:
        success, msg, stderr = run_process_with_progress(f, conn, job_id, cmd, POV_PERCENT_RE, 0, is_percent=True, track_phase=True)

    if success:
        shutil.move(tmp_path, final_path)
    elif os.path.exists(tmp_path):
        os.remove(tmp_path)

    povray_time = time.time() - start_time

    return success, msg, stderr, final_path, povray_time, 0

# --- Worker Main Loop ---

def poll_jobs():
    logger.info("Worker started.")
    
    # Map job types to handlers
    handlers = {
        'animation-frame': handle_animation_frame,
        'still': handle_still,
        'ffmpeg': handle_ffmpeg
    }
    
    while True:
        conn = get_db_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    # A rendering row whose lease has expired is taken over: the
                    # worker holding it died without resetting it, and nothing
                    # else would ever pick it up. COALESCE covers rows stranded
                    # before claimed_at existed. type != 'animation' guards
                    # against parent rows, which have no handler and are always
                    # 'rendering' while their frames run.
                    job = claim_job(cur)

                    if job:
                        global current_job_id
                        job_id, scene, job_type, frames, p_args, f_args, current_frame, clock_initial, clock_final, stage = job
                        current_job_id = job_id
                        logger.info(f"Processing job {job_id}.")
                        # node_name is recorded for every claim, not just encodes,
                        # so the dashboard can show which node holds each frame.
                        cur.execute("UPDATE jobs SET status = 'rendering', progress = 0, claimed_at = now(), node_name = %s WHERE id = %s", (NODE_NAME, job_id))
                        conn.commit()
                        
                        try:
                            parent_job_id = job_id
                            parent_scene = scene
                            # Both carry the parent id in scene_file rather than
                            # a scene name of their own.
                            if job_type in ('animation-frame', 'ffmpeg'):
                                parent_job_id = int(scene)
                                cur.execute("SELECT scene_file FROM jobs WHERE id = %s", (parent_job_id,))
                                parent_scene = cur.fetchone()[0]
                            
                            output_dir = os.path.join(OUTPUT_PATH, f"job_{parent_job_id}")
                            os.makedirs(output_dir, exist_ok=True)
                            
                            abs_scene_path = os.path.join(INPUT_PATH, parent_scene)
                            
                            # Execute handler
                            handler = handlers.get(job_type)
                            if not handler:
                                raise ValueError(f"Unknown job type: {job_type}")
                            
                            if job_type == 'animation-frame':
                                success, msg, stderr, final_path, povray_time, ffmpeg_time = handler(conn, job_id, parent_scene, frames, p_args, f_args, output_dir, abs_scene_path, current_frame, parent_scene, clock_initial, clock_final)
                            elif job_type == 'ffmpeg':
                                success, msg, stderr, final_path, povray_time, ffmpeg_time = handler(conn, job_id, parent_scene, frames, p_args, f_args, output_dir, abs_scene_path, stage)
                            else:
                                success, msg, stderr, final_path, povray_time, ffmpeg_time = handler(conn, job_id, parent_scene, frames, p_args, f_args, output_dir, abs_scene_path)
                            
                            # Update final status
                            if success:
                                if job_type == 'animation-frame':
                                    parent_job_id = int(scene)
                                    # The child row is removed first and the parent
                                    # only credited if this worker is the one that
                                    # removed it. A frame taken over after a stale
                                    # claim can be rendered twice, and counting it
                                    # twice would complete the parent early.
                                    cur.execute("DELETE FROM jobs WHERE id = %s", (job_id,))
                                    if cur.rowcount == 1:
                                        cur.execute("UPDATE jobs SET frames_rendered = frames_rendered + 1, povray_time = COALESCE(povray_time, 0) + %s WHERE id = %s RETURNING frames_rendered", (povray_time, parent_job_id))
                                        frames_rendered = cur.fetchone()[0]
                                        cur.execute("SELECT frames FROM jobs WHERE id = %s", (parent_job_id,))
                                        total_frames = cur.fetchone()[0]
                                        if frames_rendered >= total_frames:
                                            # error_log is cleared so a parent that was
                                            # marked failed by an older worker does not
                                            # keep a stale error once it completes.
                                            # A canceled parent stays canceled: frames
                                            # already running when the cancel landed
                                            # must not resurrect it.
                                            cur.execute("UPDATE jobs SET status = 'completed', progress = 100, error_log = NULL WHERE id = %s AND status != 'canceled'", (parent_job_id,))
                                            if cur.rowcount == 1:
                                                # This worker rendered the final frame,
                                                # so the encode is scheduled onto this
                                                # node. Reached only when the delete
                                                # above won, so exactly one is created.
                                                # One row per output rather than one job doing
                                                # both in turn. They are independent work on the
                                                # same finished frames, so the encoder pool can
                                                # run them at the same time and on different
                                                # nodes; the video no longer holds up the gif.
                                                cur.execute(
                                                    "INSERT INTO jobs (scene_file, type, frames, priority, ffmpeg_args, node_name, stage) "
                                                    "SELECT %s, 'ffmpeg', j.frames, j.priority + 1, j.ffmpeg_args, %s, s.stage "
                                                    "FROM jobs j, (VALUES ('mp4'), ('gif')) AS s(stage) WHERE j.id = %s",
                                                    (str(parent_job_id), NODE_NAME, parent_job_id))
                                                logger.info(f"Animation {parent_job_id} complete, mp4 and gif encodes queued.")
                                    else:
                                        logger.info(f"Job {job_id} was already completed elsewhere, not counting it twice.")
                                elif job_type == 'ffmpeg':
                                    cur.execute("UPDATE jobs SET status = 'completed', progress = 100, output_path = %s, ffmpeg_time = %s WHERE id = %s",
                                                (final_path, ffmpeg_time, job_id))
                                    # The parent carries the mp4 path, and the
                                    # dashboard derives the gif from it by swapping
                                    # the suffix, so publishing it before the gif
                                    # pass has finished would point at a file that
                                    # does not exist yet. The passes run in
                                    # parallel, so whichever finishes last sets it.
                                    cur.execute("""
                                        SELECT count(*) FILTER (WHERE status <> 'completed'),
                                               max(output_path) FILTER (WHERE output_path LIKE '%%.mp4'),
                                               COALESCE(sum(ffmpeg_time), 0)
                                        FROM jobs WHERE type = 'ffmpeg' AND scene_file = %s
                                    """, (str(parent_job_id),))
                                    outstanding, mp4_path, total_time = cur.fetchone()
                                    if outstanding == 0 and mp4_path:
                                        cur.execute("UPDATE jobs SET output_path = %s, ffmpeg_time = %s WHERE id = %s",
                                                    (mp4_path, total_time, parent_job_id))
                                        logger.info(f"Animation {parent_job_id} encodes complete.")
                                else:
                                    cur.execute("UPDATE jobs SET status = 'completed', progress = 100, output_path = %s, povray_time = %s, ffmpeg_time = %s WHERE id = %s", (final_path, povray_time, ffmpeg_time, job_id))
                            elif msg == "canceled":
                                cur.execute("UPDATE jobs SET status = 'canceled' WHERE id = %s", (job_id,))
                                if job_type == 'ffmpeg':
                                    # The encode is the animation's last step, so
                                    # cancelling it cancels the animation too,
                                    # whichever of the two rows the cancel landed
                                    # on. The parent is already 'completed' by the
                                    # time its encode runs, so that counts as
                                    # mid-encode here; a genuinely finished
                                    # animation has no ffmpeg job to cancel.
                                    cur.execute(
                                        "UPDATE jobs SET status = 'canceled', stage = NULL "
                                        "WHERE id = %s AND status IN ('rendering', 'completed')",
                                        (parent_job_id,))
                                    # And the other pass: the two run in parallel,
                                    # so cancelling one has to stop the other or the
                                    # gif carries on after the video was stopped.
                                    cur.execute(
                                        "UPDATE jobs SET status = 'canceled' WHERE type = 'ffmpeg' "
                                        "AND scene_file = %s AND status IN ('pending', 'rendering')",
                                        (str(parent_job_id),))
                            else:
                                cur.execute("UPDATE jobs SET status = 'failed', error_log = %s, povray_time = %s, ffmpeg_time = %s WHERE id = %s", (stderr, povray_time, ffmpeg_time, job_id))
                                if job_type == 'animation-frame':
                                    # An animation is only ever advanced by a frame
                                    # that succeeds, so one whose frames all fail
                                    # never reaches its total and never leaves
                                    # 'rendering': it sits at 0/N forever, neither
                                    # failed nor complete, with nothing to reap it.
                                    # Once no frame is left to run, the total can
                                    # no longer be reached, so the animation failed.
                                    cur.execute(
                                        "SELECT count(*) FROM jobs WHERE scene_file = %s "
                                        "AND type = 'animation-frame' AND status IN ('pending', 'rendering')",
                                        (str(parent_job_id),))
                                    if cur.fetchone()[0] == 0:
                                        cur.execute(
                                            "UPDATE jobs SET status = 'failed', error_log = %s "
                                            "WHERE id = %s AND status = 'rendering' "
                                            "AND frames_rendered < frames",
                                            ("Every remaining frame failed, so the sequence cannot be completed. "
                                             "Last frame error: " + (stderr or '')[-600:], parent_job_id))
                                        if cur.rowcount:
                                            logger.error(f"Animation {parent_job_id} failed: no frames left to render.")

                            conn.commit()
                            logger.info(f"Job {job_id} finished: {msg}")
                        except Exception as e:
                            logger.error(f"Job {job_id} failed: {e}")
                            cur.execute("UPDATE jobs SET status = 'failed', error_log = %s WHERE id = %s", (str(e), job_id))
                            conn.commit()
                        finally:
                            current_job_id = None
                    else:
                        time.sleep(10)
        except Exception as e:
            logger.error(f"Error polling jobs: {e}")
            time.sleep(10)
        finally:
            conn.close()

if __name__ == '__main__':
    ensure_povray_config()
    poll_jobs()
