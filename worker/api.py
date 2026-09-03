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

from flask import Flask, request, jsonify
import psycopg2
import os
import logging

logging.basicConfig(
    level=os.environ.get('LOG_LEVEL', 'INFO').upper(),
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
)
logger = logging.getLogger('povomatic.api')

app = Flask(__name__)

PORT = int(os.environ.get('PORT', '5000'))
SERVER_THREADS = int(os.environ.get('SERVER_THREADS', '8'))
DB_HOST = os.environ.get('DB_HOST', 'postgres')
DB_NAME = os.environ.get('DB_NAME', 'povomatic')
DB_USER = os.environ.get('DB_USER', 'povomatic')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'password')
DB_CONNECT_TIMEOUT = int(os.environ.get('DB_CONNECT_TIMEOUT', '5'))
if DB_PASSWORD == 'password':
    logger.warning("DB_PASSWORD is the built-in default; set it from a Secret in production")

INIT_SQL = """
CREATE UNLOGGED TABLE IF NOT EXISTS jobs (
    id SERIAL PRIMARY KEY,
    scene_file VARCHAR(255) NOT NULL,
    type VARCHAR(20) NOT NULL CHECK (type IN ('still', 'animation', 'animation-frame', 'ffmpeg')),
    frames INTEGER DEFAULT 1,
    priority INTEGER DEFAULT 0 NOT NULL,
    status VARCHAR(20) DEFAULT 'pending' NOT NULL CHECK (status IN ('pending', 'rendering', 'completed', 'failed', 'canceled')),
    progress INTEGER DEFAULT 0 NOT NULL CHECK (progress >= 0 AND progress <= 100),
    output_path VARCHAR(255),
    povray_args TEXT,
    ffmpeg_args TEXT,
    error_log TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_priority ON jobs (status, priority DESC) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs (created_at DESC);

CREATE OR REPLACE FUNCTION notify_new_job() RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify('job_updates', NEW.id::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_new_job ON jobs;
CREATE TRIGGER trg_new_job
AFTER INSERT ON jobs
FOR EACH ROW EXECUTE FUNCTION notify_new_job();
"""

def get_db_connection():
    return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER,
                            password=DB_PASSWORD, connect_timeout=DB_CONNECT_TIMEOUT)

def init_db():
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'jobs'")
                if cur.fetchone()[0] == 0:
                    logger.info("Initializing database...")
                    cur.execute(INIT_SQL)
                else:
                    cur.execute("ALTER TABLE jobs SET UNLOGGED")

                # Ensure columns exist for existing installations
                cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS povray_time NUMERIC")
                cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS ffmpeg_time NUMERIC")
                cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS current_frame INTEGER")
                cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS frames_rendered INTEGER DEFAULT 0")
                cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS clock_initial NUMERIC DEFAULT 0.0")
                cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS clock_final NUMERIC DEFAULT 1.0")
                # Lease timestamp. A worker refreshes it while rendering, so a
                # row whose claim has gone stale can be reclaimed by the fleet.
                cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP WITH TIME ZONE")
                # Node an ffmpeg job should run on: the one that rendered the
                # animation's last frame.
                cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS node_name VARCHAR(255)")
                # Which pass an encode is on. It runs ffmpeg twice, and without
                # this the dashboard cannot say whether the video or the preview
                # gif is being written.
                cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS stage VARCHAR(32)")
                # The tail of what povray is printing right now, so the dashboard
                # can show a frame's output while it renders rather than only
                # after it fails.
                cur.execute("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS log_tail TEXT")
                # Matches the worker's claim ordering, which interleaves queued
                # animations by frame number.
                cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs (priority DESC, current_frame, id) WHERE status = 'pending'")
                # Indexes for the dashboard's stats queries. Without them each
                # rebuild sequentially scanned the whole table and took seconds,
                # which was what limited how often the dashboard could refresh.
                cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_pending_frames ON jobs (status) INCLUDE (frames) WHERE status = 'pending'")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_completed_recent ON jobs (created_at DESC) WHERE status = 'completed'")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_rendering ON jobs (status, type) WHERE status = 'rendering'")
                # The history query filters with <>, which no plain index helps.
                # Without this it walked every queued frame row to find the few
                # non-frame rows: 2.4s against a 36k row queue, 0.2ms with it.
                cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_history ON jobs (created_at DESC) WHERE type <> 'animation-frame'")
                # The two lookups a worker claims through. Without them the
                # claim sorted the whole frame queue on every attempt, 177ms a
                # time from every worker at once.
                cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_frame_claim ON jobs (scene_file, current_frame) WHERE status = 'pending' AND type = 'animation-frame'")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_nonframe_claim ON jobs (priority DESC, id) WHERE status = 'pending' AND type IN ('still', 'ffmpeg')")

                # The original constraint only allowed still and animation, so
                # an install created from it could not hold frame or encode rows.
                cur.execute("ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_type_check")
                cur.execute("ALTER TABLE jobs ADD CONSTRAINT jobs_type_check CHECK (type IN ('still', 'animation', 'animation-frame', 'ffmpeg'))")
                
                # Update trigger to cover INSERT, UPDATE, DELETE
                cur.execute("""
                    CREATE OR REPLACE FUNCTION notify_job_changes() RETURNS TRIGGER AS $$
                    BEGIN
                      IF (TG_OP = 'DELETE') THEN
                        PERFORM pg_notify('notify_job_changes', OLD.id::text);
                        RETURN OLD;
                      ELSE
                        PERFORM pg_notify('notify_job_changes', NEW.id::text);
                        RETURN NEW;
                      END IF;
                    END;
                    $$ LANGUAGE plpgsql;

                    DROP TRIGGER IF EXISTS trg_new_job ON jobs;
                    DROP TRIGGER IF EXISTS trg_job_changes ON jobs;
                    CREATE TRIGGER trg_job_changes
                    AFTER INSERT OR UPDATE OR DELETE ON jobs
                    FOR EACH ROW EXECUTE FUNCTION notify_job_changes();
                """)

                # Event log behind the dashboard's Log tab. UNLOGGED to match
                # jobs: it is a history of that table, so losing it to the same
                # unclean shutdown that truncates jobs is coherent. A trigger
                # records submissions, status changes, encode passes and
                # deletions; frame rows are noisy bookkeeping and are logged
                # only when they fail. Retention is swept opportunistically from
                # the trigger so no separate job is needed.
                cur.execute("""
                    CREATE UNLOGGED TABLE IF NOT EXISTS job_events (
                        id       BIGSERIAL PRIMARY KEY,
                        ts       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
                        job_id   INTEGER NOT NULL,
                        scene_file VARCHAR(255),
                        job_type VARCHAR(20),
                        event    VARCHAR(40) NOT NULL,
                        detail   TEXT
                    );
                    CREATE INDEX IF NOT EXISTS idx_job_events_ts ON job_events (ts DESC, id DESC);

                    CREATE OR REPLACE FUNCTION log_job_event() RETURNS TRIGGER AS $$
                    DECLARE
                        j     jobs%ROWTYPE;
                        ev    VARCHAR(40);
                        det   TEXT;
                        disp  VARCHAR(255);
                    BEGIN
                        IF (TG_OP = 'DELETE') THEN j := OLD; ELSE j := NEW; END IF;

                        -- Frames and encodes carry the parent animation's id in
                        -- scene_file; resolve it to that animation's name. On a
                        -- DELETE the parent row is usually gone already (deleting
                        -- an animation removes it and its children together), so
                        -- the lookup misses and the deleted event falls back to
                        -- showing the raw parent id. Cosmetic, and only on delete.
                        disp := j.scene_file;
                        IF j.type IN ('ffmpeg', 'animation-frame') AND j.scene_file ~ '^\\d+$' THEN
                            SELECT scene_file INTO disp FROM jobs WHERE id = j.scene_file::int;
                            disp := coalesce(disp, j.scene_file);
                        END IF;

                        IF (TG_OP = 'INSERT') THEN
                            IF j.type = 'animation-frame' THEN RETURN NEW; END IF;
                            ev  := 'submitted';
                            det := j.type || CASE WHEN j.frames > 1
                                                  THEN ' · ' || j.frames || ' frames' ELSE '' END;
                        ELSIF (TG_OP = 'DELETE') THEN
                            IF j.type = 'animation-frame' THEN RETURN OLD; END IF;
                            ev  := 'deleted';
                        ELSIF (NEW.status IS DISTINCT FROM OLD.status) THEN
                            IF NEW.type = 'animation-frame' AND NEW.status <> 'failed' THEN
                                RETURN NEW;
                            END IF;
                            ev  := NEW.status;
                            det := CASE
                                     WHEN NEW.status = 'failed'
                                       THEN left(regexp_replace(coalesce(NEW.error_log, ''), '\\s+', ' ', 'g'), 400)
                                     WHEN NEW.node_name IS NOT NULL AND NEW.status IN ('rendering', 'completed')
                                       THEN 'on ' || NEW.node_name
                                   END;
                            IF NEW.type = 'animation-frame' THEN
                                det := 'frame ' || NEW.current_frame || coalesce(' — ' || det, '');
                            END IF;
                        ELSIF (NEW.type = 'ffmpeg' AND NEW.stage IS DISTINCT FROM OLD.stage
                               AND NEW.stage IS NOT NULL) THEN
                            ev  := 'encoding ' || NEW.stage;
                            det := 'on ' || coalesce(NEW.node_name, '?');
                        ELSE
                            RETURN NEW;
                        END IF;

                        INSERT INTO job_events (job_id, scene_file, job_type, event, detail)
                        VALUES (j.id, disp, j.type, ev, det);

                        IF (random() < 0.02) THEN
                            DELETE FROM job_events WHERE ts < now() - interval '7 days';
                        END IF;

                        IF (TG_OP = 'DELETE') THEN RETURN OLD; END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;

                    DROP TRIGGER IF EXISTS trg_job_events ON jobs;
                    CREATE TRIGGER trg_job_events
                    AFTER INSERT OR UPDATE OR DELETE ON jobs
                    FOR EACH ROW EXECUTE FUNCTION log_job_event();
                """)
    except Exception as e:
        logger.error(f"Error initializing DB: {e}")
    finally:
        conn.close()

@app.route('/submit', methods=['POST'])
def submit():
    data = request.json
    scene_file = data.get('scene_file')
    job_type = data.get('type')
    if not scene_file or not job_type:
        return jsonify({'error': 'Missing required fields'}), 400
    
    priority = data.get('priority', 0)
    frames = data.get('frames', 1)
    povray_args = data.get('povray_args', '')
    ffmpeg_args = data.get('ffmpeg_args', '')
    clock_initial = data.get('clock_initial', 0.0)
    clock_final = data.get('clock_final', 1.0)
    
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # An 'animation' row only tracks its per-frame children, it is
                # never rendered directly. Keeping it out of the pending queue
                # stops a worker claiming it and failing on the missing handler,
                # and stops KEDA counting it as work to scale for.
                parent_status = 'rendering' if job_type == 'animation' else 'pending'
                cur.execute("INSERT INTO jobs (scene_file, type, frames, priority, povray_args, ffmpeg_args, clock_initial, clock_final, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                            (scene_file, job_type, frames, priority, povray_args, ffmpeg_args, clock_initial, clock_final, parent_status))
                job_id = cur.fetchone()[0]
                
                if job_type == 'animation':
                    # Create sub-jobs for each frame
                    for frame in range(1, frames + 1):
                        cur.execute("INSERT INTO jobs (scene_file, type, frames, priority, povray_args, current_frame, clock_initial, clock_final) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                                    (str(job_id), 'animation-frame', frames, priority, povray_args, frame, clock_initial, clock_final))
        logger.info(f"Job {job_id} submitted.")
        return jsonify({'job_id': job_id, 'status': 'created'}), 201
    except Exception as e:
        logger.error(f"Error submitting job: {e}")
        return jsonify({'error': 'Internal server error'}), 500
    finally:
        conn.close()

@app.route('/status/<int:job_id>', methods=['GET'])
def status(job_id):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
                res = cur.fetchone()
        if res:
            return jsonify({'status': res[0]})
        return jsonify({'error': 'Not found'}), 404
    finally:
        conn.close()

@app.route('/cancel/<int:job_id>', methods=['POST'])
def cancel(job_id):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE jobs SET status = 'canceled' WHERE id = %s", (job_id,))
        logger.info(f"Job {job_id} canceled.")
        return jsonify({'message': 'Job canceled'})
    finally:
        conn.close()

@app.route('/jobs', methods=['GET'])
def list_jobs():
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, scene_file, status, progress, created_at FROM jobs ORDER BY created_at DESC")
                jobs = cur.fetchall()
        return jsonify([{'id': j[0], 'scene': j[1], 'status': j[2], 'progress': j[3], 'created_at': j[4]} for j in jobs])
    finally:
        conn.close()

@app.route('/jobs', methods=['DELETE'])
def clear_jobs():
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM jobs")
        logger.info("All jobs cleared.")
        return jsonify({'message': 'All jobs deleted'}), 200
    finally:
        conn.close()

@app.route('/jobs/<int:job_id>', methods=['DELETE'])
def delete_job(job_id):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM jobs WHERE id = %s", (job_id,))
        return jsonify({'message': 'Job deleted'}), 200
    finally:
        conn.close()

@app.route('/jobs/<int:job_id>/retry', methods=['POST'])
def retry_job(job_id):
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE jobs SET status = 'pending', progress = 0, error_log = NULL WHERE id = %s", (job_id,))
        return jsonify({'message': 'Job reset to pending'}), 200
    finally:
        conn.close()

@app.errorhandler(Exception)
def handle_unexpected(e):
    """Any unhandled error returns JSON, never a stack trace."""
    from werkzeug.exceptions import HTTPException
    if isinstance(e, HTTPException):
        return jsonify({'error': e.name}), e.code
    logger.exception("unhandled error serving a request")
    return jsonify({'error': 'internal error'}), 500

if __name__ == '__main__':
    init_db()
    # waitress, not app.run: Flask's built-in server is single-threaded by
    # default and prints a "development server" warning it means. The API is
    # plain request/response JSON, so a threaded WSGI server is all it needs.
    from waitress import serve
    logger.info("serving on 0.0.0.0:%s with %s threads", PORT, SERVER_THREADS)
    serve(app, host='0.0.0.0', port=PORT, threads=SERVER_THREADS, ident='povomatic-api')
