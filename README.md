# povomatic

Distributed POV-Ray rendering on Kubernetes. An animation is split into one job
per frame, rendered across autoscaled workers, and encoded into a video when the
last frame lands. A dashboard shows what every node is doing.

Python 3, PostgreSQL for the queue, KEDA for autoscaling, MetalLB for addresses,
NFS for scenes and output.

## How it works

Submitting an animation writes one parent row and one row per frame. Workers
claim frames, render them with povray, and write PNGs to shared storage. The
parent row is only bookkeeping: it is never rendered directly and is kept out of
the queue so nothing tries.

- **Claiming** (a render worker) is three indexed lookups in priority order: an
  abandoned job, then a still, then a frame of the animation with the fewest
  workers on it. That last step is what makes several queued animations run in
  parallel rather than one draining at a time. Encode workers run the same loop
  with `ENCODE_ONLY` and claim only ffmpeg jobs.
- **Leases.** A worker refreshes `claimed_at` every 30s while rendering. A job
  whose lease is older than 10 minutes is taken over by another worker, so a pod
  that dies mid-frame does not strand its animation. Frames can therefore be
  rendered twice, so the parent is credited only by the worker that removes the
  child row.
- **Encoding.** When the last frame lands, an ffmpeg job is queued. A separate
  pool of encode workers claims it: they hold an `amd.com/gpu` each so the H.264
  pass runs on the node APU's `h264_vaapi` encoder, falling back to libx264 if
  the GPU path fails. The mp4 plays at 25 fps unless the job's `ffmpeg_args`
  carry `-framerate` / `-r`, which set the rate the frames are read at. It produces an mp4 of the whole animation and a preview gif
  fixed at 10 seconds regardless of frame count, sampled across the whole
  sequence rather than truncated. Render workers never claim encodes, so they
  stay GPU free and run two per node; storage is shared NFS, so the encode is not
  pinned to the node that rendered the frames.
- **Cancelling** marks the rows; workers notice within 5 seconds and stop,
  including an encode mid-pass — the ffmpeg process is signalled and the parent
  animation is canceled with it. The output directory is then removed in the
  background, repeatedly until it stays gone, since frames keep landing for a few
  seconds after the click.

## Project structure

```
povomatic.py                    CLI client
worker/
  api.py                        Flask API, owns the schema and job fan-out
  worker.py                     claim loop: renders frames, or with ENCODE_ONLY
                                runs the h264_vaapi encode (libx264 fallback)
  povomatic.Dockerfile          one image for api, worker and dashboard;
                                povray, ffmpeg + mesa VA-API drivers
dashboard/
  app.py                        Flask + Socket.IO backend; artifact URLs come
                                from the DB output_path so the gif shows at once
  templates/index.html          frontend
manifests/
  postgres.yaml                 database, PVC, Service
  app.yaml                      NFS volumes, API, render workers, and the
                                GPU encoder pool (requests devic.es/dri)
  dashboard.yaml                dashboard
  keda.yaml                     ScaledObject and trigger auth (render workers)
  povomatic-lb.yaml             single entry point on one address
  metallb-resources.yaml        CPU reservation for metallb
  talos-vip-patch.yaml          Talos control plane VIP patch
  talos-longhorn.yaml           Talos kubelet extraMount that persists
                                /var/mnt/longhorn across reboots
scripts/
  apply-cpu-reservation.sh      applies CPU requests once the queue is idle
  commit-all.sh                 one-off: splits a batch of pending work into
                                three signed commits (run from a real terminal)
```

## Deployment

### 1. Build and push

```bash
docker build -t <your-registry>/povomatic:latest -f worker/povomatic.Dockerfile .
docker push <your-registry>/povomatic:latest
```

The same image runs the API, the workers and the dashboard; the manifests differ
only in the command.

### 2. Storage

Update the three PersistentVolumes in `manifests/app.yaml` with your NFS server
and export paths:

| PV | Default path | Mounted at | Contents |
|---|---|---|---|
| `nfs-input-pv` | `/nfs/povray/input` | `/app/input` | `.pov` scenes and their textures |
| `nfs-output-pv` | `/nfs/povray/output` | `/app/output` | rendered frames and videos, as `job_<id>/` |
| `nfs-assets-pv` | `/nfs/povray/assets` | `/app/assets` (read only) | shared `.inc` includes and texture library |

Both `/app/input` and `/app/assets` are passed to povray as library paths
(`+L`), because povray resolves `#include` and `image_map` against the working
directory rather than the scene file. Without them a scene cannot find a texture
sitting beside it.

`spec.nfs` is immutable on a bound PersistentVolume: changing a path means
deleting and recreating the PV and its PVC.

### 3. Apply

```bash
kubectl apply -f manifests/postgres.yaml
kubectl apply -f manifests/app.yaml
kubectl apply -f manifests/dashboard.yaml
kubectl apply -f manifests/keda.yaml
kubectl apply -f manifests/povomatic-lb.yaml   # optional, see below
```

The `encoder` deployment in `app.yaml` requests `devic.es/dri`, the `/dev/dri`
render node, so the nodes need the AMD GPU driver (on Talos, the
`siderolabs/amdgpu` system extension) and a device plugin that advertises it —
`squat/generic-device-plugin` with `--device 'name: dri … path: /dev/dri/renderD128'`.
Without it the encoder pods stay `Pending` and encodes wait; drop the
`resources.limits` block to fall back to CPU-only libx264 encoding.

`postgres.yaml` creates the namespace. The schema is created and migrated by the
API at startup, not on first use, so the API must run once before anything else
works. Re-applying `postgres.yaml` fails on the PVC, whose `storageClassName`
and `volumeName` are immutable once bound and absent from the manifest; the
Deployment still applies.

Addresses come from MetalLB. `povomatic-lb.yaml` puts the dashboard on port 80
and the API on 8080 of a single address, so both sit behind one name. Read its
comments before using it: MetalLB does not speak DHCP, so if the address is
inside the router's DHCP range it has to be reserved there, and it is announced
from one worker node to keep its MAC stable.

## Sizing

The defaults assume nodes with roughly 4 cores.

| | |
|---|---|
| worker cpu request | `1000m`, two per node |
| encoder | 2 replicas, `500m` cpu and 1 `amd.com/gpu` each |
| postgres cpu request | `800m` |
| KEDA replicas | render workers 0 to 23, polling every 15s, 5 minute cooldown |
| povray threads | `+WT4` per worker, deliberately oversubscribed |

Two workers per node rather than one because a single povray cannot keep four
cores busy: roughly half of each frame is single threaded parse and png
encoding. Overlapping two frames fills those gaps and took a twelve node cluster
from about 48% to 77% utilisation.

Postgres needs its own request. Without one it is BestEffort and gets starved by
the workers sharing its node, which stalls every query at once and with it the
dashboard. `scripts/apply-cpu-reservation.sh` applies these once the queue is
empty, because restarting postgres truncates the `UNLOGGED` jobs table if the
shutdown is not clean.

KEDA counts work in flight as well as waiting, so an encode is not killed by a
scale to zero the moment the last frame lands.

## Usage

```bash
# submit
python3 povomatic.py --api-url http://<api> --scene scene.pov --type animation \
    --frames 100 --povray-args "+W800 +H600"

# submit a still
python3 povomatic.py --api-url http://<api> --scene scene.pov --type still

python3 povomatic.py --api-url http://<api> --list
python3 povomatic.py --api-url http://<api> --cancel <job_id>
python3 povomatic.py --api-url http://<api> --clear
```

`--priority` (higher first), `--clock-initial` and `--clock-final` (the animation
clock range) and `--ffmpeg-args` are also accepted. `POVOMATIC_API` sets the
default API URL.

Note that nothing on the API deletes files. `--clear` and the `DELETE` endpoints
remove rows and leave the rendered output on disk, because the API does not mount
the output volume. Only the dashboard's delete button removes a job's files.

### API

| | |
|---|---|
| `POST /submit` | submit a job |
| `GET /status/<id>` | one job |
| `GET /jobs` | all jobs |
| `POST /cancel/<id>` | cancel |
| `POST /jobs/<id>/retry` | return a failed job to the queue |
| `DELETE /jobs/<id>` | delete one job's rows, leaving its files |
| `DELETE /jobs` | delete every row, leaving every file |

```json
{
  "scene_file": "scene.pov",
  "type": "animation",
  "frames": 100,
  "priority": 5,
  "povray_args": "+W800 +H600",
  "ffmpeg_args": "-c:v libx264"
}
```

## Dashboard

Served by the `dashboard` service in the `povomatic` namespace. It pushes over
Socket.IO two to three times a second.

- **Navbar** counts queued, running and encoding work, with the average render
  time per frame and per encode. Clicking a counter narrows the panels to that
  state; clicking it again clears the filter.
- **Active renders** shows one card per animation with its overall progress, an
  ETA and the clock time it is expected to finish, a live preview of the newest
  frame, and a tile per frame in flight with its progress and the node holding
  it. Encodes appear as their own cards, naming which ffmpeg pass is running.
- **Completed jobs** show the animated gif as a thumbnail, which links to the
  mp4. A job whose encode is still running says so and shows its progress, and
  cannot be deleted until the encode finishes.
- **Cancel** stops a render and removes its partial output. **Delete** removes a
  job and everything rendered for it.

Both buttons act through the dashboard rather than the API, because the
dashboard is the only service that mounts the output volume.

The ETA is measured from how fast frames are actually landing, over a moving
window, so it accounts for the fleet being shared between animations. Per frame
render time cannot serve this: the frame rows are deleted as they succeed.

There is no authentication. Anything that can reach the service can cancel and
delete jobs.

### Tuning

Dashboard: `PUSH_MIN_INTERVAL` (0.25s), `POLL_SECONDS` (0.5s), `ETA_WINDOW`
(180s), `DIR_CACHE_TTL` (3s), `DIR_REFRESH_SECONDS` (0.5s), `RETRY_SECONDS` (2s).

Worker: `POVRAY_THREADS` (4), `NODE_NAME` (from the downward API), and the paths
above.

Directory listings are cached and read on a background thread, never on the push
path: listing a job directory of several thousand frames takes a moment when
idle and over a second while workers are writing into it.

## Licence

Copyright (C) 2026 Jakob Flierl.

povomatic is free software under the **GNU Affero General Public License,
version 3 or later**. See [LICENSE](LICENSE) for the full text.

The AGPL differs from the GPL in one way that matters here: section 13 covers
use over a network. Anyone who interacts with a running instance of the
dashboard, not just anyone who receives a copy of the code, is entitled to its
source. If you deploy a modified povomatic where others can reach it, you have
to offer them your modified source. The dashboard carries a link to this
repository for that reason; point it at your own fork if you change the code.

POV-Ray itself is AGPLv3, which is where the copy of the licence text in this
repository came from.
