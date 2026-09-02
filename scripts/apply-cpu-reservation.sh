#!/usr/bin/env bash
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

# Applies the postgres CPU reservation, and the matching reduction in the worker
# request that makes room for it, once the cluster is idle.
#
# It waits because applying it restarts postgres, and the jobs table is UNLOGGED:
# an unclean shutdown truncates it and takes the queue with it. Waiting for an
# empty queue means there is nothing to lose if that happens. It also waits for
# the workers to scale away, because their requests are what leave no room for
# postgres to be scheduled in the first place.
set -uo pipefail
NS=povomatic
REPO="$(cd "$(dirname "$0")/.." && pwd)"
DEADLINE=$(( $(date +%s) + ${MAX_WAIT:-21600} ))   # give up after 6h by default
INTERVAL=${INTERVAL:-60}

log() { echo "[$(date '+%F %T')] $*"; }

queue_depth() {
  local pg
  pg=$(kubectl get pod -n $NS -l app=postgres --field-selector=status.phase=Running \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) || return 1
  [ -n "$pg" ] || return 1
  kubectl exec -n $NS "$pg" -- psql -U povomatic -d povomatic -t -A \
    -c "SELECT count(*) FROM jobs WHERE status IN ('pending','rendering')" 2>/dev/null | tr -d ' '
}

worker_count() {
  kubectl get pods -n $NS -l app=worker --no-headers 2>/dev/null | grep -c Running
}

log "waiting for an idle cluster (queue empty and no workers)"
while :; do
  if [ "$(date +%s)" -ge "$DEADLINE" ]; then
    log "ABORT: no quiet window within the deadline; nothing applied"; exit 2
  fi
  q=$(queue_depth); w=$(worker_count)
  if [ -z "${q:-}" ]; then
    log "postgres unreachable, will retry"
  elif [ "$q" = "0" ] && [ "${w:-0}" = "0" ]; then
    log "idle: queue=$q workers=$w — confirming"
    sleep 30
    q2=$(queue_depth); w2=$(worker_count)
    if [ "${q2:-1}" = "0" ] && [ "${w2:-1}" = "0" ]; then break; fi
    log "work reappeared (queue=$q2 workers=$w2), continuing to wait"
  else
    log "busy: queue=$q workers=$w"
  fi
  sleep "$INTERVAL"
done

log "applying worker request (1000m)"
kubectl apply -f "$REPO/manifests/app.yaml" 2>&1 | grep -vE 'unchanged|Warning' | sed 's/^/  /'

# Only the Deployment: re-applying the whole file fails on the bound PVC, whose
# storageClassName and volumeName are immutable and absent from the manifest.
log "applying postgres reservation (800m)"
python3 - "$REPO/manifests/postgres.yaml" <<'PY' | kubectl apply -f - 2>&1 | sed 's/^/  /'
import sys
docs = open(sys.argv[1]).read().split('\n---')
print('\n---'.join(d for d in docs if 'kind: Deployment' in d))
PY

log "waiting for rollouts"
kubectl rollout status deploy/postgres -n $NS --timeout=300s 2>&1 | tail -1 | sed 's/^/  /'
kubectl rollout status deploy/api -n $NS --timeout=300s 2>&1 | tail -1 | sed 's/^/  /'

log "verifying"
kubectl get pods -n $NS --no-headers 2>/dev/null | awk '{print "  "$1" "$2" "$3}'
kubectl get deploy postgres -n $NS -o jsonpath='  postgres cpu request: {.spec.template.spec.containers[0].resources.requests.cpu}{"\n"}'
kubectl get deploy worker -n $NS -o jsonpath='  worker   cpu request: {.spec.template.spec.containers[0].resources.requests.cpu}{"\n"}'
code=$(curl -s -o /dev/null -m 10 -w '%{http_code}' http://192.168.178.199:8080/jobs)
log "api through the VIP: HTTP $code"
[ "$code" = "200" ] && log "DONE" || { log "WARNING: api not healthy after apply"; exit 1; }
