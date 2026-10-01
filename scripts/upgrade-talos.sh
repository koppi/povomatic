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
#
# Upgrades Talos across the cluster, one node at a time.
#
#   upgrade-talos.sh <version> [node ...]
#
# With no nodes named, every node is upgraded: workers first, control planes
# last. Take an etcd snapshot before running this (`talosctl etcd snapshot`).
# Three control planes hold quorum through any single one of them rebooting, but
# only if they are done one at a time.
#
# Two things it is careful about.
#
# The installer image is built from the schematic the cluster already runs, so
# the system extensions that schematic carries — the amdgpu driver the encoder
# pool needs for its h264_vaapi pass, amd-ucode, the iscsi tools — survive the
# upgrade. The schematic is read from a live node rather than hardcoded, so a
# cluster provisioned with a different extension set keeps that set. Upgrading
# from a stock image instead would strip the extensions, and the encoder pods
# would sit Pending forever.
#
# Longhorn gets in the way of the drain. It creates a PodDisruptionBudget per
# instance-manager with minAvailable=1, which pins allowedDisruptions at 0, and
# talosctl drains through the eviction API. The eviction is refused, the drain
# runs until it times out, and by then the new image has already been written to
# the node's disk — so the node reports a post-check pass, never reboots, and
# goes on running the old version, with nothing but a "context deadline
# exceeded" to say so. Holding those PDBs deleted for the length of each drain
# is the workaround.
set -uo pipefail
if [ $# -lt 1 ]; then
  echo "usage: upgrade-talos.sh <version> [node ...]" >&2
  exit 2
fi
VERSION=$1
shift
LH_NS=longhorn-system

log() { echo "[$(date '+%F %T')] $*"; }
die() { echo "[$(date '+%F %T')] ERROR: $*" >&2; exit 1; }

# kubectl addresses nodes by name, and pod nodeName is a name, so an IP has to
# be resolved before anything can be looked up by it.
node_name() {
  kubectl get nodes -o json 2>/dev/null | python3 -c "
import json,sys
t='$1'
for i in json.load(sys.stdin)['items']:
    ips=[a['address'] for a in i['status'].get('addresses',[]) if a['type']=='InternalIP']
    if t==i['metadata']['name'] or t in ips:
        print(i['metadata']['name']); break
"
}

is_control_plane() {
  kubectl get node "$1" -o jsonpath='{.metadata.labels.node-role\.kubernetes\.io/control-plane}' 2>/dev/null
}

# The extensions a node runs, and the schematic ID that produced them. The
# output is a stream of concatenated JSON objects, one per extension, so it is
# decoded rather than parsed as a document.
extensions() {
  talosctl -n "$1" get extensions -o json 2>/dev/null | python3 -c "
import json,sys
s=sys.stdin.read(); dec=json.JSONDecoder(); i=0
while i < len(s):
    while i < len(s) and s[i] in ' \n\r\t': i += 1
    if i >= len(s): break
    o,i = dec.raw_decode(s,i)
    m = o.get('spec',{}).get('metadata',{})
    n,v = m.get('name'), m.get('version')
    if n and n not in ('schematic','modules.dep'): print('%s=%s' % (n,v))
"
}

schematic() {
  talosctl -n "$1" get extensions -o json 2>/dev/null | python3 -c "
import json,sys
s=sys.stdin.read(); dec=json.JSONDecoder(); i=0
while i < len(s):
    while i < len(s) and s[i] in ' \n\r\t': i += 1
    if i >= len(s): break
    o,i = dec.raw_decode(s,i)
    m = o.get('spec',{}).get('metadata',{})
    if m.get('name')=='schematic': print(m.get('version','')); break
"
}

# talosctl version prints a client block and a server block that both mention
# the version, and tab-indents every field. Only the server one says anything
# about the node, so match the field rather than the line: /^Tag:/ never fires.
server_version() {
  talosctl -n "$1" version 2>/dev/null |
    awk '/^Server:/{s=1;next} s&&$1=="Tag:"{print $2;exit}'
}

instance_managers() {
  kubectl get pods -n "$LH_NS" -o json 2>/dev/null | python3 -c "
import json,sys
for i in json.load(sys.stdin)['items']:
    if i['metadata']['name'].startswith('instance-manager-') and i['spec'].get('nodeName')=='$1':
        print(i['metadata']['name'])
"
}

# All nodes, workers before control planes so that when a control plane drains
# there is still spare capacity to reschedule onto.
all_nodes() {
  kubectl get nodes -o json 2>/dev/null | python3 -c "
import json,sys
items=json.load(sys.stdin)['items']
key=lambda i: 1 if 'node-role.kubernetes.io/control-plane' in (i['metadata'].get('labels') or {}) else 0
for i in sorted(items,key=key):
    ip=next((a['address'] for a in i['status'].get('addresses',[]) if a['type']=='InternalIP'),'')
    print('%s %s' % (i['metadata']['name'], ip))
" | while read -r n ip; do [ "$n" = "$ip" ] && echo "$n" || echo "$ip"; done
}

# Resolve and check every target before reading anything off a node, so that a
# typo names itself instead of surfacing later as an unrelated failure.
wanted=("$@")
# With no nodes named this reboots the whole cluster, one node at a time. Make
# that deliberate: a mistyped or mis-parsed argument list must never quietly
# widen from the one node meant to all twelve.
if [ ${#wanted[@]} -eq 0 ]; then
  mapfile -t wanted < <(all_nodes)
  log "WARNING: no nodes named, so all ${#wanted[@]} will be upgraded and rebooted"
  if [ "${FORCE:-}" != 1 ]; then
    printf 'continue? [y/N] '
    read -r reply
    case $reply in y|Y|yes) ;; *) die "aborted; name the nodes to upgrade instead" ;; esac
  fi
fi
[ ${#wanted[@]} -gt 0 ] || die "no nodes to upgrade"
for w in "${wanted[@]}"; do
  [ -n "$(node_name "$w")" ] || die "no such node: $w"
done

# The schematic is read off a node that is about to be upgraded, so what it
# carries is what that node keeps.
SCHEMATIC=$(schematic "${wanted[0]}")
[ -n "$SCHEMATIC" ] || die "cannot read the running schematic from ${wanted[0]}"
IMAGE="factory.talos.dev/metal-installer/$SCHEMATIC:$VERSION"

TARGETS=("${wanted[@]}")

log "schematic $SCHEMATIC"
log "image      $IMAGE"
log "targets    ${TARGETS[*]}"

# An upgrade reboots the node, so do not start one against a node that is
# already where it is being asked to go.
todo=()
for t in "${TARGETS[@]}"; do
  [ "$(server_version "$t")" = "$VERSION" ] && log "skip $t: already $VERSION" || todo+=("$t")
done
[ ${#todo[@]} -gt 0 ] || { log "DONE: every node is already on $VERSION"; exit 0; }
log "upgrading  ${todo[*]}"

for target in "${todo[@]}"; do
  name=$(node_name "$target"); [ -n "$name" ] || die "no such node: $target"
  role=worker; [ -n "$(is_control_plane "$name")" ] && role=control-plane
  before=$(extensions "$target" | sort | tr '\n' ' ')

  log "--- $name ($role, $target) ---"

  # Hold the instance-manager PDBs down for the length of the drain. Longhorn
  # recreates one within seconds of it being deleted, so deleting it once and
  # moving on is not enough.
  hold=""
  for pod in $(instance_managers "$name"); do hold="$hold $pod"; done
  if [ -n "$hold" ]; then
    for pod in $hold; do
      kubectl -n "$LH_NS" delete pdb "$pod" --wait=false >/dev/null 2>&1
    done
    ( for _ in $(seq 1 "${HOLD_SECONDS:-180}"); do
        for pod in $hold; do
          kubectl -n "$LH_NS" delete pdb "$pod" --wait=false >/dev/null 2>&1
        done
        sleep 1
      done ) &
    holder=$!
  fi

  talosctl upgrade --nodes "$target" --image "$IMAGE" --wait \
    --timeout "${TIMEOUT:-20m}" 2>&1 | grep -viE 'unavailable, retrying' | tail -5 | sed 's/^/  /'

  [ -n "${holder:-}" ] && kill "$holder" 2>/dev/null
  kubectl uncordon "$name" >/dev/null 2>&1

  got=$(server_version "$target" 2>/dev/null)
  after=$(extensions "$target" | sort | tr '\n' ' ')
  if [ "$got" != "$VERSION" ]; then
    log "FAILED: $name reports $got, expected $VERSION"
    exit 1
  fi
  if [ "$before" != "$after" ]; then
    log "WARNING: extensions changed on $name"
    log "  before: $before"
    log "  after:  $after"
  else
    log "$name on $got, extensions unchanged: $after"
  fi
done

log "DONE: every node in ${TARGETS[*]} is on $VERSION"