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
# Takes an etcd snapshot and prunes the old ones.
#
#   etcd-snapshot.sh
#
# This is the thing that makes a Talos upgrade survivable. Upgrading a control
# plane reboots it, and the three of them hold quorum only because they are done
# one at a time; a snapshot is what you fall back to when one does not come back.
#
# Written for a weekly cron. The failure modes that matter at 4am are the ones
# that look like success, so it keeps the last good snapshot if anything goes
# wrong, writes the checksum beside each file, and only prunes after a snapshot
# that actually verified.
#
# DESTR_DIR    where snapshots live
# KEEP         how many to keep (default 8, so a week of daily plus slack)
# KEEP_DAYS    also drop anything older than this, whatever KEEP says
# TALOS_NODE   control plane to snapshot from; first reachable if unset
set -uo pipefail
# cron runs with a minimal PATH and neither tool is on it: talosctl is
# /usr/local/bin, kubectl is a snap in /snap/bin. Without this the job fails at
# 4am with "kubectl: not found" and takes no snapshot.
export PATH=/usr/local/bin:/snap/bin:/usr/bin:/bin:$PATH
DESTR_DIR=${DESTR_DIR:-/nfs/talos-backups}
KEEP=${KEEP:-8}
KEEP_DAYS=${KEEP_DAYS:-30}
LOG_PREFIX=${LOG_PREFIX:-}

log() { echo "[$(date '+%F %T')] ${LOG_PREFIX}$*"; }
die() { log "ERROR: $*" >&2; exit 1; }

command -v talosctl >/dev/null || die "talosctl not in PATH"
mkdir -p "$DESTR_DIR" || die "cannot create $DESTR_DIR"

# Snapshot from a control plane: only they have an etcd member to snapshot, and
# reading a follower would work but is one step further from the data than asking
# the leader.
# Quietly: under cron a kubectl that cannot reach the API should produce the
# "no reachable control plane" error below, not a JSON traceback.
control_planes() {
  kubectl get nodes -o json 2>/dev/null | python3 -c "
import json, sys
for n in json.load(sys.stdin)['items']:
    if 'node-role.kubernetes.io/control-plane' in n['metadata']['labels']:
        for a in n['status']['addresses']:
            if a['type'] == 'InternalIP':
                print(a['address']); break
"
}

# talosctl reaches one node at a time, so name them rather than trusting a
# cluster endpoint that may not be one of the members.
target=${TALOS_NODE:-}
if [ -z "$target" ]; then
  for ip in $(control_planes); do
    if talosctl -n "$ip" etcd status >/dev/null 2>&1; then target=$ip; break; fi
  done
fi
[ -n "$target" ] || die "no reachable control plane; is the kubeconfig current?"
log "snapshotting from $target"

# Name it for the Talos version it captures, so a restore does not have to be
# matched up with a release by hand. `talosctl version` prints a Client block then
# a Server block, both fields indented with a tab, so take the first Tag after
# "Server:" rather than the client's.
version=$(talosctl -n "$target" version 2>/dev/null |
  awk '/^Server:/ {s=1} s && $1 == "Tag:" {print $2; exit}')
[ -n "$version" ] || version=unknown

TS=$(date +%Y%m%d-%H%M%S)
out="$DESTR_DIR/etcd-snapshot-$version-$TS.db"

# To a temp name and moved into place only once verified, so a snapshot cut short
# by a reboot or a full disk never looks like a usable restore point. It still
# gets its name written into the log, so nothing goes missing quietly.
tmp="$out.partial"
info=$(talosctl -n "$target" etcd snapshot "$tmp" --endpoints "$target" 2>&1) ||
  { rm -f "$tmp"; die "talosctl failed; previous snapshots untouched"; }
printf '%s\n' "$info" | sed 's/^/  /'
[ -s "$tmp" ] || { rm -f "$tmp"; die "empty snapshot; previous snapshots untouched"; }

# An etcd snapshot is a BoltDB file, and a truncated one is still a valid BoltDB
# file, so size proves nothing. talosctl only prints "snapshot info: ... revision
# N, total keys N" after reading the file back, so a missing or zero revision
# here means it is not a usable restore point.
rev=$(printf '%s\n' "$info" | sed -n 's/.*revision \([0-9]*\).*/\1/p' | head -1)
keys=$(printf '%s\n' "$info" | sed -n 's/.*total keys \([0-9]*\).*/\1/p' | head -1)
[ -n "$rev" ] && [ "$rev" -gt 0 ] 2>/dev/null ||
  { rm -f "$tmp"; die "no revision read back, so not a usable snapshot; previous ones untouched"; }
[ -n "$keys" ] && [ "$keys" -gt 0 ] 2>/dev/null ||
  { rm -f "$tmp"; die "snapshot has no keys; previous ones untouched"; }

mv "$tmp" "$out" || { rm -f "$tmp"; die "cannot write to $DESTR_DIR"; }
sha256sum "$out" > "$out.sha256" || die "cannot write checksum"
chmod 600 "$out"
log "saved $(basename "$out"): $(stat -c %s "$out") bytes, revision $rev, $keys keys"

# Only now, with a verified snapshot on disk, is it safe to drop the old ones.
# -1 keeps the newest matching file, which is the one just written.
log "pruning: keeping $KEEP newest, $KEEP_DAYS days"
find "$DESTR_DIR" -maxdepth 1 -name 'etcd-snapshot-*.db' -mtime "+$KEEP_DAYS" -print |
  while read -r old; do
    log "  too old: $(basename "$old")"
    rm -f "$old" "$old.sha256"
  done
# Oldest first, and drop everything past the KEEP newest, so what survives is the
# most recent KEEP. Taking the tail of a sorted list here would delete the newest
# instead — the snapshot just written, which is the one that matters.
find "$DESTR_DIR" -maxdepth 1 -name 'etcd-snapshot-*.db' -print |
  sort | head -n "-$KEEP" |
  while read -r old; do
    log "  over $KEEP: $(basename "$old")"
    rm -f "$old" "$old.sha256"
  done

log "on hand: $(find "$DESTR_DIR" -maxdepth 1 -name 'etcd-snapshot-*.db' | wc -l) snapshot(s)"
log "DONE"
