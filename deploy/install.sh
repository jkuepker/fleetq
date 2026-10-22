#!/usr/bin/env bash
# Deploy fleetq to a Linux host (a small VM or container is enough) with systemd.
# Safe to re-run: keeps both tokens and the database.
# Usage: deploy/install.sh root@HOST   (or set FLEETQ_DEPLOY_TARGET)
# The roster is deploy/fleet.toml (untracked: it is site-specific and may hold secrets such as
# an ntfy topic); FLEETQ_ROSTER=/path/to/fleet.toml picks another file.
set -euo pipefail
TARGET=${1:-${FLEETQ_DEPLOY_TARGET:-}}
[ -n "$TARGET" ] || { echo "usage: deploy/install.sh root@HOST" >&2; exit 2; }
cd "$(dirname "$0")/.."
ROSTER=${FLEETQ_ROSTER:-deploy/fleet.toml}
[ -f "$ROSTER" ] || { echo "no $ROSTER: copy deploy/fleet.example.toml and edit it" >&2; exit 2; }
COPYFILE_DISABLE=1 tar --no-xattrs --exclude __pycache__ -C src -czf - fleetq | ssh "$TARGET" 'set -e
  id fleetq >/dev/null 2>&1 || useradd --system --home-dir /var/lib/fleetq --shell /usr/sbin/nologin fleetq
  install -d -o root -g root -m 755 /opt/fleetq
  rm -rf /opt/fleetq/fleetq && tar -C /opt/fleetq -xzf -
  install -d -o fleetq -g fleetq -m 750 /var/lib/fleetq
  install -d -o root -g fleetq -m 750 /etc/fleetq /etc/fleetq/collectors'
scp -q "$ROSTER" "$TARGET":/etc/fleetq/fleet.toml
scp -q deploy/fleetq.service "$TARGET":/etc/systemd/system/fleetq.service
ssh "$TARGET" 'set -e
  chown root:fleetq /etc/fleetq/fleet.toml && chmod 640 /etc/fleetq/fleet.toml
  [ -s /etc/fleetq/token ] || python3 -c "import secrets; print(secrets.token_urlsafe(32))" > /etc/fleetq/token
  chown fleetq:fleetq /etc/fleetq/token && chmod 600 /etc/fleetq/token
  [ -s /etc/fleetq/readonly_token ] || python3 -c "import secrets; print(secrets.token_urlsafe(32))" > /etc/fleetq/readonly_token
  chown fleetq:fleetq /etc/fleetq/readonly_token && chmod 600 /etc/fleetq/readonly_token
  PYTHONPATH=/opt/fleetq python3 -m fleetq.server --config /etc/fleetq/fleet.toml --check
  systemctl daemon-reload && systemctl enable --quiet fleetq && systemctl restart fleetq
  sleep 2 && systemctl is-active fleetq'
