#!/usr/bin/env bash
#
# One-shot Ubuntu setup for the IAyuGram companion capture server.
#
# Run it from inside the cloned repo, AFTER you have placed a valid .env next to
# pyproject.toml (the .env holds SESSION_STRING + keys and is NOT in git — copy
# it over manually from your Windows box).
#
#   git clone https://github.com/mbabichev1309/iayugram-server.git
#   cd iayugram-server
#   cp /path/to/your/.env .env        # transferred manually
#   bash deploy/setup-ubuntu.sh
#
# Overridable via env:
#   SVC_USER   systemd service user  (default: current user)
#   APP_DIR    install location      (default: this repo's absolute path)
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR="${APP_DIR:-$REPO_DIR}"
SVC_USER="${SVC_USER:-$(id -un)}"
VENV="$APP_DIR/.venv"
UNIT=/etc/systemd/system/iayugram-server.service

echo "==> repo:   $REPO_DIR"
echo "==> app dir:$APP_DIR"
echo "==> user:   $SVC_USER"

if [ ! -f "$APP_DIR/.env" ]; then
  echo "ERROR: $APP_DIR/.env is missing." >&2
  echo "Copy your .env (SESSION_STRING + API_ID/API_HASH + CONTENT_KEY + CLIENT_TOKEN) here first." >&2
  exit 1
fi

echo "==> installing system packages"
sudo apt-get update -y
sudo apt-get install -y python3 python3-venv python3-pip

echo "==> creating venv + installing deps"
python3 -m venv "$VENV"
"$VENV/bin/pip" install --upgrade pip
"$VENV/bin/pip" install -e "$APP_DIR"

echo "==> writing systemd unit -> $UNIT"
sudo tee "$UNIT" >/dev/null <<UNITEOF
[Unit]
Description=IAyuGram companion capture server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SVC_USER
WorkingDirectory=$APP_DIR
EnvironmentFile=$APP_DIR/.env
ExecStart=$VENV/bin/python -m iayugram_server

# Uptime is critical: differenceTooLong won't replay deletes missed while down.
Restart=always
RestartSec=3

# Hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full

[Install]
WantedBy=multi-user.target
UNITEOF

echo "==> enabling + starting service"
sudo systemctl daemon-reload
sudo systemctl enable --now iayugram-server

echo "==> status"
sudo systemctl --no-pager status iayugram-server | head -12 || true
echo
echo "Done. Follow logs with:  journalctl -u iayugram-server -f"
