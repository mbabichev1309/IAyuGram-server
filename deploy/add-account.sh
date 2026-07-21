#!/usr/bin/env bash
#
# Onboard one more account as its own systemd instance (Variant A: one process
# per account, fully isolated — own session, port, DB and encryption key).
#
# Run with sudo, from inside the repo:
#   sudo bash deploy/add-account.sh <name> [path-to-session-string-file]
#
# <name>  short instance id (letters/digits/_-), e.g. "alice".
# The friend generates their SESSION_STRING on THEIR desktop with
# scripts/tdata_to_session.py, then sends it to you; pass it as a file or paste
# it when prompted. A SESSION_STRING grants full access to that account — handle
# it accordingly (this script stores per-account env files root-only, chmod 600).
#
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  echo "Run with sudo: sudo bash deploy/add-account.sh <name> [session-file]" >&2
  exit 1
fi

NAME="${1:-}"
SESSION_FILE="${2:-}"
if ! [[ "$NAME" =~ ^[A-Za-z0-9_-]+$ ]]; then
  echo "Usage: sudo bash deploy/add-account.sh <name> [session-file]" >&2
  echo "  <name> must be letters/digits/_/- only." >&2
  exit 1
fi

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="$REPO_DIR/.venv"
PY="$VENV/bin/python"
ENV_DIR=/etc/iayugram
INST_ENV="$ENV_DIR/$NAME.env"
UNIT=/etc/systemd/system/iayugram-server@.service
# Run the service as the repo owner (so it can write data/ there).
SVC_USER="$(stat -c '%U' "$REPO_DIR")"

[ -x "$PY" ] || { echo "venv missing at $VENV — run setup-ubuntu.sh first" >&2; exit 1; }
[ -f "$REPO_DIR/.env" ] || { echo "$REPO_DIR/.env missing (need API_ID/API_HASH to copy)" >&2; exit 1; }

mkdir -p "$ENV_DIR"; chmod 700 "$ENV_DIR"

if [ -f "$INST_ENV" ]; then
  echo "Instance '$NAME' already exists ($INST_ENV). Remove it first to recreate." >&2
  exit 1
fi

# --- session string -------------------------------------------------------
if [ -n "$SESSION_FILE" ]; then
  SESSION_STRING="$(tr -d '\r\n' < "$SESSION_FILE")"
else
  echo "Paste ${NAME}'s SESSION_STRING (input hidden), then Enter:"
  read -rs SESSION_STRING; echo
fi
[ -n "$SESSION_STRING" ] || { echo "empty SESSION_STRING" >&2; exit 1; }

# --- reuse app credentials, generate per-account secrets -------------------
API_ID="$(grep -E '^API_ID=' "$REPO_DIR/.env" | cut -d= -f2-)"
API_HASH="$(grep -E '^API_HASH=' "$REPO_DIR/.env" | cut -d= -f2-)"
CONTENT_KEY="$("$PY" -c 'from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())')"
CLIENT_TOKEN="$("$PY" -c 'import secrets;print(secrets.token_urlsafe(32))')"

# --- pick a free port (8787 is the primary; new ones start at 8788) --------
PORT=8788
while grep -rhoE '^PORT=[0-9]+' "$ENV_DIR" 2>/dev/null | cut -d= -f2 | grep -qx "$PORT"; do
  PORT=$((PORT + 1))
done

# --- write the per-account env (root-only) --------------------------------
umask 077
cat > "$INST_ENV" <<EOF
# IAyuGram capture — account: $NAME (generated $(date -u +%Y-%m-%dT%H:%M:%SZ))
API_ID=$API_ID
API_HASH=$API_HASH
SESSION_STRING=$SESSION_STRING
CONTENT_KEY=$CONTENT_KEY
CLIENT_TOKEN=$CLIENT_TOKEN
HOST=0.0.0.0
PORT=$PORT
DB_PATH=data/$NAME.db
EOF
chmod 600 "$INST_ENV"

# --- install the systemd template unit (once) -----------------------------
cat > "$UNIT" <<EOF
[Unit]
Description=IAyuGram capture (account %i)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SVC_USER
WorkingDirectory=$REPO_DIR
EnvironmentFile=$ENV_DIR/%i.env
ExecStart=$VENV/bin/python -m iayugram_server
Restart=always
RestartSec=3
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now "iayugram-server@$NAME"
sleep 3
systemctl --no-pager status "iayugram-server@$NAME" | head -8 || true

echo
echo "=== account '$NAME' onboarded ==="
echo "  port        : $PORT   (DB: data/$NAME.db, own CONTENT_KEY)"
echo "  CLIENT_TOKEN: $CLIENT_TOKEN"
echo "Give the client: host=<server-ip> port=$PORT token=<the CLIENT_TOKEN above>"
echo "Logs: journalctl -u iayugram-server@$NAME -f"
