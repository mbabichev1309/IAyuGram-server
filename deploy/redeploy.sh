#!/usr/bin/env bash
# Pull, restart, verify. Run from anywhere: paths are derived from this script's own
# location, so it works whether the checkout lives in /opt or under ~/Documents.
#
# The restart needs root. To stop it asking for a password every time, allow just that
# one command:
#   echo "$USER ALL=(root) NOPASSWD: /usr/bin/systemctl restart iayugram-server" | sudo tee /etc/sudoers.d/iayugram-redeploy && sudo visudo -c
set -euo pipefail

cd "$(dirname "$(readlink -f "$0")")/.."
echo "== repo: $PWD"

before=$(git rev-parse --short HEAD)
# --ff-only so a dirty or diverged checkout fails loudly instead of opening a merge.
git pull --ff-only
after=$(git rev-parse --short HEAD)

if [ "$before" = "$after" ]; then
    echo "== already at $after, restarting anyway"
else
    echo "== $before -> $after"
    git --no-pager log --oneline "$before..$after" | sed 's/^/   /'
fi

# If dependencies moved, a plain restart would run the old ones.
if [ -x .venv/bin/pip ] && ! git diff --quiet "$before" "$after" -- pyproject.toml; then
    echo "== pyproject changed, reinstalling"
    .venv/bin/pip install -q -e .
fi

sudo systemctl restart iayugram-server

# Restart returns before the app is serving, so poll rather than sleep-and-hope.
port=$(grep -E '^PORT=' .env 2>/dev/null | cut -d= -f2 | tr -d '[:space:]')
port=${port:-8787}
for i in $(seq 1 20); do
    if curl -fsS "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then
        echo "== healthy on :$port after ${i}s"
        systemctl is-active iayugram-server
        exit 0
    fi
    sleep 1
done

echo "!! not healthy after 20s — last log lines:" >&2
journalctl -u iayugram-server -n 30 --no-pager >&2
exit 1
