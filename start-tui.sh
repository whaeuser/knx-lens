#!/usr/bin/env bash
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ ! -d "$SCRIPT_DIR/.venv" ]; then
    echo "Erstelle virtuelles Environment..."
    python3 -m venv "$SCRIPT_DIR/.venv"
    "$SCRIPT_DIR/.venv/bin/pip" install --quiet -r "$SCRIPT_DIR/requirements.txt"
fi

if ! grep -q "^KNX_PROJECT_PATH=.\+" "$SCRIPT_DIR/.env" 2>/dev/null; then
    echo "Keine .env gefunden – Setup wird gestartet..."
    "$SCRIPT_DIR/.venv/bin/python" "$SCRIPT_DIR/setup.py"
fi

exec "$SCRIPT_DIR/.venv/bin/python" "$SCRIPT_DIR/knx-lens.py" "$@"
