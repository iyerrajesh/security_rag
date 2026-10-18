#!/usr/bin/env bash
# Create/refresh a virtualenv and run the fleet simulator.
# Usage: simulator/run.sh [simulator flags...]   e.g. simulator/run.sh --vehicles 1000 --duration 600
# Env:   PYTHON (default python3), VENV_DIR (default .venv)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON="${PYTHON:-python3}"
VENV_DIR="${VENV_DIR:-.venv}"
REQUIREMENTS="requirements.txt"

if [[ ! -x "$VENV_DIR/bin/python" ]]; then
  echo "Creating virtualenv in $VENV_DIR" >&2
  "$PYTHON" -m venv "$VENV_DIR"
fi

# Reinstall only when requirements change.
STAMP="$VENV_DIR/.platform-requirements"
if ! cat requirements.txt simulator/requirements.txt validation/requirements.txt lakehouse/requirements.txt | cmp -s - "$STAMP"; then
  echo "Installing $REQUIREMENTS" >&2
  "$VENV_DIR/bin/python" -m pip install --quiet --upgrade pip
  "$VENV_DIR/bin/python" -m pip install --quiet -r "$REQUIREMENTS"
  cat requirements.txt simulator/requirements.txt validation/requirements.txt lakehouse/requirements.txt > "$STAMP"
fi

exec "$VENV_DIR/bin/python" -m simulator "$@"
