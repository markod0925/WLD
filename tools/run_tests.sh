#!/usr/bin/env bash
set -euo pipefail

export TMPDIR=/tmp
export TEMP=/tmp
export TMP=/tmp

if [[ -x .venv/bin/pytest ]]; then
  exec .venv/bin/pytest "$@"
fi

exec pytest "$@"
