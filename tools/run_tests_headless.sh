#!/usr/bin/env bash
set -euo pipefail

exec bash tools/run_tests.sh -m "not qt" "$@"
