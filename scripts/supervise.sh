#!/usr/bin/env bash
# Keep one Polymarket WS watch process alive.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$ROOT/.venv/bin/python"
OUT="$ROOT/out"
LOG="$OUT/supervise.log"
WATCH_SCRIPT="${WATCH_SCRIPT:-watch_monthly.py}"
BASE="$(basename "$WATCH_SCRIPT" .py)"
WATCH_LOG="$OUT/${BASE}.log"
PIDFILE="$ROOT/${BASE}.pid"
mkdir -p "$OUT"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$LOG"; }

watch_alive() {
  if [[ -f "$PIDFILE" ]]; then
    pid="$(cat "$PIDFILE" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
  fi
  return 1
}

ensure_watch() {
  if watch_alive; then
    return 0
  fi
  if [[ ! -x "$VENV" ]]; then
    log "missing venv python at $VENV"
    return 1
  fi
  if [[ ! -f "$ROOT/.env" ]]; then
    log "missing .env (copy .env.example and fill WEBHOOK_*)"
    return 1
  fi
  if [[ ! -f "$ROOT/$WATCH_SCRIPT" ]]; then
    log "missing watcher $ROOT/$WATCH_SCRIPT"
    return 1
  fi
  log "starting $WATCH_SCRIPT"
  nohup "$VENV" "$ROOT/$WATCH_SCRIPT" >>"$WATCH_LOG" 2>&1 &
  echo $! >"$PIDFILE"
  sleep 1
  if watch_alive; then
    log "watch up pid=$(cat "$PIDFILE") script=$WATCH_SCRIPT"
    return 0
  fi
  log "FAILED to start $WATCH_SCRIPT"
  return 1
}

log "supervise start script=$WATCH_SCRIPT"
while true; do
  ensure_watch || true
  sleep 20
done
