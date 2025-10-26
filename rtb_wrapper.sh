#!/usr/bin/env bash
set -euo pipefail

SRC=${SRC:-/srv/nas/test}
RTB=${RTB:-/mnt/backup/rtb_nas}
RTB_SCRIPT=${RTB_SCRIPT:-/opt/apps/rtb/rsync_tmbackup.sh}
RTB_EXCL=${RTB_EXCL:-/opt/apps/rtb/excludes.txt}

LOCKFILE=/var/lock/backup_pipeline.lock     # <— gleiches Lock wie Sync
WAIT_SEC=${WAIT_SEC:-7200}                  # max. 2h warten, falls Sync noch läuft

log(){ printf "%s %s\n" "$(date '+%F %T')" "$*" >&2; }
NO_CHANGE_EXIT=${NO_CHANGE_EXIT:-1}   # 1=bei keinen Änderungen Exit 0, 0=immer laufen lassen

# ========================================================================
# ========= KRITISCH: Globales Lock (gemeinsam mit pCloud-Sync) =========
# ========================================================================

exec 9>"$LOCKFILE"
if ! flock -w "$WAIT_SEC" 9; then
  log "[skip] konnte globales Lock innerhalb ${WAIT_SEC}s nicht bekommen (Sync läuft zu lange)."
  exit 0
fi

log "[start] RTB"

# === NEU: Pre-Check für Änderungen (Dry-Run) ===
LAST="$(readlink -f "${RTB}/latest" 2>/dev/null || true)"
if [[ -n "$LAST" && -d "$LAST" ]]; then
  log "[check] Prüfe auf Änderungen seit letztem Snapshot..."

  # Nutze exakt die gleichen Flags wie rsync_tmbackup
  # WICHTIG: --delete flag prüft auch gelöschte Dateien!
  if rsync -ni --delete \
      --links --hard-links --one-file-system --times --recursive --perms --owner --group \
      --exclude-from "${EXCLUDES:-/opt/apps/rtb/excludes.txt}" \
      "${SOURCE:-/srv/nas/test}/" "$LAST/" \
      | grep -qE '^[<>ch*]'; then
    log "[info] Änderungen erkannt - starte Backup"
  else
    log "[skip] Keine Änderungen seit letztem Backup - kein neuer Snapshot nötig"
    if [[ "$NO_CHANGE_EXIT" -eq 1 ]]; then
        exit 0
    fi
  fi
fi
# === ENDE NEU ===


sudo bash "$RTB_SCRIPT" "$SRC" "$RTB" "${RTB_EXCL}"
log "[done] RTB"
