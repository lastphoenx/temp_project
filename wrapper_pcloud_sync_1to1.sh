#!/usr/bin/env bash
set -euo pipefail

# ========= Konfiguration =========
MAIN_DIR=${MAIN_DIR:-/opt/apps/pcloud-tools/main}
RTB=${RTB:-/mnt/backup/rtb_nas}

ENV_FILE=${ENV_FILE:-${MAIN_DIR}/.env}
PCLOUD_DEST=${PCLOUD_DEST:-/Backup/rtb_1to1}

MANI=${MANI:-${MAIN_DIR}/pcloud_json_manifest.py}
PUSH=${PUSH:-${MAIN_DIR}/pcloud_push_json_manifest_to_pcloud.py}

# Python-Interpreter (venv bevorzugt)
if [[ -n "${VIRTUAL_ENV:-}" && -x "${VIRTUAL_ENV}/bin/python" ]]; then
  PY="${VIRTUAL_ENV}/bin/python"
elif [[ -x "/opt/apps/pcloud-tools/venv/bin/python" ]]; then
  PY="/opt/apps/pcloud-tools/venv/bin/python"
else
  PY="${PY:-python3}"
fi

# Module auffindbar machen
export PYTHONPATH="${MAIN_DIR}:${PYTHONPATH:-}"

# finalize im wrapper immer überspringen. Bei Bedarf manuell starten
export PCLOUD_SKIP_FINALIZE=1

# EAGER-FileID default aus (verhindert unnötige Index-Änderungen)
#export PCLOUD_EAGER_FILEID=0


# Nur manuell bei Bedarf, Code-Beispiel für Cli - einfach ohne # verwenden:
# python3 - <<'PY'
# from pcloud_push_json_manifest_to_pcloud import finalize_index_fileids
# import pcloud_bin_lib as pc
# cfg = pc.effective_config(env_file=".env")
# finalize_index_fileids(cfg, "/Backup/rtb_1to1/_snapshots")
# PY


# ========= KRITISCH: Globales Lock (gemeinsam mit RTB) =========
LOCKFILE=${LOCKFILE:-/var/lock/backup_pipeline.lock}
WAIT_SEC=${WAIT_SEC:-7200}  # max. 2h warten falls RTB läuft

# Safety-Delay nach RTB-Abschluss (File-System settle time)
SAFETY_DELAY_SEC=${SAFETY_DELAY_SEC:-120}

# === FIX: Global Temp-Manifest Cleanup ===
declare -a TMP_MANIS=()
trap 'rm -f "${TMP_MANIS[@]:-}"' EXIT

log() { printf "%s %s\n" "$(date '+%F %T')" "$*" >&2; }

last_snapshot_mtime() {
  local latest_dir; latest_dir="$(readlink -f "${RTB}/latest" 2>/dev/null || true)"
  [[ -z "$latest_dir" ]] && echo 0 && return
  stat -c %Y "$latest_dir" 2>/dev/null || echo 0
}

# === OPTIMIERT: Cached Remote-Snapshot-Liste ===
_REMOTE_SNAPS_CACHE=""
_REMOTE_SNAPS_LOADED=0

load_remote_snapshots() {
  if [[ $_REMOTE_SNAPS_LOADED -eq 1 ]]; then
    return  # Cache hit
  fi
  
  _REMOTE_SNAPS_CACHE=$("${PY}" - <<'PY'
import os, sys
sys.path.insert(0, os.environ.get("MAIN_DIR","/opt/apps/pcloud-tools/main"))
import pcloud_bin_lib as pc
cfg = pc.effective_config(env_file=os.environ.get("ENV_FILE"))
dest_root = os.environ.get("PCLOUD_DEST","/Backup/rtb_1to1")
snap_root = f"{pc._norm_remote_path(dest_root).rstrip('/')}/_snapshots"
try:
    top = pc.listfolder(cfg, path=snap_root, recursive=False, nofiles=True, showpath=False) or {}
    contents = ((top.get("metadata") or {}).get("contents") or [])
    names = [c["name"] for c in contents if c.get("isfolder") and c.get("name") != "_index"]
    print("\n".join(sorted(names)))
except Exception as e:
    sys.stderr.write(f"[warn] remote snapshot listing failed: {e}\n")
    print("")
PY
)
  _REMOTE_SNAPS_LOADED=1
}

remote_has_snapshots() {
  load_remote_snapshots
  [[ -n "$_REMOTE_SNAPS_CACHE" ]] && echo "YES" || echo "NO"
}

remote_snapshot_exists() {
  local snapname="$1"
  load_remote_snapshots
  grep -qx "$snapname" <<<"$_REMOTE_SNAPS_CACHE" && echo "YES" || echo "NO"
}

local_snapshot_names() {
  find "$RTB" -maxdepth 1 -type d -printf '%f\n' \
  | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}-' \
  | sort
}

remote_snapshot_names() {
  load_remote_snapshots
  echo "$_REMOTE_SNAPS_CACHE"
}

# === FIX: Set-Differenz für Retention-Entscheid ===
need_retention_sync() {
  # Vergleicht Sets: Retention nötig, wenn es mind. einen Remote-Snapshot gibt,
  # der lokal NICHT (mehr) vorhanden ist.
  load_remote_snapshots
  local locals remotes remote_only s

  locals="$(local_snapshot_names | sort -u)"
  remotes="$(remote_snapshot_names | sort -u)"

  remote_only=""
  # Zeilenweise iterieren über Remote-Snapshots
  while IFS= read -r s; do
    [[ -z "$s" ]] && continue
    if ! grep -qxF "$s" <<<"$locals"; then
      remote_only+="$s"$'\n'
    fi
  done <<<"$remotes"

  [[ -n "$remote_only" ]] && echo YES || echo NO
}

build_and_push() {
  local SNAP="$1" SNAPNAME; SNAPNAME="$(basename "$SNAP")"
  log "[info] push snapshot: $SNAPNAME"
  
  # === FIX: mktemp mit doppeltem Cleanup (RETURN + EXIT) ===
  local mani
  mani="$(mktemp -t "pcloud_mani.${SNAPNAME}.XXXXXX.json")"
  TMP_MANIS+=("$mani")
  trap 'rm -f "$mani"' RETURN
  
  local T0=$(date +%s)
  
  if [[ "${PCLOUD_TIMING:-0}" == "1" ]]; then
    log "[t] start manifest"
  fi
  
  "${PY}" "$MANI" --root "$SNAP" --snapshot "$SNAPNAME" --out "$mani" --hash sha256
  
  if [[ "${PCLOUD_TIMING:-0}" == "1" ]]; then
    log "[t] done manifest (Δ=$(( $(date +%s)-T0 ))s)"; T1=$(date +%s)
  fi

  # Retention-Flag entscheiden (nutzt Set-Differenz)
  local RET=""
  [[ "$(need_retention_sync)" == "YES" ]] && RET="--retention-sync"

  if [[ "${PCLOUD_TIMING:-0}" == "1" ]]; then
    log "[t] start push"
  fi

  "${PY}" "$PUSH" --manifest "$mani" --dest-root "$PCLOUD_DEST" --snapshot-mode 1to1 $RET --env-file "$ENV_FILE"
  
  if [[ "${PCLOUD_TIMING:-0}" == "1" ]]; then
    log "[t] done push (Δ=$(( $(date +%s)-T1 ))s), total=$(( $(date +%s)-T0 ))s)"
  fi
  
  # Cache invalidieren nach Push
  _REMOTE_SNAPS_LOADED=0
}


# --- Preflight: Token/Quota/Userinfo & Zielpfad prüfen ----------------------
validate_inputs_or_exit() {
  # ENV_FILE vorhanden?
  if [[ -n "${ENV_FILE:-}" && ! -f "$ENV_FILE" ]]; then
    log "[error] ENV_FILE nicht gefunden: $ENV_FILE"
    exit 2
  fi
  # PCLOUD_DEST muss mit / beginnen
  if [[ -z "${PCLOUD_DEST:-}" || "${PCLOUD_DEST:0:1}" != "/" ]]; then
    log "[error] Ungültiger PCLOUD_DEST (muss mit / beginnen): '${PCLOUD_DEST:-<leer>}'"
    exit 2
  fi
}

preflight_or_mark_down() {
  # Gibt eine von drei Strings auf stdout: OK | DOWN | OVERQUOTA
  "${PY}" - <<'PY'
import os, sys
sys.path.insert(0, os.environ.get("MAIN_DIR","/opt/apps/pcloud-tools/main"))
try:
    import pcloud_bin_lib as pc
except Exception as e:
    print("DOWN")
    sys.exit(0)

try:
    cfg = pc.effective_config(env_file=os.environ.get("ENV_FILE"))
    # Hard check: userinfo (Auth & Quota)
    ui = pc.userinfo(cfg)
    if not ui or ui.get("result") != 0:
        print("DOWN"); sys.exit(0)
    quota = ui.get("quota", 0); used = ui.get("usedquota", 0)
    if quota and used and used >= quota:
        print("OVERQUOTA"); sys.exit(0)

    # Lightweight sanity: list root folder (metadata fetch)
    md = pc.listfolder(cfg, path="/", recursive=False, nofiles=True, showpath=False)
    if not md or md.get("result") not in (0, None):
        print("DOWN"); sys.exit(0)
    print("OK")
except Exception as e:
    # Netzwerk/5000/etc.
    print("DOWN")
PY
}



# ========================================================================
# ========= KRITISCH: Lock-Acquire (mit Timeout für RTB-Wartezeit) ======
# ========================================================================
exec 9>"$LOCKFILE"
if ! flock -w "$WAIT_SEC" 9; then
  log "[skip] Konnte Lock innerhalb ${WAIT_SEC}s nicht bekommen (RTB läuft zu lange oder anderer Sync)."
  exit 0
fi
# Ab hier: Lock ist AKTIV und wird bis Script-Ende gehalten!
# RTB kann NICHT mehr starten (nutzt gleiches Lock)
# ========================================================================

log "[start] pcloud_sync_1to1"

validate_inputs_or_exit
PF="$(preflight_or_mark_down)"
case "$PF" in
  OK)        log "[ok] pCloud Preflight ok";;
  OVERQUOTA) log "[warn] pCloud Preflight: Konto über Quota – Sync wird übersprungen."; exit 0;;
  DOWN)      log "[warn] pCloud Preflight: API/Auth nicht erreichbar – Sync wird übersprungen."; exit 0;;
  *)         log "[warn] pCloud Preflight: unbekannter Status '$PF' – Sync wird übersprungen."; exit 0;;
esac

# Safety-Delay nach RTB (nur Sleep, kein Exit)
if [[ -L "${RTB}/latest" || -d "${RTB}/latest" ]]; then
  latest_dir="$(readlink -f "${RTB}/latest" 2>/dev/null || echo "")"
  if [[ -n "$latest_dir" && -d "$latest_dir" ]]; then
    now=$(date +%s); lm=$(stat -c '%Y' "$latest_dir" 2>/dev/null || echo 0)
    if (( lm > 0 && now - lm < SAFETY_DELAY_SEC )); then
      wait=$(( SAFETY_DELAY_SEC - (now - lm) ))
      log "[info] safety-delay ${wait}s (warte nach RTB)"
      sleep "$wait"
    fi
  fi
fi

# ========================================================================
# Bootstrap-Modus: Remote komplett leer → alle Snapshots hochladen
# ========================================================================
RHAS=$(remote_has_snapshots)
if [[ "$RHAS" == "NO" ]]; then
  log "[bootstrap] Remote leer erkannt – backfill aller lokalen Snapshots (alt → neu)"
  # Hinweis: An dieser Stelle sind wir bereits durch Preflight OK gegangen.
  mapfile -t SNAPS < <(find "$RTB" -maxdepth 1 -type d -printf '%f\n' | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}-' | sort)
  if [[ ${#SNAPS[@]} -eq 0 ]]; then
    log "[skip] keine lokalen Snapshots gefunden."
    exit 0
  fi
  
  # Während Backfill: Finalize überspringen (Performance)
  export PCLOUD_SKIP_FINALIZE=1

  for s in "${SNAPS[@]}"; do
    build_and_push "$RTB/$s"
  done
  
  # Zum Schluss einmalig finalisieren
  "${PY}" - <<'PY'
import pcloud_bin_lib as pc
import os
cfg = pc.effective_config(env_file=os.environ.get("ENV_FILE"))
dest_root = os.environ.get("PCLOUD_DEST","/Backup/rtb_1to1")
snapshots_root = f"{pc._norm_remote_path(dest_root).rstrip('/')}/_snapshots"
from pcloud_push_json_manifest_to_pcloud import finalize_index_fileids
fixed = finalize_index_fileids(cfg, snapshots_root)
print(f"[finalize] index fileids fixed={fixed}")
PY
  
  _REMOTE_SNAPS_LOADED=0
  
  log "[done] bootstrap/backfill abgeschlossen."
  exit 0
fi

# ========================================================================
# Normalfall: Nur latest-Snapshot syncen (mit Deduplizierungs-Check)
# ========================================================================
SNAP="$(readlink -f "${RTB}/latest" 2>/dev/null || true)"
if [[ -z "$SNAP" || ! -d "$SNAP" ]]; then 
    log "[skip] kein lokaler Snapshot gefunden."
    exit 0
fi

SNAPNAME="$(basename "$SNAP")"

if [[ "$(remote_snapshot_exists "$SNAPNAME")" == "YES" ]]; then
    log "[skip] Snapshot $SNAPNAME bereits auf pCloud vorhanden - nichts zu tun."
    exit 0
fi

build_and_push "$SNAP"

log "[done] pcloud_sync_1to1"
# Lock wird HIER automatisch freigegeben (FD 9 wird geschlossen beim exit)
