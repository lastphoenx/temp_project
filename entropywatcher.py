#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EntropyWatcher – gehärteter Entropie-/Integritätswächter (Raspberry Pi / Debian Bookworm)

- immer in der Umgebung starten: source /opt/entropywatcher/venv/bin/activate

- Baseline (Start-Entropie) einmalig; prev/last-Verlauf mit Zeitstempeln
- Nur neue/geänderte Dateien scannen; Härtung via Quick-Fingerprint + periodische Vollverifikation
- Hybrid-Entropie: NumPy im Prozess; optional 'ent' (C) ab ENT_THRESHOLD, mit Timeout
- Filter: Excludes/Score-Excludes per glob ODER regex; "score_exempt" pro File
- Logging: zentral + multiprocess-fähig via QueueHandler
- Parallelisierung: Heavy-Jobs (Hash/Entropie/Quick-FP) parallel; DB-Writes zentral
- Export: CSV/JSON aus dem Report
- Pfad-Key: VARBINARY(1024) (UTF-8-Bytes), robust gegen 4-Byte-Unicode/Emoji
"""

import os, sys, stat, importlib.util
from pathlib import Path

# 1) Zielinterpreter bestimmen (ENV überschreibt Default)
_VENV_DIR = Path("/opt/entropywatcher/venv")
_EXPECTED_PY_ENV = os.environ.get("ENTROPYWATCHER_VENV_PY", "")
_CANDIDATES = []
if _EXPECTED_PY_ENV:
    _CANDIDATES.append(Path(_EXPECTED_PY_ENV))
# gängige Kandidaten:
_CANDIDATES += [
    _VENV_DIR / "bin" / "python3",
    _VENV_DIR / "bin" / "python",
]

def _is_executable(p: Path) -> bool:
    try:
        st = p.stat()
        return bool(st.st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))
    except FileNotFoundError:
        return False

def _in_venv() -> bool:
    # In venv: sys.prefix != sys.base_prefix
    return getattr(sys, "prefix", "") != getattr(sys, "base_prefix", "")

# 2) Falls NICHT in venv: in vorhandenen venv-Python reexecen
if not _in_venv():
    for cand in _CANDIDATES:
        if _is_executable(cand):
            sys.stderr.write(f"[EntropyWatcher] Starte neu in venv: {cand}\n")
            os.execv(str(cand), [str(cand)] + sys.argv)
    # Kein Interpreter gefunden → weiterlaufen, aber später klar meckern

# 3) Abhängigkeitsprüfung (nur Verfügbarkeit checken; noch keine echten Imports)
def _check_deps_or_exit():
    deps = {
        "click": "click",
        "dotenv": "python-dotenv",
        "mysql.connector": "mysql-connector-python",
        "numpy": "numpy",
    }
    missing = []
    for mod, pkg in deps.items():
        if importlib.util.find_spec(mod) is None:
            missing.append((mod, pkg))
    if missing:
        sys.stderr.write("Fehlende Python-Pakete:\n")
        for mod, pkg in missing:
            sys.stderr.write(f"  - Modul '{mod}' (pip Paket: {pkg})\n")
        # Beste Startweise vorschlagen (venv-Python direkt)
        suggested = str((_VENV_DIR / "bin" / ("python3" if (_VENV_DIR / "bin" / "python3").exists() else "python")).resolve())
        sys.stderr.write(
            "\nBitte venv aktivieren und installieren:\n"
            "  source /opt/entropywatcher/venv/bin/activate\n"
            "  pip install -r /opt/entropywatcher/requirements.txt\n"
            "\nOder starte direkt mit der venv:\n"
            f"  {suggested} " + " ".join(sys.argv) + "\n"
        )
        sys.exit(2)

_check_deps_or_exit()

# --- erst JETZT deine normalen Imports ---
import stat, fnmatch, hashlib, datetime, subprocess, shlex,  shutil, re, logging, logging.handlers, queue, unicodedata
import click
import mysql.connector as mariadb
import numpy as np
import time
import smtplib, ssl
import socket
import textwrap
import json
import datetime
import re
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
from email.message import EmailMessage
from zoneinfo import ZoneInfo

# lokale Zeitstempel in CLI-Ausgabe-Protokoll, etc
def _now_local(cfg: Dict[str, Any]) -> datetime.datetime:
    """Lokale Zeit (tz-aware) für Anzeigen/Mails; Standard Europe/Zurich."""
    tzname = (cfg.get("TZ") or "Europe/Zurich").strip() if isinstance(cfg, dict) else "Europe/Zurich"
    try:
        tz = ZoneInfo(tzname)
    except Exception:
        tz = ZoneInfo("Europe/Zurich")
    return datetime.datetime.now(tz)

# Zeitstempel sicherstellen
def now_db() -> datetime.datetime:
    """UTC, auf Sekunden gerundet – passt zu MariaDB DATETIME ohne Mikrosekunden."""
    return datetime.datetime.utcnow().replace(microsecond=0)

# --- Freundliche Hinweise zur Umgebung/venv ---
_EXPECTED_PY = os.environ.get("ENTROPYWATCHER_VENV_PY", "/opt/entropywatcher/venv/bin/python")
if os.path.exists(_EXPECTED_PY) and os.path.realpath(sys.executable) != os.path.realpath(_EXPECTED_PY):
    sys.stderr.write(
        f"Warnung: Dieses Skript läuft nicht aus der venv ({sys.executable}).\n"
        f"Empfohlen: {_EXPECTED_PY}\n"
        "Tipp: source /opt/entropywatcher/venv/bin/activate\n"
    )

try:
    from dotenv import load_dotenv, dotenv_values
except ModuleNotFoundError:
    sys.stderr.write(
        "Fehlendes Paket 'python-dotenv'.\n"
        "Bitte Umgebung aktivieren oder installieren:\n"
        "  source /opt/entropywatcher/venv/bin/activate\n"
        "  pip install python-dotenv\n"
        "(Oder das Skript immer mit /opt/entropywatcher/venv/bin/python aufrufen.)\n"
    )
    sys.exit(2)

# -----------------------
# mehrere .env Dateien nacheinander laden
# -----------------------

def _launched_by_systemd() -> bool:
    # systemd setzt i.d.R. mindestens einen dieser Marker
    return any(k in os.environ for k in ("INVOCATION_ID", "JOURNAL_STREAM", "NOTIFY_SOCKET"))

def _load_env_chain(paths: list[str], override: bool) -> tuple[list[str], list[tuple[str, str, str, str]], list[tuple[str, str, str]]]:
    """
    Lädt .env-Dateien in Reihenfolge.
    override=False  → existierende os.environ-Werte bleiben bestehen
    override=True   → spätere Dateien dürfen frühere Werte überschreiben

    Rückgabe:
      loaded_files:  Liste geladener Dateien
      overrides:     Liste (key, old, new, file) für tatsächlich überschrieben
      sets:          Liste (key, new, file) für frisch gesetzte Variablen
    """
    loaded_files: list[str] = []
    overrides: list[tuple[str, str, str, str]] = []
    sets: list[tuple[str, str, str]] = []

    for p in paths:
        if not p:
            continue
        try:
            if not os.path.exists(p):
                continue
            vals = dotenv_values(p)  # parst ohne zu exportieren
            for k, v in vals.items():
                if v is None:
                    continue
                if k in os.environ:
                    if override:
                        old = os.environ[k]
                        if old != v:
                            os.environ[k] = v
                            overrides.append((k, old, v, p))
                    else:
                        # nicht überschreiben
                        pass
                else:
                    os.environ[k] = v
                    sets.append((k, v, p))
            loaded_files.append(p)
        except PermissionError:
            logging.warning("Kann %s nicht lesen (Permission). Überspringe.", p)
    return loaded_files, overrides, sets

# -----------------------
# Config & Validation
# -----------------------

def _as_bool(v: str) -> bool:
    """Robustes Bool-Parsing aus Strings."""
    return str(v).strip().lower() in ("1","true","yes","y","on")

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name, str(default))
    try:
        v = str(v).split("#", 1)[0].strip()  # Inline-Kommentar abschneiden
        return int(v)
    except Exception:
        return int(default)

def _env_float(name: str, default: float) -> float:
    v = os.getenv(name, str(default))
    try:
        v = str(v).split("#", 1)[0].strip()
        return float(v)
    except Exception:
        return float(default)

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name, default)
    return str(v).split("#", 1)[0].strip()

def _parse_paths_arg(paths_arg: str) -> list[str]:
    """Kommagetrennte Pfade aus --paths robust parsen; leere Einträge verwerfen."""
    items = [p.strip() for p in (paths_arg or "").split(",")]
    return [p for p in items if p]

def load_config(ctx=None, command_name: str = "") -> tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Ladelogik gemäß deiner Regeln:
      • systemd: keine --env zulässig (nur EnvironmentFile= aus Unit). Abbruch, falls dennoch angegeben.
      • CLI: mindestens eine --env Pflicht; diese Dateien werden in Reihenfolge mit override=True geladen.
    Rückgabe: (cfg, trace) – trace protokolliert geladene Dateien / Overrides für späteres Logging & DB-Audit.
    """
    systemd   = bool(ctx and ctx.obj.get("systemd"))
    env_files = (ctx.obj.get("env_files") if ctx else []) or []

    loaded_base: list[str] = []
    ov_base: list[tuple[str,str,str,str]] = []
    set_base: list[tuple[str,str,str]] = []
    loaded_extra: list[str] = []
    ov_extra: list[tuple[str,str,str,str]] = []
    set_extra: list[tuple[str,str,str]] = []

    # — Regel 1: Unter systemd sind zusätzliche --env verboten
    if systemd and env_files:
        sys.stderr.write(
            "Fehler: Unter systemd sind zusätzliche --env Dateien nicht erlaubt.\n"
            "Pflege stattdessen EnvironmentFile= in der .service-Unit.\n"
        )
        # (Optional) Mail schicken, falls Mail-Settings schon aus der Umgebung vorhanden:
        try:
            tmp_cfg = {
                "MAIL_ENABLE": str(os.getenv("MAIL_ENABLE","0")).lower() in ("1","true","yes"),
                "MAIL_SMTP_HOST": os.getenv("MAIL_SMTP_HOST",""),
                "MAIL_SMTP_PORT": int(os.getenv("MAIL_SMTP_PORT","587")),
                "MAIL_STARTTLS": str(os.getenv("MAIL_STARTTLS","1")).lower() in ("1","true","yes"),
                "MAIL_SSL": str(os.getenv("MAIL_SSL","0")).lower() in ("1","true","yes"),
                "MAIL_USER": os.getenv("MAIL_USER",""),
                "MAIL_PASS": os.getenv("MAIL_PASS",""),
                "MAIL_FROM": os.getenv("MAIL_FROM","entropywatcher@localhost"),
                "MAIL_TO": os.getenv("MAIL_TO",""),
                "MAIL_SUBJECT_PREFIX": os.getenv("MAIL_SUBJECT_PREFIX","[EntropyWatcher]"),
                "MAIL_MIN_ALERT_INTERVAL_MIN": int(os.getenv("MAIL_MIN_ALERT_INTERVAL_MIN","30")),
                "ALERT_STATE_FILE": os.getenv("ALERT_STATE_FILE","/var/lib/entropywatcher/last_alert.txt"),
            }
            if tmp_cfg["MAIL_ENABLE"] and tmp_cfg["MAIL_SMTP_HOST"] and tmp_cfg["MAIL_TO"]:
                send_alert_email(
                    subject_core=f"Verbotene --env unter systemd ({command_name})",
                    body="Das Skript wurde unter systemd mit --env gestartet. "
                         "Bitte die Unit via EnvironmentFile= konfigurieren.",
                    cfg=tmp_cfg
                )
        except Exception:
            pass
        sys.exit(2)

    # — Regel 2: Bei CLI muss mind. eine --env angegeben werden (keine impliziten Defaults)
    info_only = command_name in ("help-examples",)
    if (not systemd) and (not info_only) and (not env_files):
        sys.stderr.write(
            "Fehler: Bei CLI-Aufruf muss mindestens eine --env angegeben werden.\n"
            "Beispiel:\n"
            "  entropywatcher.py --env /opt/entropywatcher/common.env "
            "--env /opt/entropywatcher/nas.env scan --paths \"/srv/nas\"\n"
        )
        sys.exit(2)

    # — CLI: die angegebenen Dateien in Reihenfolge laden (spätere gewinnen)
    if (not systemd) and env_files:
        loaded_extra, ov_extra, set_extra = _load_env_chain(env_files, override=True)

    # — Ab hier: Konfiguration aus der (jetzt vorbereiteten) Umgebung lesen
    cfg: Dict[str, Any] = {
        # DB
        "DB_HOST": os.getenv("DB_HOST","localhost"),
        "DB_PORT": _env_int("DB_PORT", 3306),
        "DB_NAME": os.getenv("DB_NAME","entropywatcher"),
        "DB_USER": os.getenv("DB_USER","entropyuser"),
        "DB_PASS": os.getenv("DB_PASS",""),

        # Filter
        "EXCLUDES": [e.strip() for e in _env_str("EXCLUDES","").split(",") if e.strip()],
        "SCORE_EXCLUDES": [e.strip() for e in _env_str("SCORE_EXCLUDES","").split(",") if e.strip()],
        "EXCLUDES_MODE": _env_str("EXCLUDES_MODE","glob"),
        "SCORE_EXCLUDES_MODE": _env_str("SCORE_EXCLUDES_MODE","glob"),

        # Größen
        "MIN_SIZE": _env_int("MIN_SIZE", 1),
        "MAX_SIZE": _env_int("MAX_SIZE", 0),

        # Engine & Performance
        "USE_ENT": _as_bool(os.getenv("USE_ENT","1")),
        "ENT_THRESHOLD": _env_int("ENT_THRESHOLD", 1048576),
        "ENT_TIMEOUT": _env_int("ENT_TIMEOUT", 30),
        "CHUNK_SIZE": _env_int("CHUNK_SIZE", 4194304),
        "WORKERS": _env_int("WORKERS", 3),

        # Heuristik
        "ALERT_ENTROPY_ABS": _env_float("ALERT_ENTROPY_ABS", 7.8),
        "ALERT_ENTROPY_JUMP": _env_float("ALERT_ENTROPY_JUMP", 1.0),

        # Härtung
        "QUICK_FINGERPRINT": _as_bool(os.getenv("QUICK_FINGERPRINT","1")),
        "QUICK_FP_SAMPLE": _env_int("QUICK_FP_SAMPLE", 65536),
        "PERIODIC_REVERIFY_DAYS": _env_int("PERIODIC_REVERIFY_DAYS", 7),

        # Logging
        "LOG_LEVEL": _env_str("LOG_LEVEL","INFO").upper(),
        "LOG_FILE": os.getenv("LOG_FILE",""),
        "SOURCE_LABEL": os.getenv("SOURCE_LABEL","").strip(),
        "TZ": os.getenv("TZ", "Europe/Zurich"),
    }

    # Mail
    cfg.update({
        "MAIL_ENABLE": _as_bool(os.getenv("MAIL_ENABLE","0")),
        "MAIL_SMTP_HOST": _env_str("MAIL_SMTP_HOST",""),
        "MAIL_SMTP_PORT": _env_int("MAIL_SMTP_PORT", 587),
        "MAIL_STARTTLS": _as_bool(os.getenv("MAIL_STARTTLS","1")),
        "MAIL_SSL": _as_bool(os.getenv("MAIL_SSL","0")),
        "MAIL_USER": os.getenv("MAIL_USER",""),
        "MAIL_PASS": os.getenv("MAIL_PASS",""),
        "MAIL_FROM": _env_str("MAIL_FROM","entropywatcher@localhost"),
        "MAIL_TO": _env_str("MAIL_TO",""),
        "MAIL_SUBJECT_PREFIX": _env_str("MAIL_SUBJECT_PREFIX","[EntropyWatcher]"),
        "MAIL_MIN_ALERT_INTERVAL_MIN": _env_int("MAIL_MIN_ALERT_INTERVAL_MIN", 30),
        "ALERT_STATE_FILE": os.getenv("ALERT_STATE_FILE","/var/lib/entropywatcher/last_alert.txt"),
    })

    # ClamAV
    cfg.update({
        "CLAMAV_ENABLE": _as_bool(os.getenv("CLAMAV_ENABLE","0")),
        "CLAMAV_USE_CLAMD": _as_bool(os.getenv("CLAMAV_USE_CLAMD","0")),
        "CLAMAV_EXCLUDES": [e.strip() for e in _env_str("CLAMAV_EXCLUDES","").split(",") if e.strip()],
        "CLAMAV_EXCLUDES_MODE": _env_str("CLAMAV_EXCLUDES_MODE","glob"),
        "CLAMAV_MAX_FILESIZE_MB": _env_int("CLAMAV_MAX_FILESIZE_MB", 0),
        "CLAMAV_THREADS": _env_int("CLAMAV_THREADS", 2),
        "CLAMAV_TIMEOUT": _env_int("CLAMAV_TIMEOUT", 1800),

        # (deine) Quarantäne-Settings bleiben erhalten
        "AV_QUARANTINE_DIR": _env_str("AV_QUARANTINE_DIR", "/srv/nas/av-quarantine"),
        "AV_QUARANTINE_ACTION": _env_str("AV_QUARANTINE_ACTION", "move"),
        "AV_QUARANTINE_CHMOD": _env_str("AV_QUARANTINE_CHMOD", "000"),
    })

    # --- Validation ---
    if cfg["ENT_THRESHOLD"] < 0: raise ValueError("ENT_THRESHOLD muss >= 0 sein")
    if cfg["ENT_TIMEOUT"] <= 0: raise ValueError("ENT_TIMEOUT muss > 0 sein")
    if cfg["CHUNK_SIZE"] <= 0: raise ValueError("CHUNK_SIZE muss > 0 sein")
    if cfg["QUICK_FP_SAMPLE"] < 0: raise ValueError("QUICK_FP_SAMPLE muss >= 0 sein")
    if cfg["PERIODIC_REVERIFY_DAYS"] < 0: raise ValueError("PERIODIC_REVERIFY_DAYS muss >= 0 sein")
    if cfg["MIN_SIZE"] < 0: raise ValueError("MIN_SIZE muss >= 0 sein")
    if cfg["MAX_SIZE"] < 0: raise ValueError("MAX_SIZE muss >= 0 sein (0 = kein Limit)")
    if cfg["WORKERS"] < 1: cfg["WORKERS"] = 1
    if cfg["EXCLUDES_MODE"] not in ("glob", "regex"): cfg["EXCLUDES_MODE"] = "glob"
    if cfg["SCORE_EXCLUDES_MODE"] not in ("glob", "regex"): cfg["SCORE_EXCLUDES_MODE"] = "glob"

    # --- Ladespur für späteres Logging zurückgeben ---
    trace: Dict[str, Any] = {
        "systemd": systemd,
        "loaded_base": loaded_base,       # hier leer (wir laden keine Defaults mehr)
        "loaded_extra": loaded_extra,     # die per CLI angegebenen Dateien
        "overrides": ov_base + ov_extra,  # von _load_env_chain geliefert
        "sets": set_base + set_extra,     # 
    }

    return cfg, trace


# -----------------------
# Logging der Ladespur (mit Redaction)
# -----------------------

_SENSITIVE_KEY_SUBSTRINGS = ("PASS", "PASSWORD", "SECRET", "TOKEN", "KEY", "AUTH", "API")
_SENSITIVE_KEYS = {
    "DB_PASS", "MAIL_PASS",
}

def _redact(k: str, v: str) -> str:
    k_up = (k or "").upper()
    if k_up in _SENSITIVE_KEYS or any(s in k_up for s in _SENSITIVE_KEY_SUBSTRINGS):
        return "***"
    return v

def _log_load_trace(trace: Dict[str, Any]) -> None:
    try:
        logging.info("ENV geladen (systemd=%s):", trace.get("systemd"))
        lb = trace.get("loaded_base") or []
        le = trace.get("loaded_extra") or []
        if lb: logging.info("  Basis: %s", ", ".join(lb))
        if le: logging.info("  Extra: %s", ", ".join(le))
        for k, old, new, src in trace.get("overrides") or []:
            logging.info(
                "  Override: %s  '%s' → '%s'  aus %s",
                k, _redact(k, str(old)), _redact(k, str(new)), src
            )
        for k, new, src in trace.get("sets") or []:
            logging.info("  Set: %s = '%s'  aus %s", k, _redact(k, str(new)), src)
    except Exception:
        # Logging darf nie crashen
        pass

# -----------------------
# Multiprocess Logging
# -----------------------

def setup_logging(cfg: Dict[str, Any]):
    """Initialisiert Logging mit QueueHandler. Rückgabe: (logger, queue, listener)."""
    lvl = getattr(logging, str(cfg["LOG_LEVEL"]).upper(), logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(lvl)
    logger.propagate = False  # sonst doppelte Ausgabe bei Root-Logger

    # vorhandene Handler entfernen
    for h in list(logger.handlers):
        logger.removeHandler(h)

    # Queue + QueueHandler (Producer)
    log_queue = queue.Queue()
    queue_handler = logging.handlers.QueueHandler(log_queue)
    logger.addHandler(queue_handler)

    # Label-Filter: fügt 'source_label' in jeden Log-Record ein
    class _LabelFilter(logging.Filter):
        def __init__(self, label: str):
            super().__init__()
            self._label = label or "-"

        def filter(self, record):
            if not hasattr(record, "source_label"):
                record.source_label = self._label
            return True

    # finale Handler (Consumer), z. B. Stream oder File
    handlers: List[logging.Handler] = []
    if cfg["LOG_FILE"]:
        try:
            log_dir = os.path.dirname(cfg["LOG_FILE"])
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            handlers.append(logging.FileHandler(cfg["LOG_FILE"], encoding="utf-8"))
        except Exception:
            handlers.append(logging.StreamHandler())
    else:
        handlers.append(logging.StreamHandler())

    # Logformat MIT Label
    # logging.Formatter.converter = time.gmtime  # falls du UTC-Ausgabe willst
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(source_label)s] %(message)s")
    for h in handlers:
        h.setFormatter(formatter)

    # >>> HIER: Filter instanziieren und registrieren
    label_filter = _LabelFilter(cfg.get("SOURCE_LABEL"))

    # Filter an Root-Logger (wirkt vor dem QueueHandler)
    logger.addFilter(label_filter)
    # und zusätzlich an die finalen Handler (robust, falls Records später noch verändert werden)
    for h in handlers:
        h.addFilter(label_filter)

    # QueueListener (Consumer) starten
    listener = logging.handlers.QueueListener(log_queue, *handlers, respect_handler_level=True)
    listener.start()
    return logger, log_queue, listener

def teardown_logging(listener):
    """Listener sicher stoppen (keinen Crash beim Shutdown riskieren)."""
    try:
        if listener:
            listener.stop()
    except Exception:
        pass

# -----------------------
# AV-Events & Scan-Summary: Helper
# -----------------------

def record_av_event(conn, cfg: Dict[str, Any], path: str, signature: str,
                    engine: str, action: str = "none", quarantine_path: Optional[str] = None,
                    extra: Optional[dict] = None) -> None:
    """Schreibt einen ClamAV-Fund in die Tabelle av_events (idempotent via INSERT IGNORE)."""
    try:
        # (Optional) Whitelist
        whitelisted = False
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM av_whitelist WHERE signature=%s LIMIT 1", (signature,))
                whitelisted = cur.fetchone() is not None
        except Exception:
            pass

        if whitelisted:
            logging.info("AV whitelist: %s auf %s – Event unterdrückt", signature, path)
            return

        with conn.cursor() as cur:
            cur.execute("""
                INSERT IGNORE INTO av_events
                  (detected_at, source, path, signature, engine, action, quarantine_path, extra)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                now_db(),
                cfg.get("SOURCE_LABEL") or None,
                path,
                signature,
                engine,
                action,
                quarantine_path,
                (json.dumps(extra, ensure_ascii=False) if extra else None)
            ))
        conn.commit()
    except Exception:
        logging.exception("record_av_event failed: %s (%s)", path, signature)

def write_scan_summary(conn,
                       cfg: Dict[str, Any],
                       started_at: datetime.datetime,
                       finished_at: datetime.datetime,
                       candidates_count: int,
                       files_processed: int,
                       bytes_processed: int,
                       flagged_new_count: int,
                       changed_count: int,
                       reverified_count: int,
                       note: Optional[str] = None,
                       scan_paths: Optional[str] = None) -> None:
    """Aggregiert Kennzahlen und schreibt einen Datensatz in scan_summary (inkl. scan_paths/note)."""
    try:
        src = cfg.get("SOURCE_LABEL") or None
        with conn.cursor() as cur:
            # missing_count
            if src is None:
                cur.execute("SELECT COUNT(*) FROM files WHERE missing_since IS NOT NULL")
            else:
                cur.execute("SELECT COUNT(*) FROM files WHERE source=%s AND missing_since IS NOT NULL", (src,))
            missing_count = int((cur.fetchone() or [0])[0])

            # flagged_total_after
            if src is None:
                cur.execute("SELECT COUNT(*) FROM files WHERE flagged=1")
            else:
                cur.execute("SELECT COUNT(*) FROM files WHERE source=%s AND flagged=1", (src,))
            flagged_total_after = int((cur.fetchone() or [0])[0])

            # AV-Tageszahlen (einfach gehalten)
            try:
                if src is None:
                    cur.execute("SELECT COUNT(*) FROM av_events WHERE DATE(detected_at)=UTC_DATE()")
                    av_found_count = int((cur.fetchone() or [0])[0])
                    cur.execute("SELECT COUNT(*) FROM av_events WHERE action='quarantine' AND DATE(detected_at)=UTC_DATE()")
                    av_quarantined_count = int((cur.fetchone() or [0])[0])
                else:
                    cur.execute("SELECT COUNT(*) FROM av_events WHERE source=%s AND DATE(detected_at)=UTC_DATE()", (src,))
                    av_found_count = int((cur.fetchone() or [0])[0])
                    cur.execute("SELECT COUNT(*) FROM av_events WHERE source=%s AND action='quarantine' AND DATE(detected_at)=UTC_DATE()", (src,))
                    av_quarantined_count = int((cur.fetchone() or [0])[0])
            except Exception:
                av_found_count = 0
                av_quarantined_count = 0

            # Insert
            cur.execute("""
                INSERT INTO scan_summary
                  (source, scan_paths, started_at, finished_at, candidates, files_processed, bytes_processed,
                   flagged_new_count, flagged_total_after, missing_count,
                   changed_count, reverified_count,
                   av_found_count, av_quarantined_count, note)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                src, (scan_paths or None), started_at, finished_at,
                int(candidates_count), int(files_processed), int(bytes_processed),
                int(flagged_new_count), int(flagged_total_after), int(missing_count),
                int(changed_count), int(reverified_count),
                int(av_found_count), int(av_quarantined_count),
                note
            ))

        conn.commit()
        logging.info("scan_summary geschrieben: flagged_new=%d total_after=%d missing=%d",
                     flagged_new_count, flagged_total_after, missing_count)
    except Exception:
        logging.exception("write_scan_summary failed")


# -----------------------
# DB
# -----------------------

DDL = """
CREATE TABLE IF NOT EXISTS files (
  path VARBINARY(1024) PRIMARY KEY,
  source VARCHAR(16) NULL,
  inode BIGINT UNSIGNED,
  size BIGINT,
  mtime_ns BIGINT,
  sha256 BINARY(32),

  start_entropy DOUBLE,
  start_time DATETIME,

  prev_entropy DOUBLE,
  prev_time DATETIME,

  last_entropy DOUBLE,
  last_time DATETIME,

  scans INT DEFAULT 0,
  flagged TINYINT(1) DEFAULT 0,
  note VARCHAR(255),
  missing_since DATETIME NULL,

  score_exempt TINYINT(1) DEFAULT 0,
  tags VARCHAR(255) NULL,
  quick_md5 BINARY(16) NULL,
  last_full_verify DATETIME NULL
) ENGINE=InnoDB;

CREATE INDEX IF NOT EXISTS idx_mtime ON files (mtime_ns);

CREATE TABLE IF NOT EXISTS scan_summary (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  source VARCHAR(32),
  scan_paths TEXT,
  started_at DATETIME NOT NULL,
  finished_at DATETIME NOT NULL,
  candidates INT NOT NULL,
  files_processed INT NOT NULL,
  bytes_processed BIGINT NOT NULL,
  flagged_new_count INT NOT NULL,
  flagged_total_after INT NOT NULL,
  missing_count INT NOT NULL,
  changed_count INT NOT NULL,
  reverified_count INT NOT NULL,
  av_found_count INT NOT NULL,
  av_quarantined_count INT NOT NULL,
  note TEXT,
  KEY (source, finished_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS av_events (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  detected_at DATETIME NOT NULL,
  source VARCHAR(32),
  path TEXT NOT NULL,
  signature VARCHAR(255) NOT NULL,
  engine VARCHAR(32) NOT NULL,
  action VARCHAR(32) NOT NULL,
  quarantine_path TEXT NULL,
  extra JSON NULL,
  UNIQUE KEY uniq_event (detected_at, signature(120), engine, path(255))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS av_whitelist (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  signature VARCHAR(255) NOT NULL,
  pattern TEXT NULL,
  reason TEXT NULL,
  UNIQUE KEY uniq_sig (signature(120))
) ENGINE=InnoDB;
"""

# --- DB Helper ---
def db_connect(cfg: Dict[str, Any]):
    """
    Liefert eine MariaDB/MySQL-Verbindung gemäß cfg.
    Erwartet Keys: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS.
    """
    return mariadb.connect(
        host=cfg["DB_HOST"],
        port=int(cfg["DB_PORT"]),
        database=cfg["DB_NAME"],
        user=cfg["DB_USER"],
        password=cfg["DB_PASS"],
        autocommit=False,
    )

def ensure_schema(conn) -> None:
    cur = conn.cursor()
    for stmt in [s.strip() for s in DDL.split(";") if s.strip()]:
        cur.execute(stmt)
    cur.close()

# -----------------------
# Path<->DB-Key
# -----------------------

def db_key_from_path(p: Path | str) -> bytes:
    """
    VARBINARY-Key: UTF-8-Bytes des NFC-normalisierten Pfads.
    Robust gegen 4-Byte-Unicode/Emoji (keine Collation).
    """
    s = str(p)
    s = unicodedata.normalize("NFC", s)
    return s.encode("utf-8")

def db_key_to_display(b: bytes) -> str:
    """Bytes → UTF-8-String für Anzeige (Ersatzzeichen bei Fehlern)."""
    return b.decode("utf-8", errors="replace")

# -----------------------
# Pattern Matching
# -----------------------

def _compile_patterns(patterns: List[str], mode: str):
    if mode == "regex":
        return [re.compile(p) for p in patterns]
    return patterns

def _match(path: Path, compiled, mode: str) -> bool:
    s, n = str(path), path.name
    if mode == "regex":
        return any(r.search(s) or r.search(n) for r in compiled)
    else:
        return any(fnmatch.fnmatch(n, p) or fnmatch.fnmatch(s, p) for p in compiled)

# -----------------------
# FS & Hash/Entropy
# -----------------------

def file_iter(dirs: List[str]):
    """Yieldet Dateien rekursiv aus den übergebenen Verzeichnissen."""
    seen = set()
    for root in dirs:
        root = os.path.abspath(root.strip().rstrip("/"))
        if not root or root in seen:
            continue
        seen.add(root)
        if not os.path.exists(root):
            logging.warning("SCAN_PATH nicht gefunden: %s", root)
            continue
        for base, _, files in os.walk(root, followlinks=False):
            for f in files:
                yield Path(os.path.join(base, f))

def should_skip(path: Path, cfg, exc_patterns, exc_mode) -> bool:
    """Schneller Skip: Typ/Größe/Glob/Regex vor inhaltlichem Lesen prüfen."""
    try:
        st = path.stat()
    except FileNotFoundError:
        return True
    except Exception:
        logging.warning("Stat-Fehler: %s", path, exc_info=True)
        return True

    if not stat.S_ISREG(st.st_mode):
        return True
    if cfg["MIN_SIZE"] and st.st_size < cfg["MIN_SIZE"]:
        return True
    if cfg["MAX_SIZE"] and cfg["MAX_SIZE"] > 0 and st.st_size > cfg["MAX_SIZE"]:
        return True
    if _match(path, exc_patterns, exc_mode):
        return True
    return False

def is_score_excluded(path: Path, cfg, sexc_patterns, sexc_mode) -> bool:
    """True, wenn Datei gemessen, aber nicht fürs Scoring gewertet werden soll."""
    return _match(path, sexc_patterns, sexc_mode)

def sha256_of(path: Path, chunk: int) -> bytes:
    """Vollständiger SHA-256 (chunked)."""
    h = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            b = fp.read(chunk)
            if not b: break
            h.update(b)
    return h.digest()

def quick_md5_head_tail(path: Path, sample: int) -> bytes:
    """Head+Tail-Sample (MD5) als Quick-Fingerprint (robust gegen mtime-Fakes)."""
    size = os.path.getsize(path)
    if sample <= 0 or size == 0:
        return hashlib.md5(b"").digest()
    head = min(sample, size)
    tail = min(sample, max(0, size - head))
    m = hashlib.md5()
    with path.open("rb") as f:
        m.update(f.read(head))
        if tail > 0:
            f.seek(size - tail)
            m.update(f.read(tail))
    return m.digest()

def entropy_numpy(path: Path, chunk: int) -> float:
    """Schnelle Shannon-Entropie mit NumPy (bits/Byte)."""
    counts = np.zeros(256, dtype=np.int64)
    n = 0
    with path.open("rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            arr = np.frombuffer(b, dtype=np.uint8)
            n += arr.size
            counts[:256] += np.bincount(arr, minlength=256)
    if n == 0:
        return 0.0
    p = counts[counts > 0] / n
    return float(-(p * np.log2(p)).sum())

def entropy_with_ent(path: Path, timeout_s: int) -> Optional[float]:
    """Ruft 'ent -c' mit Timeout auf; gibt None bei Fehler/Timeout."""
    try:
        out = subprocess.check_output(["ent", "-c", str(path)],
                                      stderr=subprocess.STDOUT, text=True,
                                      timeout=timeout_s)
        for line in out.splitlines():
            if "Entropy =" in line and "bits per byte" in line:
                return float(line.split("=")[1].split("bits")[0].strip())
    except Exception:
        return None
    return None

def entropy_of(path: Path, cfg) -> float:
    """Hybrid: ab Threshold 'ent' (falls vorhanden), sonst NumPy."""
    size = os.path.getsize(path)
    use_ent = cfg["USE_ENT"] and shutil.which("ent") and (size >= cfg["ENT_THRESHOLD"])
    if use_ent:
        val = entropy_with_ent(path, cfg["ENT_TIMEOUT"])
        if val is not None:
            return val
    return entropy_numpy(path, cfg["CHUNK_SIZE"])

# -----------------------
# DB helpers
# -----------------------

def fetch_row(cur, path_key: bytes):
    cur.execute("""
        SELECT path,inode,size,mtime_ns,sha256,start_entropy,start_time,
               prev_entropy,prev_time,last_entropy,last_time,scans,flagged,note,missing_since,
               score_exempt,tags,quick_md5,last_full_verify
        FROM files WHERE path=%s
    """, (path_key,))
    return cur.fetchone()

def mark_missing(cur, path_key: bytes):
    now = now_db()
    cur.execute("""
        UPDATE files SET missing_since = IF(missing_since IS NULL,%s,missing_since)
        WHERE path=%s
    """, (now, path_key))

def effectively_exempt(row, score_excluded: bool) -> bool:
    """score_exempt (DB) ODER Score-Exclude-Pattern → True."""
    db_exempt = bool(row[15]) if row else False
    return db_exempt or score_excluded


def _should_rate_limit(cfg: dict) -> bool:
    """Rate-Limit: ließt Zeitstempel aus ALERT_STATE_FILE (Fallback: mtime)."""
    try:
        mins = int(cfg.get("MAIL_MIN_ALERT_INTERVAL_MIN", 0))
    except Exception:
        mins = 0
    if mins <= 0:
        return False

    path = cfg.get("ALERT_STATE_FILE") or ""
    if not path or not os.path.exists(path):
        return False

    try:
        # 1) Versuch: Inhalt (z.B. 'YYYY-MM-DD HH:MM:SS')
        with open(path, "r", encoding="utf-8") as f:
            txt = f.read().strip()
        last = None
        if txt:
            try:
                # fromisoformat akzeptiert 'YYYY-MM-DD HH:MM:SS'
                last = datetime.datetime.fromisoformat(txt)
            except Exception:
                last = None

        # 2) Fallback: mtime
        if last is None:
            last = datetime.datetime.fromtimestamp(os.path.getmtime(path))

        return (now_db() - last) < datetime.timedelta(minutes=mins)
    except Exception:
        logging.exception("Rate-Limit Check fehlgeschlagen")
        return False

# -----------------------
# Mail helpers
# -----------------------

def _write_alert_state(cfg: dict) -> None:
    """Schreibt den letzten Mail-Versandzeitpunkt atomar in ALERT_STATE_FILE."""
    path = cfg.get("ALERT_STATE_FILE") or ""
    if not path:
        return
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        ts = now_db().strftime("%Y-%m-%d %H:%M:%S")
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(ts)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)  # atomar
    except Exception:
        logging.exception("Konnte ALERT_STATE_FILE nicht aktualisieren")


def send_alert_email(subject_core: str, body: str, cfg) -> None:
    """Versendet eine Alert-Mail. Betreff enthält Prefix + Hostname."""
    if not cfg.get("MAIL_ENABLE", False):
        return
    if not cfg.get("MAIL_SMTP_HOST") or not cfg.get("MAIL_TO"):
        logging.warning("MAIL_ENABLE=1, aber SMTP_HOST/MAIL_TO fehlt – kein Versand.")
        return

    try:
        host = socket.gethostname()
        prefix = cfg.get("MAIL_SUBJECT_PREFIX", "[EntropyWatcher]")
        subject = f"{prefix} {subject_core} auf {host}"

        msg = EmailMessage()
        msg["From"] = cfg.get("MAIL_FROM", "entropywatcher@localhost")
        msg["To"] = cfg.get("MAIL_TO", "")
        msg["Subject"] = subject
        msg.set_content(body)

        sent = False

        if cfg.get("MAIL_SSL", False):
            # SMTPS (465)
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(cfg.get("MAIL_SMTP_HOST"),
                                  cfg.get("MAIL_SMTP_PORT", 465),
                                  context=context) as s:
                if cfg.get("MAIL_USER"):
                    s.login(cfg.get("MAIL_USER"), cfg.get("MAIL_PASS", ""))
                s.send_message(msg)
                sent = True
        else:
            # SMTP (587) + STARTTLS (typisch: GMX)
            with smtplib.SMTP(cfg.get("MAIL_SMTP_HOST"),
                              cfg.get("MAIL_SMTP_PORT", 587)) as s:
                if cfg.get("MAIL_STARTTLS", True):
                    s.starttls(context=ssl.create_default_context())
                if cfg.get("MAIL_USER"):
                    s.login(cfg.get("MAIL_USER"), cfg.get("MAIL_PASS", ""))
                s.send_message(msg)
                sent = True

        if sent:
            _write_alert_state(cfg)   # Rate-Limit-Marker NUR nach Erfolg
            logging.info("Alert-Mail versendet an: %s", cfg.get("MAIL_TO"))

    except Exception:
        logging.exception("Mailversand fehlgeschlagen")

# -----------------------
# Mail-Text helpers
# ----------------------

def _build_flagged_summary(cur, limit=50) -> str:
    """Liest die wichtigsten flagged Files für die Mail zusammen."""
    cur.execute("""
        SELECT path, source, start_entropy, last_entropy, start_time, last_time, note
        FROM files
        WHERE flagged=1
        ORDER BY last_time DESC
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    lines = []
    for (path_key, source, sH, lH, sT, lT, note) in rows:
        p = db_key_to_display(path_key)
        src = source or "-"
        sHs = "-" if sH is None else f"{sH:.3f}"
        lHs = "-" if lH is None else f"{lH:.3f}"
        sTs = "-" if not sT else sT.strftime("%Y-%m-%d %H:%M:%S")
        lTs = "-" if not lT else lT.strftime("%Y-%m-%d %H:%M:%S")
        lines.append(f"[{src}] {p}\n  start={sHs} ({sTs})  last={lHs} ({lTs})  note={note or ''}")
    if not lines:
        return "Keine Details (keine Zeilen gefunden)."
    return "Verdächtige Dateien:\n\n" + "\n".join(lines)

# -----------------------
# Quarantine helpers
# ----------------------

def _quarantine_file(src_path: str, cfg: Dict[str, Any]) -> tuple[str, str]:
    """
    Quarantäne-Aktion gemäß cfg:
      move  -> Datei nach AV_QUARANTINE_DIR/<basename> verschieben
      copy  -> dorthin kopieren (Original bleibt)
      chmod -> Modus auf AV_QUARANTINE_CHMOD setzen (z. B. '000')
      none  -> nichts tun
    Rückgabe: (action, quarantine_path|""), action eine von:
      "quarantine" | "copy" | "chmod" | "already_quarantined" | "none"
    """
    action_cfg = (cfg.get("AV_QUARANTINE_ACTION") or "none").lower()
    qdir = (cfg.get("AV_QUARANTINE_DIR") or "").rstrip("/")
    if action_cfg == "none" or not src_path:
        return ("none", "")

    # Normierte absolute Pfade vergleichen
    try:
        src_abs = os.path.abspath(src_path)
    except Exception:
        src_abs = src_path
    qdir_abs = os.path.abspath(qdir) if qdir else ""

    # Wenn die Quelle schon im Quarantäne-Verzeichnis liegt: NICHT erneut verschieben/umbenennen
    if qdir_abs and os.path.commonpath([src_abs, qdir_abs]) == qdir_abs:
        return ("already_quarantined", src_path)

    # Zielpfad (flach) bestimmen
    base = os.path.basename(src_path)
    dst = os.path.join(qdir_abs, base)

    try:
        os.makedirs(qdir_abs, exist_ok=True)

        if action_cfg == "move":
            # Kollisionen vermeiden: .1, .2, ...
            if os.path.exists(dst):
                i = 1
                root, ext = os.path.splitext(dst)
                while os.path.exists(dst):
                    dst = f"{root}.{i}{ext}"
                    i += 1
            shutil.move(src_abs, dst)
            return ("quarantine", dst)

        elif action_cfg == "copy":
            if os.path.exists(dst):
                i = 1
                root, ext = os.path.splitext(dst)
                while os.path.exists(dst):
                    dst = f"{root}.{i}{ext}"
                    i += 1
            shutil.copy2(src_abs, dst)
            return ("copy", dst)

        elif action_cfg == "chmod":
            mode_str = (cfg.get("AV_QUARANTINE_CHMOD") or "000").strip()
            mode = int(mode_str, 8)
            os.chmod(src_abs, mode)
            return ("chmod", src_abs)

    except Exception:
        logging.exception("Quarantaene-Aktion fehlgeschlagen für %s", src_path)

    return ("none", "")

# -----------------------
# ClamAV helpers
# -----------------------

def _clamav_build_cmd(cfg) -> list[str]:
    use_clamd = cfg.get("CLAMAV_USE_CLAMD", False)
    cmd = ["clamdscan"] if use_clamd else ["clamscan"]

    # generische Schalter
    if not use_clamd:
        # clamscan kann multithreaded scannen
        if cfg["CLAMAV_THREADS"] > 1:
            cmd += ["--threads", str(cfg["CLAMAV_THREADS"])]
        if cfg["CLAMAV_MAX_FILESIZE_MB"] > 0:
            cmd += ["--max-filesize", f"{cfg['CLAMAV_MAX_FILESIZE_MB']}M"]
        cmd += ["--recursive=yes", "--infected", "--no-summary"]
    else:
        # clamdscan: --fdpass hilft bei Berechtigungen, --multiscan nutzt mehrere Threads des Daemons
        cmd += ["--fdpass", "--multiscan", "--infected", "--no-summary"]


    # Excludes: nur clamscan versteht --exclude
    # Für glob-Muster übersetzen wir grob: * -> .*, ? -> .  (einfach & ausreichend für häufige Fälle)
    def glob_to_regex(pat: str) -> str:
        # escape alles, dann * und ? ersetzen
        s = re.escape(pat).replace(r"\*", ".*").replace(r"\?", ".")
        return "^" + s + "$"

    if not use_clamd:
        for ex in cfg["CLAMAV_EXCLUDES"]:
            if not ex:
                continue
            if cfg["CLAMAV_EXCLUDES_MODE"] == "glob":
                rex = glob_to_regex(ex)
            else:
                rex = ex
            cmd += ["--exclude", rex]

        # Quarantäne-Verzeichnis IMMER ausschließen (falls gesetzt)
        qdir = (cfg.get("AV_QUARANTINE_DIR") or "").rstrip("/")
        if qdir:
        # einfache Regex, die qdir und Unterpfade matcht
            qdir_re = "^" + re.escape(qdir) + "(/|$)"
            cmd += ["--exclude", qdir_re]
    else:
        logging.debug("clamdscan aktiv – Excludes in /etc/clamav/clamd.conf via ExcludePath pflegen")

    return cmd

def _clamav_run(paths: list[str], cfg) -> tuple[int, str, str]:
    """Startet ClamAV und gibt (exitcode, stdout, stderr) zurück."""
    cmd = _clamav_build_cmd(cfg) + paths
    logging.info("ClamAV starte: %s", " ".join(shlex.quote(c) for c in cmd))
    try:
        cp = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=cfg.get("CLAMAV_TIMEOUT", 1800)
        )
        # Bei Fehlern stderr ins Log hängen – hilft bei RC=2 sofort
        if cp.returncode != 0 and cp.stderr:
            logging.warning("ClamAV stderr:\n%s", cp.stderr.strip())
        return cp.returncode, cp.stdout, cp.stderr
    except subprocess.TimeoutExpired as e:
        logging.error("ClamAV Timeout nach %ss", cfg.get("CLAMAV_TIMEOUT", 1800))
        return 124, e.stdout or "", e.stderr or ""
    except FileNotFoundError:
        logging.error("ClamAV nicht gefunden. Installiere 'clamscan' oder setze CLAMAV_USE_CLAMD=1 und installiere 'clamdscan'.")
        return 127, "", ""
    except Exception:
        logging.exception("ClamAV-Lauf fehlgeschlagen")
        return 1, "", ""

def _clamav_parse_findings(stdout: str) -> list[tuple[str,str]]:
    """
    Parst Zeilen im Format:
    /pfad/zur/datei: Eicar-Test-Signature FOUND
    Gibt Liste (pfad, signatur) zurück.
    """
    findings = []
    for line in stdout.splitlines():
        if not line.strip():
            continue
        # typische Zeile: "<pfad>: <MalwareName> FOUND"
        if line.endswith(" FOUND"):
            parts = line.rsplit(":", 1)
            if len(parts) == 2:
                p = parts[0].strip()
                sig = parts[1].replace("FOUND", "").strip()
                findings.append((p, sig))
    return findings



# -----------------------
# Insert/Update
# -----------------------

def upsert_initial(cur, p: Path, st, digest, ent: float, cfg) -> None:
    """Erstbefüllung (Startwerte bleiben unverändert)."""
    now = now_db()
    qmd5 = quick_md5_head_tail(p, cfg["QUICK_FP_SAMPLE"]) if cfg["QUICK_FINGERPRINT"] else None
    key = db_key_from_path(p)
    cur.execute("""
        INSERT INTO files (path,source,inode,size,mtime_ns,sha256,
                           start_entropy,start_time,
                           prev_entropy,prev_time,
                           last_entropy,last_time,
                           scans,flagged,note,missing_since,
                           quick_md5,last_full_verify)
        VALUES (%s,%s,%s,%s,%s,%s, %s,%s, NULL,NULL, %s,%s, 1, 0, NULL, NULL, %s, %s)
        ON DUPLICATE KEY UPDATE
          inode=VALUES(inode),
          size=VALUES(size),
          mtime_ns=VALUES(mtime_ns),
          sha256=VALUES(sha256),
          last_entropy=IF(last_entropy IS NULL, VALUES(last_entropy), last_entropy),
          last_time=IF(last_time IS NULL, VALUES(last_time), last_time),
          scans=scans+1,
          missing_since=NULL,
          quick_md5=VALUES(quick_md5)
    """, (key, cfg["SOURCE_LABEL"] or None, st.st_ino, st.st_size, st.st_mtime_ns, digest,
          ent, now, ent, now, qmd5, now))

def update_on_change(cur, row, p: Path, st, digest, ent: float, cfg, score_excluded: bool, full_verified: bool) -> None:
    """
    Aktualisiert Werte auf Änderung/Reverify.
    - prev_* <= last_*, last_* <= neue Werte
    - flagged/note nur, wenn nicht exempt
    - last_full_verify NUR setzen, wenn 'full_verified' True (also Voll-Check, nicht Skip).
    """
    now = now_db()
    start_entropy = row[5]
    flagged = 0
    note = None
    if not effectively_exempt(row, score_excluded):
        if ent >= cfg["ALERT_ENTROPY_ABS"] or (start_entropy is not None and ent - start_entropy >= cfg["ALERT_ENTROPY_JUMP"]):
            flagged = 1
            rsn = []
            if ent >= cfg["ALERT_ENTROPY_ABS"]: rsn.append(f"abs>={cfg['ALERT_ENTROPY_ABS']}")
            if start_entropy is not None and (ent - start_entropy) >= cfg["ALERT_ENTROPY_JUMP"]:
                rsn.append(f"jump {round(ent - start_entropy,3)}")
            note = ", ".join(rsn)

    qmd5 = quick_md5_head_tail(p, cfg["QUICK_FP_SAMPLE"]) if cfg["QUICK_FINGERPRINT"] else None
    key = db_key_from_path(p)
    if full_verified:
        cur.execute("""
            UPDATE files SET
              inode=%s, size=%s, mtime_ns=%s, sha256=%s,
              prev_entropy=last_entropy, prev_time=last_time,
              last_entropy=%s, last_time=%s,
              scans=scans+1, flagged=%s, note=%s,
              missing_since=NULL,
              quick_md5=%s,
              last_full_verify=%s
            WHERE path=%s
        """, (st.st_ino, st.st_size, st.st_mtime_ns, digest,
              ent, now, flagged, note,
              qmd5, now, key))
    else:
        # Sollte selten vorkommen – Voll-Check deckt Updates/Reverify ab.
        cur.execute("""
            UPDATE files SET
              inode=%s, size=%s, mtime_ns=%s,
              scans=scans+1,
              missing_since=NULL
            WHERE path=%s
        """, (st.st_ino, st.st_size, st.st_mtime_ns, key))

# -----------------------
# Parallel Worker
# -----------------------

def _heavy_job(args) -> Tuple[str, Optional[os.stat_result], Optional[bytes], Optional[float], Optional[bytes], Optional[str]]:
    """
    Rechenintensiver Job: SHA-256, Entropie, Quick-FP
    Rückgabe: (path_str, stat, sha256, entropy, quick_md5, error_str)
    """
    p_str, cfg = args
    p = Path(p_str)
    try:
        st = p.stat()
        digest = sha256_of(p, cfg["CHUNK_SIZE"])
        ent = entropy_of(p, cfg)
        qmd5 = quick_md5_head_tail(p, cfg["QUICK_FP_SAMPLE"]) if cfg["QUICK_FINGERPRINT"] else None
        return (p_str, st, digest, ent, qmd5, None)
    except Exception as e:
        return (p_str, None, None, None, None, repr(e))

# -----------------------
# CLI
# -----------------------

@click.group(
    context_settings=dict(
        help_option_names=["-h", "--help"],
        max_content_width=100,   # breitere Help
    )
)
@click.option(
    "--env", "env_files", multiple=True,
    help="Zusätzliche .env-Dateien in Reihenfolge (CLI-Pflicht; unter systemd verboten)."
         "Beispiel: --env /opt/entropywatcher/commmon.env --env /opt/entropywatcher/nas.env"
)
@click.pass_context
def cli(ctx, env_files):
    """
    EntropyWatcher – Entropie erfassen & überwachen (gehärtet)

    \b
    Hilfe-Tipps:
      - Allgemein:  entropywatcher.py --help
      - Hilfe zu Befehlen:  entropywatcher.py <COMMAND> --help   (z.B. scan, init-scan, av-scan)
      - Beispiele:  entropywatcher.py help-examples
    """
    ctx.ensure_object(dict)
    ctx.obj["env_files"] = list(env_files)
    ctx.obj["systemd"]   = _launched_by_systemd()

# help
@cli.command("help", context_settings=dict(ignore_unknown_options=True, allow_extra_args=True))
@click.pass_context
def help_cmd(ctx):
    """Hilfe anzeigen (auch für Unterbefehle). Usage: help [COMMAND]"""
    if ctx.args:
        name = ctx.args[0]
        cmd = cli.get_command(ctx, name)
        if cmd is not None:
            with click.Context(cmd) as subctx:
                click.echo(cmd.get_help(subctx))
            return
        click.echo(f"Unbekannter Befehl: {name}\n")
    # Fallback: Top-Level-Hilfe
    with click.Context(cli) as topctx:
        click.echo(cli.get_help(topctx))

# help-examples
@cli.command("help-examples")
@click.option("--pager/--no-pager", default=True, help="Mit/ohne Pager ausgeben. Default: --pager")
def help_examples(pager: bool):
    """Zeigt Beispiel-Aufrufe und erklärt die wichtigsten Kombinationen."""
    txt = textwrap.dedent("""

##############   Hinweise:       ##############
# Bitte erst Umgebung aktivieren oder installieren:
      source /opt/entropywatcher/venv/bin/activate
    # oder
      python3 -m venv ~/entropywatcher-venv
      source ~/entropywatcher-venv/bin/activate

# Erst danach Skript starten, bspw.:
# (Standardmäßig wird immer erst "common.env" geladen)
      python entropywatcher.py scan --force --paths "/srv/nas" --env /opt/entropywatcher/os.env
    # oder
      python /opt/entropywatcher/entropywatcher.py --env /opt/entropywatcher/os.env report --source os | head

# Pfadangabe:
# Pfad-Scopes werden ausschließlich über --paths gesetzt
# (nicht aus einer Umgebungsvariable oder Datei).
# Pfadangabe ist bei init-scan, scan und av-scan Pflicht:
      --paths "/mein/pfad"

# Syntax-Check:
python -m py_compile /opt/entropywatcher/entropywatcher.py && echo "Syntax ok ✅"

##############  Ende Hinweise:   ##############

## Beispiele:

  # 1) Baseline einmalig erfassen (wenn DB leer, als 1. Scan)
      # Baseline (einmalig) für NAS
      python entropywatcher.py init-scan --force --paths "/srv/nas"

  # 2) Regulärer Folgescan (nur neue/geänderte prüfen)
      # Regulärer Scan für NAS
      python entropywatcher.py scan --force --paths "/srv/nas"

  # 3) Report komplett / gefiltert
      # alles mit Kopfzeilen
      python entropywatcher.py report

      # nur verdächtige
      python entropywatcher.py report --only-flagged

      # nur fehlende
      python entropywatcher.py report --since-missing

      # nach Quelle filtern (os|nas)
      python entropywatcher.py report --source nas
      python entropywatcher.py report --source os

      # fehlende auf dem OS
      python entropywatcher.py report --source os --only-flagged

      # Export
      python entropywatcher.py report --export out.csv --format csv
      python entropywatcher.py report --export out.json --format json

  # 4) ClamAV (Virenscanner)
      # Aufruf des Virenscanners:
      python entropywatcher.py av-scan --paths "/srv/nas/Thomas,/srv/nas/Monika"

  # 5) Einzelne Datei vom Scoring ausnehmen / wieder normal
      # Dateien werden weiterhin gescannt und protokolliert, aber es wird kein Alarm-Mail versendet
      python entropywatcher.py tag-exempt "/srv/nas/Thomas/foo.bin"
      python entropywatcher.py tag-normal "/srv/nas/Thomas/foo.bin"

  # 6) Stats
      python entropywatcher.py stats

  # 7) Services & Timer
      # Pfad
      /etc/systemd/system/

      #### Generelles #####
      - Skript: /opt/entropywatcher/entropywatcher.py
      - Zwei Grundfunktionen:
        A) Entropie-Scan
           - einmaliger Initial-Scan: "init-scan"
           - täglicher Delta-Scan:    "scan"
           - nutzt MariaDB
        B) VirenScan mit ClamAV
           - täglicher Scan für "Hot-Paths"
           - wöchentlicher "Full Scan"

      Es werden zwei Bereiche gescannt:
      1) NAS (Dateien auf SATA-SSD)
      2) OS  (Betriebssystem auf der SD-Karte)

      Alle Services werden primär über .env-Files gesteuert (/opt/entropywatcher/):
      1) common.env
      2) service-spezifisches .env

      NAS: nas.env, nas-av.env, nas-av-weekly.env
      OS:  os.env,  os-av.env,  os-av-weekly.env

      3) alle haben ein Label (SOURCE_LABEL)
      4) eigener SysLogIdentifier (journald)
      5) Mail-Bremse (Rate-Limit) + Logfile

      7) Liste der Files & Services & Timer

      #####  NAS  #####
      # Entropie-Delta-Scan für NAS
      /etc/systemd/system/entropywatcher-nas.service         # SOURCE_LABEL=nas,  scan --paths ${SCAN_PATHS}
      /etc/systemd/system/entropywatcher-nas.timer

      # ClamAV-Scan für Hot-Folder auf dem NAS
      /etc/systemd/system/entropywatcher-nas-av.service      # SOURCE_LABEL=nas-av,        av-scan --paths ${SCAN_PATHS}
      /etc/systemd/system/entropywatcher-nas-av.timer

      # Wöchentlicher Full-ClamAV-Scan des NAS
      /etc/systemd/system/entropywatcher-nas-av-weekly.service  # SOURCE_LABEL=nas-av-weekly, av-scan --paths ${SCAN_PATHS}
      /etc/systemd/system/entropywatcher-nas-av-weekly.timer    # So 03:00

      #####  OS (Betriebssystem)  #####
      # Entropie-Delta-Scan für OS
      /etc/systemd/system/entropywatcher-os.service          # SOURCE_LABEL=os,    scan --paths ${SCAN_PATHS}
      /etc/systemd/system/entropywatcher-os.timer

      # ClamAV-Scan für Hot-Folder auf dem OS
      /etc/systemd/system/entropywatcher-os-av.service       # SOURCE_LABEL=os-av,        av-scan --paths ${SCAN_PATHS}
      /etc/systemd/system/entropywatcher-os-av.timer

      # Wöchentlicher Full-ClamAV-Scan des OS
      /etc/systemd/system/entropywatcher-os-av-weekly.service  # SOURCE_LABEL=os-av-weekly, av-scan --paths ${SCAN_PATHS}
      /etc/systemd/system/entropywatcher-os-av-weekly.timer


	### DB-Reports #####
	# Kompakter Zustand (24h)
	entropywatcher.py db-report

	# Verdächtige Dateien (nur os), 100 Zeilen
	entropywatcher.py db-report --what flagged --source os --limit 100

	# AV-Events der letzten 6h für os-av
	entropywatcher.py db-report --what av --source os-av --hours 6

	# Letzte 20 Summaries für alle Quellen
	entropywatcher.py db-report --what summary --limit 20

	# Missing-Einträge (nas)
	entropywatcher.py db-report --what missing --source nas

	# Zuletzt geänderte in 12h (alle Quellen)
	entropywatcher.py db-report --what recent --hours 12

        # Neuste / letzte Läufe/Scans
        entropywatcher.py db-report --what last-runs


""").strip() + "\n"

    if pager and sys.stdout.isatty():
        click.echo_via_pager(txt + "\n")   # nutzt $PAGER (z.B. less)
    else:
        click.echo(txt + "\n")



@cli.command("init-scan", short_help="Einmaliger Basis-Scan mit Härtung; Pfade via --paths")
@click.option("--paths", required=True, help="Kommagetrennte Pfade/Verzeichnisse für den Baseline-Scan (Pflicht).")
@click.option("--force", is_flag=True, help="Initialen Vollscan erzwingen, auch wenn DB bereits Einträge hat.")
@click.pass_context
def init_scan(ctx, paths, force):
    """Erstscan: Baseline setzen (Startwerte nie überschreiben)."""
    cfg, trace = load_config(ctx, "init-scan")
    logger, q, listener = setup_logging(cfg)
    _log_load_trace(trace)
    conn = db_connect(cfg)
    ensure_schema(conn)
    cur = conn.cursor()

    scan_dirs = _parse_paths_arg(paths)
    if not scan_dirs:
        click.echo("Fehler: --paths ist leer."); cur.close(); conn.close(); teardown_logging(listener); return

    # Guard: vermeide versehentlichen Vollscan
    cur.execute("SELECT COUNT(*) FROM files")
    if cur.fetchone()[0] > 0 and not force:
        click.echo("Abbruch: DB enthält bereits Einträge. Verwende 'scan' oder '--force' für erneuten Vollscan.")
        cur.close(); conn.close(); teardown_logging(listener); return

    if cfg["USE_ENT"] and not shutil.which("ent"):
        logging.warning("USE_ENT=1, aber 'ent' ist nicht installiert → NumPy-Fallback.")

    exc_patterns = _compile_patterns(cfg["EXCLUDES"], cfg["EXCLUDES_MODE"])

    # Timing
    t0 = time.perf_counter()
    bytes_total = 0

    seen = set()
    count = 0
    for p in file_iter(scan_dirs):
        if should_skip(p, cfg, exc_patterns, cfg["EXCLUDES_MODE"]):
            continue
        try:
            st = p.stat()
            digest = sha256_of(p, cfg["CHUNK_SIZE"])
            ent = entropy_of(p, cfg)
            bytes_total += st.st_size
        except PermissionError:
            logging.warning("Keine Berechtigung: %s", p); continue
        except Exception:
            logging.exception("Fehler bei %s", p); continue
        upsert_initial(cur, p, st, digest, ent, cfg)
        seen.add(db_key_from_path(p))
        count += 1
        if count % 500 == 0:
            logging.info("... %d Dateien initialisiert", count)

    # Fehlende markieren – nur für *diese* Quelle
    src = cfg.get("SOURCE_LABEL") or None
    if src is None:
        cur.execute("SELECT path FROM files WHERE source IS NULL")
    else:
        cur.execute("SELECT path FROM files WHERE source=%s", (src,))
    for (path_key,) in cur.fetchall():
        if path_key not in seen:
            mark_missing(cur, path_key)

    cur.close(); conn.close()

    # Timing-Log
    dt = time.perf_counter() - t0
    thr = (bytes_total / dt) if dt > 0 else 0
    logging.info(
        "Init fertig. Dateien erfasst: %d | total=%.2fs | bytes=%d | thru=%.2f MiB/s",
        len(seen), dt, bytes_total, thr/1024/1024
    )
    teardown_logging(listener)

@cli.command("scan")
@click.option(
    "--paths",
    required=True,
    help="Kommagetrennte Pfade/Verzeichnisse, für den Delta-/Reverify-Scan (Pflicht).",
)
@click.pass_context
def scan(ctx, paths):
    """Folgescan: nur neue/geänderte; Härtung via Quick-FP & periodische Reverify; parallelisiert."""
    from multiprocessing import cpu_count, get_context

    cfg, trace = load_config(ctx, "scan")

    # Konsistente Startzeit (DB/Log) + Liste für neu auf 'flagged' gewechselte Dateien
    scan_started_at = now_db()
    newly_flagged: List[Tuple[str, float, str]] = []  # (path, last_entropy, note)

    logger, q, listener = setup_logging(cfg)
    conn = db_connect(cfg)
    ensure_schema(conn)
    cur = conn.cursor()
    _log_load_trace(trace)

    logging.info("ALERT_STATE_FILE eff.: %s", cfg.get("ALERT_STATE_FILE"))

    scan_dirs = _parse_paths_arg(paths)
    if not scan_dirs:
        click.echo("Fehler: --paths ist leer.")
        cur.close()
        conn.close()
        teardown_logging(listener)
        return

    if cfg["USE_ENT"] and not shutil.which("ent"):
        logging.warning("USE_ENT=1, aber 'ent' ist nicht installiert → NumPy-Fallback.")

    exc_patterns = _compile_patterns(cfg["EXCLUDES"], cfg["EXCLUDES_MODE"])
    sexc_patterns = _compile_patterns(cfg["SCORE_EXCLUDES"], cfg["SCORE_EXCLUDES_MODE"])

    # Timings / Zähler
    t_all = time.perf_counter()
    t_disc0 = time.perf_counter()
    bytes_processed = 0
    files_processed = 0

    present: set[bytes] = set()
    inserted = 0
    changed = 0
    reverified = 0
    candidates: List[str] = []
    now_ts = now_db()

    # --- Phase 1: Kandidaten bestimmen (leichte Checks) ---
    for p in file_iter(scan_dirs):
        if should_skip(p, cfg, exc_patterns, cfg["EXCLUDES_MODE"]):
            continue
        key = db_key_from_path(p)
        present.add(key)
        try:
            st = p.stat()
        except Exception:
            logging.exception("Stat-Fehler: %s", p)
            continue

        row = fetch_row(cur, key)

        # Neu -> voller Job nötig
        if row is None:
            candidates.append(str(p))
            continue

        old_size, old_mtime = row[2], row[3]
        need_full = False

        # Periodische Vollverifikation?
        if cfg["PERIODIC_REVERIFY_DAYS"] > 0:
            last_verify = row[18]  # last_full_verify
            if (not last_verify) or (
                now_ts - last_verify >= datetime.timedelta(days=cfg["PERIODIC_REVERIFY_DAYS"])
            ):
                need_full = True

        # Quick-FP bei unverändertem mtime/size?
        if (
            not need_full
            and cfg["QUICK_FINGERPRINT"]
            and old_size == st.st_size
            and old_mtime == st.st_mtime_ns
        ):
            try:
                qold = row[17]  # quick_md5
                if qold is not None:
                    qnew = quick_md5_head_tail(p, cfg["QUICK_FP_SAMPLE"])
                    if qnew != qold:
                        need_full = True
            except Exception:
                logging.exception("Quick-FP-Fehler: %s", p)

        # Geändert? (mtime/size) oder Full nötig?
        if need_full or old_size != st.st_size or old_mtime != st.st_mtime_ns:
            candidates.append(str(p))

    # --- Phase 2: Heavy-Jobs parallel ---
    t_disc = time.perf_counter() - t_disc0
    t_heavy0 = time.perf_counter()

    if candidates:
        procs = min(max(1, cfg["WORKERS"]), max(1, cpu_count() - 1))
        logging.info("Starte Heavy-Jobs: %d Kandidaten, %d Worker ...", len(candidates), procs)

        try:
            with get_context("fork").Pool(processes=procs) as pool:
                jobs = [(c, cfg) for c in candidates]
                for p_str, st, digest, ent, qmd5_unused, err in pool.imap_unordered(
                    _heavy_job, jobs, chunksize=8
                ):
                    if err:
                        logging.error("Jobfehler %s: %s", p_str, err)
                        continue
                    if st is not None:
                        bytes_processed += st.st_size
                        files_processed += 1

                    p = Path(p_str)
                    key = db_key_from_path(p)
                    score_excluded = is_score_excluded(
                        p, cfg, sexc_patterns, cfg["SCORE_EXCLUDES_MODE"]
                    )
                    row = fetch_row(cur, key)
                    if row is None:
                        # Datei während der Berechnung neu geworden
                        try:
                            st_now = p.stat()
                            upsert_initial(cur, p, st_now, digest, ent, cfg)
                            inserted += 1
                        except Exception:
                            logging.exception("Insert-Fehler bei %s", p_str)
                        continue

                    old_size, old_mtime = row[2], row[3]

                    # --- 0→1-Transition (wird JETZT geflaggt?) ---
                    old_flagged = bool(row[12])       # flagged
                    start_entropy = row[5]            # start_entropy
                    will_flag = False
                    note_local = None
                    if not score_excluded and ent is not None:
                        reasons = []
                        if ent >= cfg["ALERT_ENTROPY_ABS"]:
                            reasons.append(f"abs>={cfg['ALERT_ENTROPY_ABS']}")
                        if start_entropy is not None and (ent - start_entropy) >= cfg["ALERT_ENTROPY_JUMP"]:
                            reasons.append(f"jump {round(ent - start_entropy, 3)}")
                        if reasons:
                            will_flag = True
                            note_local = ", ".join(reasons)

                    full_verified = True
                    try:
                        update_on_change(cur, row, p, st, digest, ent, cfg, score_excluded, full_verified)
                        if old_size == st.st_size and old_mtime == st.st_mtime_ns:
                            reverified += 1
                        else:
                            changed += 1
                    except Exception:
                        logging.exception("Update-Fehler bei %s", p_str)

                    # Jetzt Transition prüfen: vorher 0, jetzt (laut Heuristik) 1
                    if will_flag and not old_flagged:
                        newly_flagged.append(
                            (str(p), float(ent) if ent is not None else float("nan"), note_local or "")
                        )

        except Exception:
            logging.exception("Parallel-Phase fehlgeschlagen – fallback seriell.")
            # Serieller Fallback
            for c in candidates:
                p = Path(c)
                try:
                    st = p.stat()
                    digest = sha256_of(p, cfg["CHUNK_SIZE"])
                    ent = entropy_of(p, cfg)
                    score_excluded = is_score_excluded(
                        p, cfg, sexc_patterns, cfg["SCORE_EXCLUDES_MODE"]
                    )
                except Exception:
                    logging.exception("Heavy-Fallback-Fehler: %s", p)
                    continue

                if st is not None:
                    bytes_processed += st.st_size
                    files_processed += 1

                key = db_key_from_path(p)
                row = fetch_row(cur, key)
                if row is None:
                    try:
                        upsert_initial(cur, p, st, digest, ent, cfg)
                        inserted += 1
                    except Exception:
                        logging.exception("Insert-Fehler (seriell) bei %s", c)
                    continue

                old_size, old_mtime = row[2], row[3]

                # --- 0→1-Transition (seriell) ---
                old_flagged = bool(row[12])
                start_entropy = row[5]
                will_flag = False
                note_local = None
                if not score_excluded and ent is not None:
                    reasons = []
                    if ent >= cfg["ALERT_ENTROPY_ABS"]:
                        reasons.append(f"abs>={cfg['ALERT_ENTROPY_ABS']}")
                    if start_entropy is not None and (ent - start_entropy) >= cfg["ALERT_ENTROPY_JUMP"]:
                        reasons.append(f"jump {round(ent - start_entropy, 3)}")
                    if reasons:
                        will_flag = True
                        note_local = ", ".join(reasons)

                try:
                    update_on_change(cur, row, p, st, digest, ent, cfg, score_excluded, full_verified=True)
                    if old_size == st.st_size and old_mtime == st.st_mtime_ns:
                        reverified += 1
                    else:
                        changed += 1
                except Exception:
                    logging.exception("Update-Fehler (seriell) bei %s", c)

                if will_flag and not old_flagged:
                    newly_flagged.append(
                        (str(p), float(ent) if ent is not None else float("nan"), note_local or "")
                    )

    # Fehlende markieren – nur für *diese* Quelle
    src = cfg.get("SOURCE_LABEL") or None
    if src is None:
        cur.execute("SELECT path FROM files WHERE source IS NULL")
    else:
        cur.execute("SELECT path FROM files WHERE source=%s", (src,))
    for (path_key,) in cur.fetchall():
        if path_key not in present:
            mark_missing(cur, path_key)

    # Timings-Log
    t_heavy = time.perf_counter() - t_heavy0
    t_total = time.perf_counter() - t_all
    thr = (bytes_processed / t_heavy) if t_heavy > 0 else 0.0
    logging.info(
        "Timings: discovery=%.2fs heavy=%.2fs total=%.2fs | bytes=%d | thru=%.2f MiB/s | files=%d candidates=%d present=%d",
        t_disc, t_heavy, t_total, bytes_processed, thr / 1024 / 1024, files_processed, len(candidates), len(present)
    )

    # --- Mail-Alert NUR bei 0→1-Transitionen dieses Laufs ---
    try:
        if cfg.get("MAIL_ENABLE", False):
            if newly_flagged and not _should_rate_limit(cfg):
                lines = []
                for path_str, ent_val, note_local in newly_flagged[:50]:
                    ent_s = "-" if ent_val != ent_val else f"{ent_val:.3f}"  # NaN-Check
                    lines.append(f"{path_str}\n  last={ent_s}  note={note_local}")

                local_ts = _now_local(cfg)
                summary = (
                    f"Scan start (local): {local_ts:%Y-%m-%d %H:%M:%S %Z}\n"

                    # optional zusätzlich UTC:
                    # f"Scan start (UTC):   {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC\n"

                    f"Timings: discovery={t_disc:.2f}s heavy={t_heavy:.2f}s total={t_total:.2f}s\n"
                    f"Candidates={len(candidates)} Present={len(present)}\n\n"
                    "Neue verdächtige Dateien:\n\n" + "\n".join(lines)
                )
                send_alert_email(
                    subject_core=f"{len(newly_flagged)} neue verdächtige Datei(en)",
                    body=summary,
                    cfg=cfg
                )
            elif not newly_flagged:
                logging.info("Keine neuen flagged in diesem Lauf – keine Mail.")
            else:
                logging.info("Alert-Mail unterdrückt (Rate-Limit aktiv).")
    except Exception:
        logging.exception("Alert-Phase fehlgeschlagen")

    # Aufräumen
    try:
        conn.commit()
    except Exception:
        logging.exception("Commit fehlgeschlagen")
    # --- Summary persistieren ---
    try:
        write_scan_summary(
            conn=conn,
            cfg=cfg,
            started_at=scan_started_at,               # bereits oben (UTC) gesetzt
            finished_at=now_db(),
            candidates_count=len(candidates),
            files_processed=files_processed,
            bytes_processed=bytes_processed,
            flagged_new_count=len(newly_flagged),
            changed_count=changed,
            reverified_count=reverified,
            scan_paths=paths                           # <— NEU: Original-CLI-Argument
        )
        # WICHTIG: Commit JETZT, nach dem Insert
        conn.commit()

    except Exception:
        logging.exception("Summary-Insert fehlgeschlagen")
    try:
        cur.close()
        conn.close()
    finally:
        teardown_logging(listener)


 # --- Report ---
@cli.command("report")
@click.option("--since-missing", is_flag=True, help="Fehlende Dateien zeigen")
@click.option("--only-flagged", is_flag=True, help="Nur verdächtige Dateien")
@click.option("--export", type=click.Path(dir_okay=False), help="Export-Datei (CSV/JSON)")
@click.option("--format", "fmt", type=click.Choice(["csv","json"]), default="csv")
@click.option("--source", type=click.Choice(["os","nas",""]), default="", help="Nach Quelle filtern")
@click.pass_context
def report(ctx, only_flagged, since_missing, export, fmt, source):
    """Report mit Start/Prev/Last + Datum; Exempt markieren; optional Export."""
    cfg, trace = load_config(ctx, "report")
    logger, q, listener = setup_logging(cfg)
    conn = db_connect(cfg)
    cur = conn.cursor(dictionary=True)
    _log_load_trace(trace)

    where = []
    params = []
    if only_flagged:
        where.append("flagged=1")
    if source:
        where.append("source=%s")
        params.append(source)
    if since_missing:
        where.append("missing_since IS NOT NULL")

    wh = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT path, source, start_entropy,start_time,prev_entropy,prev_time,last_entropy,last_time,flagged,note,missing_since,score_exempt
        FROM files
        {wh}
        ORDER BY flagged DESC, last_time DESC
        LIMIT 10000
    """
    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close(); conn.close()

    if export:
        if not rows:
            click.echo("Nichts zu exportieren."); teardown_logging(listener); return
        if fmt == "csv":
            import csv
            with open(export, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                for r in rows:
                    r["path"] = db_key_to_display(r["path"])
                w.writeheader(); w.writerows(rows)
        else:
            import json
            for r in rows:
                r["path"] = db_key_to_display(r["path"])
            with open(export, "w") as f:
                json.dump(rows, f, default=str, ensure_ascii=False, indent=2)
        click.echo(f"Exportiert nach {export}")
        teardown_logging(listener); return

    if not rows:
        click.echo("Kein Eintrag."); teardown_logging(listener); return

    click.echo("PATH | start(H,t) | prev(H,t) | last(H,t) | flagged | note | missing_since")
    click.echo("-"*120)


    from zoneinfo import ZoneInfo
    local_tz = ZoneInfo(cfg.get("TZ", "Europe/Zurich"))

    def _fmt_local(dt):
        if not dt: return "-"
        # naive Zeit als UTC interpretieren und in Lokalzeit wandeln
        return dt.replace(tzinfo=datetime.timezone.utc).astimezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")

    for r in rows:
        display_path = db_key_to_display(r["path"])
        src = r.get("source") or "-"
        ex = "⛔ " if r.get("score_exempt") else ""

        sH = f"{r['start_entropy']:.3f}" if r['start_entropy'] is not None else "-"
        # sT = r['start_time'].strftime("%Y-%m-%d %H:%M:%S") if r['start_time'] else "-"
        sT = _fmt_local(r['start_time'])
        pH = f"{r['prev_entropy']:.3f}" if r['prev_entropy'] is not None else "-"
        #pT = r['prev_time'].strftime("%Y-%m-%d %H:%M:%S") if r['prev_time'] else "-"
        pT = _fmt_local(r['prev_time'])
        lH = f"{r['last_entropy']:.3f}" if r['last_entropy'] is not None else "-"
        #lT = r['last_time'].strftime("%Y-%m-%d %H:%M:%S") if r['last_time'] else "-"
        lT = _fmt_local(r['last_time'])
        flag = "⚠️" if r['flagged'] else ""
        note = r['note'] or ""
        # miss = r['missing_since'].strftime("%Y-%m-%d %H:%M:%S") if r['missing_since'] else ""
        miss = _fmt_local(r['missing_since']) if r['missing_since'] else ""
        click.echo(f"[{src}] {ex}{display_path} | {sH},{sT} | {pH},{pT} | {lH},{lT} | {flag} | {note} | {miss}")

    teardown_logging(listener)

# -----------------------
# DB-Report CLI (Preset-Abfragen)
# -----------------------
@cli.command("db-report")
@click.option("--what",
    type=click.Choice(["status","flagged","av","summary","missing","recent","last-runs","backup-last"], case_sensitive=False),
    default="status",
    help="Vordefinierter Report: status, flagged, av, summary, missing, recent, last-runs")
@click.option("--source", default="", help="Nach Quelle filtern (z.B. os, nas, os-av). Leer = alle.")
@click.option("--hours", type=int, default=24, help="Zeitfenster in Stunden für zeitbasierte Reports (z.B. av, recent, status).")
@click.option("--limit", type=int, default=50, help="Maximale Anzahl Zeilen (falls zutreffend).")
def db_report(what, source, hours, limit):
    """
    Vordefinierte DB-Reports ohne SQL-Tippen.

    Beispiele:
      entropywatcher.py db-report                     -> Status-Übersicht (24h)
      entropywatcher.py db-report --what flagged      -> Aktuell verdächtige Dateien (Top 50)
      entropywatcher.py db-report --what av           -> AV-Events der letzten 24h
      entropywatcher.py db-report --what summary      -> Letzte Scan-Summaries
      entropywatcher.py db-report --what missing      -> Als 'missing' markierte Dateien
      entropywatcher.py db-report --what recent       -> Zuletzt veränderte Dateien
      entropywatcher.py db-report --what last-runs    -> Die letzten Scan-Läufe (inkl. AV-Läufe ohne Funde)
    """
    cfg, trace = load_config()
    logger, q, listener = setup_logging(cfg)

    conn = None
    try:
        conn = db_connect(cfg)
        cur = conn.cursor(dictionary=True)

        # Zeitfenster (UTC, DB speichert naive UTC)
        now_utc = now_db()
        since_utc = now_utc - datetime.timedelta(hours=max(0, int(hours)))
        src_filter = source.strip() or None

        # Lokalzeit-Formatter
        local_tz = ZoneInfo(cfg.get("TZ", "Europe/Zurich"))
        def _fmt_local(dt):
            if not dt:
                return "-"
            return dt.replace(tzinfo=datetime.timezone.utc).astimezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")

        # Schlichte Tabelle ausgeben
        def _print_rows(rows, cols):
            if not rows:
                click.echo("(leer)")
                return
            colw = {c: len(c) for c in cols}
            for r in rows:
                for c in cols:
                    s = str(r.get(c, ""))
                    colw[c] = max(colw[c], len(s))
            header = " | ".join(c.ljust(colw[c]) for c in cols)
            click.echo(header)
            click.echo("-" * len(header))
            for r in rows:
                line = " | ".join(str(r.get(c, "")).ljust(colw[c]) for c in cols)
                click.echo(line)

        if what == "status":
            # Flagged pro Quelle
            if src_filter is None:
                cur.execute("SELECT source, COUNT(*) AS cnt FROM files WHERE flagged=1 GROUP BY source ORDER BY cnt DESC")
            else:
                cur.execute("SELECT source, COUNT(*) AS cnt FROM files WHERE flagged=1 AND source=%s GROUP BY source", (src_filter,))
            flagged_by_src = cur.fetchall()

            # Missing pro Quelle
            if src_filter is None:
                cur.execute("SELECT source, COUNT(*) AS cnt FROM files WHERE missing_since IS NOT NULL GROUP BY source ORDER BY cnt DESC")
            else:
                cur.execute("SELECT source, COUNT(*) AS cnt FROM files WHERE missing_since IS NOT NULL AND source=%s GROUP BY source", (src_filter,))
            missing_by_src = cur.fetchall()

            # Letzter Änderungszeitpunkt in files
            if src_filter is None:
                cur.execute("SELECT source, MAX(last_time) AS last_time FROM files GROUP BY source ORDER BY source")
            else:
                cur.execute("SELECT source, MAX(last_time) AS last_time FROM files WHERE source=%s GROUP BY source", (src_filter,))
            last_by_src = cur.fetchall()
            for r in last_by_src:
                r["last_time"] = _fmt_local(r["last_time"])

            # AV-Events im Zeitfenster
            if src_filter is None:
                cur.execute("SELECT source, COUNT(*) AS cnt FROM av_events WHERE detected_at >= %s GROUP BY source ORDER BY cnt DESC", (since_utc,))
            else:
                cur.execute("SELECT source, COUNT(*) AS cnt FROM av_events WHERE source=%s AND detected_at >= %s GROUP BY source", (src_filter, since_utc))
            av_by_src = cur.fetchall()

            click.echo(f"== STATUS (Zeitfenster: letzte {hours}h) ==")
            click.echo("-- Flagged --")
            _print_rows(flagged_by_src, ["source", "cnt"])
            click.echo("-- Missing --")
            _print_rows(missing_by_src, ["source", "cnt"])
            click.echo("-- Letzter Scan (local time) --")
            _print_rows(last_by_src, ["source", "last_time"])
            click.echo("-- AV-Events im Zeitfenster --")
            _print_rows(av_by_src, ["source", "cnt"])

        elif what == "flagged":
            if src_filter is None:
                cur.execute("""
                    SELECT source, path, last_entropy, last_time, note
                    FROM files
                    WHERE flagged=1
                    ORDER BY last_time DESC
                    LIMIT %s
                """, (limit,))
            else:
                cur.execute("""
                    SELECT source, path, last_entropy, last_time, note
                    FROM files
                    WHERE flagged=1 AND source=%s
                    ORDER BY last_time DESC
                    LIMIT %s
                """, (src_filter, limit))
            rows = cur.fetchall()
            for r in rows:
                r["last_time"] = _fmt_local(r["last_time"])
                if r.get("last_entropy") is not None:
                    r["last_entropy"] = f"{r['last_entropy']:.3f}"
            click.echo("== FLAGGED ==")
            _print_rows(rows, ["source","path","last_entropy","last_time","note"])

        elif what == "av":
            if src_filter is None:
                cur.execute("""
                    SELECT detected_at, source, path, signature, engine, action, quarantine_path
                    FROM av_events
                    WHERE detected_at >= %s
                    ORDER BY detected_at DESC
                    LIMIT %s
                """, (since_utc, limit))
            else:
                cur.execute("""
                    SELECT detected_at, source, path, signature, engine, action, quarantine_path
                    FROM av_events
                    WHERE source=%s AND detected_at >= %s
                    ORDER BY detected_at DESC
                    LIMIT %s
                """, (src_filter, since_utc, limit))
            rows = cur.fetchall()
            for r in rows:
                r["detected_at"] = _fmt_local(r["detected_at"])
            click.echo(f"== AV-EVENTS (letzte {hours}h) ==")
            _print_rows(rows, ["detected_at","source","path","signature","engine","action","quarantine_path"])

        elif what == "summary":
            if src_filter is None:
                cur.execute("""
                    SELECT finished_at, source, candidates, files_processed, bytes_processed,
                           flagged_new_count, flagged_total_after, missing_count,
                           changed_count, reverified_count, av_found_count, av_quarantined_count
                    FROM scan_summary
                    ORDER BY finished_at DESC
                    LIMIT %s
                """, (limit,))
            else:
                cur.execute("""
                    SELECT finished_at, source, candidates, files_processed, bytes_processed,
                           flagged_new_count, flagged_total_after, missing_count,
                           changed_count, reverified_count, av_found_count, av_quarantined_count
                    FROM scan_summary
                    WHERE source=%s
                    ORDER BY finished_at DESC
                    LIMIT %s
                """, (src_filter, limit))
            rows = cur.fetchall()
            for r in rows:
                r["finished_at"] = _fmt_local(r["finished_at"])
            click.echo("== SCAN SUMMARY ==")
            _print_rows(rows, [
                "finished_at","source","candidates","files_processed","bytes_processed",
                "flagged_new_count","flagged_total_after","missing_count",
                "changed_count","reverified_count","av_found_count","av_quarantined_count"
            ])

        elif what == "backup-last":
            # Letzte Backup-Runs aus backup_runs (falls Tabelle existiert)
            try:
                cols = [
                    "started_at","finished_at","source","mode","policy_reason",
                    "status","rc","files_added","files_changed","files_total",
                    "size_original_bytes","size_compressed_bytes","size_dedup_bytes",
                    "duration_seconds","archive","repo"
                ]
                if src_filter is None:
                    cur.execute(f"""
                        SELECT {", ".join(cols)}
                        FROM backup_runs
                        ORDER BY started_at DESC
                        LIMIT %s
                    """, (limit,))
                else:
                    cur.execute(f"""
                        SELECT {", ".join(cols)}
                        FROM backup_runs
                        WHERE source=%s
                        ORDER BY started_at DESC
                        LIMIT %s
                    """, (src_filter, limit))
                rows = cur.fetchall()

                # Zeiten lokal formatieren
                for r in rows:
                    r["started_at"]  = _fmt_local(r.get("started_at"))
                    r["finished_at"] = _fmt_local(r.get("finished_at"))

                    # Größen hübsch (MiB/GiB) – optional knapp halten
                    def _fmt_sz(x):
                        try:
                            x = int(x or 0)
                            if x >= 1024**3: return f"{x/1024**3:.2f} GiB"
                            if x >= 1024**2: return f"{x/1024**2:.2f} MiB"
                            if x >= 1024:    return f"{x/1024:.2f} KiB"
                            return str(x)
                        except: return "-"
                    r["size_original_bytes"]   = _fmt_sz(r.get("size_original_bytes"))
                    r["size_compressed_bytes"] = _fmt_sz(r.get("size_compressed_bytes"))
                    r["size_dedup_bytes"]      = _fmt_sz(r.get("size_dedup_bytes"))

                click.echo("== BACKUP LAST ==")
                _print_rows(rows, [
                    "started_at","finished_at","source","mode","status","rc",
                    "files_added","files_changed","files_total",
                    "size_original_bytes","size_compressed_bytes","size_dedup_bytes",
                    "duration_seconds","archive"
                ])
            except Exception:
                logging.exception("backup-last report fehlgeschlagen")
                click.echo("(backup_runs Tabelle nicht vorhanden?)")

        elif what == "last-runs":
            # Erwartete Standard-Quellen (werden mit DB-Quellen vereinigt)
            expected_sources = ["os", "nas", "os-av", "nas-av", "os-av-weekly", "nas-av-weekly"]

            # Quellen dynamisch aus allen Tabellen ermitteln (deckt weitere Labels ab)
            cur.execute("SELECT DISTINCT source FROM files WHERE source IS NOT NULL")
            src_files = {r["source"] for r in cur.fetchall() if r["source"]}

            cur.execute("SELECT DISTINCT source FROM scan_summary WHERE source IS NOT NULL")
            src_summary = {r["source"] for r in cur.fetchall() if r["source"]}

            cur.execute("SELECT DISTINCT source FROM av_events WHERE source IS NOT NULL")
            src_av = {r["source"] for r in cur.fetchall() if r["source"]}

            all_sources = sorted(set(expected_sources) | src_files | src_summary | src_av)

            # Optional filtern
            if src_filter:
                all_sources = [s for s in all_sources if s == src_filter]

            # Vorab-Aggregationen holen
            # (1) Letzte scan_summary je Quelle inkl. scan_paths
            cur.execute("""
                SELECT s.source,
                       s.started_at   AS last_started,
                       s.finished_at  AS last_finished,
                       s.scan_paths   AS scan_paths
                FROM scan_summary s
                JOIN (
                    SELECT source, MAX(finished_at) AS max_finished
                    FROM scan_summary
                    GROUP BY source
                ) t ON t.source = s.source AND t.max_finished = s.finished_at
            """)
            ss_rows = {r["source"]: r for r in cur.fetchall() if r["source"]}

            # (2) Letztes AV-Event je Quelle
            cur.execute("""
                SELECT source, MAX(detected_at) AS last_av_event
                FROM av_events
                GROUP BY source
            """)
            av_last = {r["source"]: r for r in cur.fetchall() if r["source"]}

            # (3) Eindeutige AV-Events (path+signature) der letzten 24h je Quelle
            #    -> verhindert Doppelzählungen, wenn derselbe Fund mehrfach gemeldet wird
            cur.execute("""
                SELECT source,
                       COUNT(DISTINCT CONCAT(signature, '\x00', path)) AS cnt_24h
                FROM av_events
                WHERE detected_at >= UTC_TIMESTAMP() - INTERVAL 24 HOUR
                GROUP BY source
            """)
            av_24h = {r["source"]: int(r["cnt_24h"]) for r in cur.fetchall() if r["source"]}

            # Ausgabe
            click.echo("== LAST RUNS ==")
            click.echo("source           | last_started          | last_finished         | last_av_event         | av_events_24h | scan_paths")
            click.echo("-" * 160)
            for s in all_sources:
                ls = _fmt_local(ss_rows.get(s, {}).get("last_started"))
                lf = _fmt_local(ss_rows.get(s, {}).get("last_finished"))
                la = _fmt_local(av_last.get(s, {}).get("last_av_event"))
                c24 = av_24h.get(s, 0)
                sp = ss_rows.get(s, {}).get("scan_paths") or "-"
                click.echo(f"{s:<16} | {ls:<20} | {lf:<20} | {la:<20} | {c24:<13} | {sp}")


        elif what == "missing":
            if src_filter is None:
                cur.execute("""
                    SELECT source, path, missing_since
                    FROM files
                    WHERE missing_since IS NOT NULL
                    ORDER BY missing_since DESC
                    LIMIT %s
                """, (limit,))
            else:
                cur.execute("""
                    SELECT source, path, missing_since
                    FROM files
                    WHERE source=%s AND missing_since IS NOT NULL
                    ORDER BY missing_since DESC
                    LIMIT %s
                """, (src_filter, limit))
            rows = cur.fetchall()
            for r in rows:
                r["missing_since"] = _fmt_local(r["missing_since"])
            click.echo("== MISSING ==")
            _print_rows(rows, ["source","path","missing_since"])

        elif what == "recent":
            if src_filter is None:
                cur.execute("""
                    SELECT source, path, last_entropy, last_time, flagged
                    FROM files
                    WHERE last_time >= %s
                    ORDER BY last_time DESC
                    LIMIT %s
                """, (since_utc, limit))
            else:
                cur.execute("""
                    SELECT source, path, last_entropy, last_time, flagged
                    FROM files
                    WHERE source=%s AND last_time >= %s
                    ORDER BY last_time DESC
                    LIMIT %s
                """, (src_filter, since_utc, limit))
            rows = cur.fetchall()
            for r in rows:
                r["last_time"] = _fmt_local(r["last_time"])
                if r.get("last_entropy") is not None:
                    r["last_entropy"] = f"{r['last_entropy']:.3f}"
                r["flagged"] = "⚠️" if r.get("flagged") else ""
            click.echo(f"== RECENT (letzte {hours}h) ==")
            _print_rows(rows, ["source","path","last_entropy","last_time","flagged"])

        else:
            click.echo("Unbekannter Report. Siehe --help.")

        cur.close()
        conn.close()
    except Exception:
        logging.exception("db-report fehlgeschlagen")
    finally:
        teardown_logging(listener)

# Health-Status CLI
@cli.command("status")
@click.option("--window-min", type=int, default=None, help="Zeitraum in Minuten (override HEALTH_WINDOW_MIN).")
@click.option("--json-out", type=click.Path(dir_okay=False), help="Optional: JSON-Report schreiben.")
def status_cmd(window_min, json_out):
    """
    Liefert Health-Status für Safe-Backup-Gating.
    Exit-Codes: 0=grün, 1=gelb (warn), 2=rot (alarm)
    JSON: {"status":"green|yellow|red", "since":"ISO", "counters":{...}}
    """
    cfg, trace = load_config()
    logger, q, listener = setup_logging(cfg)
    try:
        import datetime as _dt
        import mysql.connector as mariadb
        from zoneinfo import ZoneInfo

        # ENV/Defaults
        W = int(os.getenv("HEALTH_WINDOW_MIN", "120"))
        if window_min is not None: W = max(5, window_min)
        AV_MAX = int(os.getenv("HEALTH_AV_FINDINGS_MAX", "0"))
        FLAG_MAX = int(os.getenv("HEALTH_FLAGGED_MAX", "0"))
        SAFEAGE = int(os.getenv("HEALTH_SAFEAGE_MIN", "30"))

        now = now_db()
        t_from = now - _dt.timedelta(minutes=W)

        conn = mariadb.connect(
            host=cfg["DB_HOST"], port=cfg["DB_PORT"], user=cfg["DB_USER"],
            password=cfg["DB_PASS"], database=cfg["DB_NAME"]
        )
        cur = conn.cursor(dictionary=True)

        # 1) Letzte Läufe (scan + av-scan) innerhalb des Fensters
        cur.execute("""
            SELECT kind, MAX(ts) AS last_ts
            FROM run_summaries
            WHERE ts >= %s
            GROUP BY kind
        """, (t_from,))
        runs = {r["kind"]: r["last_ts"] for r in cur.fetchall()}

        # 2) AV-Funde im Fenster
        cur.execute("""
            SELECT COUNT(*) AS c FROM av_events
            WHERE ts >= %s AND severity >= %s
        """, (t_from, 1))
        av_count = cur.fetchone()["c"]

        # 3) Flagged Files im Fenster
        cur.execute("""
            SELECT COUNT(*) AS c FROM files
            WHERE flagged=1 AND (last_time >= %s OR missing_since >= %s)
        """, (t_from, t_from))
        flagged = cur.fetchone()["c"]

        # 4) Heuristik bewerten
        last_scan_ts = runs.get("scan")
        last_av_ts   = runs.get("av-scan")
        age_ok = True
        if last_scan_ts:
            age_min = (now - last_scan_ts).total_seconds()/60.0
            age_ok = (age_min >= SAFEAGE)

        status = "green"; code = 0; reasons = []
        if not last_scan_ts or not last_av_ts:
            status, code = "yellow", 1
            reasons.append("no_recent_runs")
        if av_count > AV_MAX:
            status, code = "red", 2
            reasons.append("av_findings")
        if flagged > FLAG_MAX:
            status, code = "red", 2
            reasons.append("flagged_files")
        if not age_ok and code == 0:
            status, code = "yellow", 1
            reasons.append("too_fresh_to_trust")

        report = {
            "status": status,
            "since": now.isoformat(),
            "window_min": W,
            "counters": {
                "av_findings": int(av_count),
                "flagged": int(flagged),
            },
            "last_runs": {
                "scan": (last_scan_ts.isoformat() if last_scan_ts else None),
                "av_scan": (last_av_ts.isoformat() if last_av_ts else None),
            },
            "reasons": reasons,
        }

        js = json.dumps(report, ensure_ascii=False)
        click.echo(js)
        if json_out:
            with open(json_out, "w") as f:
                f.write(js)
        sys.exit(code)
    finally:
        teardown_logging(listener)




# Tagging
@cli.command("tag-exempt")
@click.argument("path", type=str)
@click.pass_context
def tag_exempt(ctx, path):
    """Markiert eine Datei als 'score_exempt' (weiter messen, aber nicht alarmieren)."""
    cfg, trace = load_config(ctx, "tag-exempt"); logger, q, listener = setup_logging(cfg)
    key = db_key_from_path(path)
    conn = db_connect(cfg); cur = conn.cursor()
    cur.execute("UPDATE files SET score_exempt=1 WHERE path=%s", (key,))
    cur.close(); conn.close()
    click.echo("OK: exempt gesetzt.")
    _log_load_trace(trace)
    teardown_logging(listener)

@cli.command("tag-normal")
@click.argument("path", type=str)
@click.pass_context
def tag_normal(ctx, path):
    """Entfernt das 'score_exempt'-Flag."""
    cfg, trace = load_config(ctx, "tag-normal"); logger, q, listener = setup_logging(cfg)
    key = db_key_from_path(path)
    conn = db_connect(cfg); cur = conn.cursor()
    cur.execute("UPDATE files SET score_exempt=0 WHERE path=%s", (key,))
    cur.close(); conn.close()
    click.echo("OK: exempt entfernt.")
    _log_load_trace(trace)
    teardown_logging(listener)

@cli.command("stats")
@click.pass_context
def stats(ctx):
    """Kleine Übersicht: Gesamt, Flagged, Missing, Exempt."""
    cfg, trace = load_config(ctx, "stats"); logger, q, listener = setup_logging(cfg)
    conn = db_connect(cfg); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files"); total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM files WHERE flagged=1"); flg = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM files WHERE missing_since IS NOT NULL"); miss = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM files WHERE score_exempt=1"); exm = cur.fetchone()[0]
    cur.close(); conn.close()
    click.echo(f"Total: {total}, Flagged: {flg}, Missing: {miss}, Exempt: {exm}")
    _log_load_trace(trace)
    teardown_logging(listener)

@cli.command("av-scan")
@click.option("--paths", required=True, help="Kommagetrennte Pfade für ClamAV-Scan (Pflicht).")
@click.pass_context
def av_scan(ctx, paths):
    """ClamAV-Scan über die angegebenen Pfade; Mail bei Funden."""
    cfg, trace = load_config(ctx, "av-scan")
    logger, q, listener = setup_logging(cfg)
    _log_load_trace(trace)

    # Startzeit (UTC) für die Summary dieses AV-Laufes
    av_started_at = now_db()
    scan_paths_list = _parse_paths_arg(paths)

    try:
        if not cfg.get("CLAMAV_ENABLE", False):
            click.echo("CLAMAV_ENABLE=0 – überspringe.")
            return

        if not scan_paths_list:
            click.echo("Fehler: --paths ist leer.")
            return

        # Scan starten
        rc, out, err = _clamav_run(scan_paths_list, cfg)
        findings = _clamav_parse_findings(out)

        # Funde aus Quarantäne-Pfad ignorieren (clamdscan kann nicht --exclude)
        qdir = (cfg.get("AV_QUARANTINE_DIR") or "").rstrip("/")
        if qdir:
            qdir_abs = os.path.abspath(qdir)
            def _in_quarantine(p: str) -> bool:
                try:
                    return os.path.commonpath([os.path.abspath(p), qdir_abs]) == qdir_abs
                except Exception:
                    return False
            findings = [(p, sig) for (p, sig) in findings if not _in_quarantine(p)]

        # RC interpretieren (0=sauber, 1=Funde, 2=Fehler)
        if rc == 0:
            logging.info("ClamAV: sauber. Findings=%d", len(findings))
        elif rc == 1:
            logging.warning("ClamAV: MALWARE gefunden! Findings=%d", len(findings))
        else:
            logging.error("ClamAV: Fehler (RC=%d). Findings=%d", rc, len(findings))

        conn = None
        quarantined_n = 0
        try:
            conn = db_connect(cfg)
            ensure_schema(conn)

            # AV-Events + Quarantäne
            for p, sig in findings:
                logging.warning("ClamAV: %s  ->  %s", p, sig)
                try:
                    act, qpath = _quarantine_file(p, cfg)  # setzt "already_quarantined", falls im qdir
                    engine = "clamd" if cfg.get("CLAMAV_USE_CLAMD") else "clamscan"

                    final_action = ("quarantine" if act == "quarantine" else act)
                    if final_action == "quarantine":
                        quarantined_n += 1

                    record_av_event(
                        conn=conn,
                        cfg=cfg,
                        path=p,
                        signature=sig,
                        engine=engine,
                        action=final_action,            # "quarantine" | "copy" | "chmod" | "already_quarantined" | "none"
                        quarantine_path=(qpath or None),
                        extra=None
                    )
                except Exception:
                    logging.exception("AV-Event DB-Insert fehlgeschlagen für %s (%s)", p, sig)

            # Commit der AV-Events (falls welche geschrieben wurden)
            try:
                conn.commit()
            except Exception:
                logging.exception("AV-Events Commit fehlgeschlagen")

            # IMMER einen scan_summary-Datensatz schreiben (auch ohne Findings!)
            try:
                note = (
                    f"av-scan paths={','.join(scan_paths_list)}; "
                    f"findings={len(findings)}; quarantined={quarantined_n}; rc={rc}"
                )
                write_scan_summary(
                    conn=conn,
                    cfg=cfg,
                    started_at=av_started_at,
                    finished_at=now_db(),
                    candidates_count=0,
                    files_processed=0,
                    bytes_processed=0,
                    flagged_new_count=0,
                    changed_count=0,
                    reverified_count=0,
                    note=note,
                    scan_paths=",".join(scan_paths_list)
                )
                try:
                    conn.commit()
                except Exception:
                    logging.exception("AV-Scan Summary Commit fehlgeschlagen")
            except Exception:
                logging.exception("AV-Scan Summary Insert fehlgeschlagen")

        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        # Kurzes Summary (auch für Mail)
        summary = f"ClamAV ExitCode={rc}, Findings={len(findings)}"
        logging.info(summary)

        # Mail bei Funden
        if findings and cfg.get("MAIL_ENABLE", False):
            lines = "\n".join(f"{p}\n  {sig}" for p, sig in findings[:50])
            send_alert_email(
                subject_core=f"{len(findings)} ClamAV-Fund(e)",
                body=(summary + "\n\n" + lines),
                cfg=cfg
            )

    finally:
        teardown_logging(listener)


if __name__ == "__main__":
    cli()
