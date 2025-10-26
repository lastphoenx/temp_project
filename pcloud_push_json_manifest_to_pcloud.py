#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pcloud_push_json_manifest_to_pcloud.py

Lädt ein lokales Manifest (v2) nach pCloud.

Zwei Betriebsarten:
- --snapshot-mode objects  : Hash-Object-Store + JSON-Stubs (effizient, globale Dedupe).
- --snapshot-mode 1to1     : 1:1-Snapshot-Bäume; erstes Auftreten eines Inhalts wird "materialisiert"
                             (echte Datei im Snapshot), weitere Hardlinks als Stubs.
                             Content-Index in _snapshots/_index/content_index.json.

Erwartetes Manifest (schema=2):
{
  "schema": 2,
  "snapshot": "YYYY-mm-dd-HHMMSS" oder ähnlich,
  "root": "/abs/pfad/zum/snapshot",
  "hash": "sha256",
  "follow_symlinks": bool,
  "follow_hardlinks": bool,
  "items": [
    {"type":"dir","relpath":"..."},
    {"type":"file","relpath":"...", "size":..., "mtime":..., "source_path":"...", "sha256":"...", "ext":".txt",
     "inode": {"dev": 2049, "ino": 228196364, "nlink": 3}}
    ...
  ]
}

Benötigt: pcloud_bin_lib.py im selben Verzeichnis oder PYTHONPATH.
"""

from __future__ import annotations
import os, sys, json, argparse, time
from typing import Dict, Any, Optional, Tuple



# ---- Lib laden ----
try:
    import pcloud_bin_lib as pc
except Exception as e:
    print(f"Fehler: pcloud_bin_lib konnte nicht importiert werden: {e}", file=sys.stderr)
    sys.exit(2)


# Performance-Messung
fid_cache = {}
fid_lookups = 0          # Anzahl _fid_for Aufrufe
fid_cache_hits = 0       # Treffer im Cache
fid_rest_ms = 0.0        # aufsummierte Zeit in pc.resolve_fileid_cached
t_phase_start = time.time()

# --- shared fileid cache for this process ---
_fid_cache_shared: dict = {}

# ----------------- Utilities -----------------

def _ensure_parent(cfg, remote_path: str, *, dry: bool = False) -> None:
    """
    Stellt sicher, dass alle Elternordner für `remote_path` existieren.
    Delegiert vollständig an pcloud_bin_lib.ensure_parent_dirs(...).
    """
    import pcloud_bin_lib as pc
    if dry:
        return
    pc.ensure_parent_dirs(cfg, remote_path)


def write_hardlink_stub_1to1(cfg, snapshots_root, snapshot_name, relpath, file_item, node, dry=False):
    """
    Schreibt die .meta.json für einen 1:1-Hardlink-Stub und sorgt dafür,
    dass 'fileid' (falls möglich) gesetzt und im Index-Node mitgeführt wird.
    """
    import json as _json
    import pcloud_bin_lib as pc

    meta_path = f"{snapshots_root.rstrip('/')}/{snapshot_name}/{relpath}.meta.json"

    # Ordner sicherstellen (nur via Lib-Helper)
    _ensure_parent(cfg, meta_path, dry=dry)

    # fileid nachziehen, falls im Node noch None
    fileid = node.get("fileid")
    if not fileid and node.get("anchor_path"):
        fid = pc.resolve_fileid_cached(cfg, path=node.get("anchor_path"), cache=_fid_cache_shared)
        if fid:
            fileid = fid
            node["fileid"] = fid

    payload = {
        "type":   "hardlink",
        "sha256": (file_item.get("sha256") or "").lower(),
        "size":   int(file_item.get("size") or 0),
        "mtime":  float(file_item.get("mtime") or 0.0),
        "inode":  {
            "dev":   int(((file_item.get("inode") or {}).get("dev")  or 0)),
            "ino":   int(((file_item.get("inode") or {}).get("ino")  or 0)),
            "nlink": int(((file_item.get("inode") or {}).get("nlink") or 1)),
        },
        "anchor_path": node.get("anchor_path"),
        "fileid": fileid if fileid is not None else None,
        "snapshot": snapshot_name,
        "relpath": relpath,
    }

    if dry:
        print(f"[dry] stub: {meta_path} -> {node.get('anchor_path')}")
    else:
        pc.write_json_at_path(cfg, path=meta_path, obj=payload)

    # holders[] pflegen
    holders = node.setdefault("holders", [])
    h = {"snapshot": snapshot_name, "relpath": relpath}
    if h not in holders:
        holders.append(h)

    return meta_path, payload


def stat_file_safe(cfg: dict, *, path: Optional[str]=None, fileid: Optional[int]=None) -> Optional[dict]:
    """Stat-Datei; gibt None bei 'not found' zurück (anstatt Exception)."""
    try:
        if path is not None:
            md = pc.stat_file(cfg, path=pc._norm_remote_path(path), with_checksum=False, enrich_path=True)
        else:
            md = pc.stat_file(cfg, fileid=int(fileid), with_checksum=False, enrich_path=True)
        if not md or md.get("isfolder"):
            return None
        return md
    except Exception as e:
        msg = (str(e) or "").lower()
        if " 2055" in msg or "not found" in msg or "no such file" in msg:
            return None
        return None
def ensure_parent_dirs(cfg: dict, remote_path: str, *, dry: bool=False) -> None:
    """Sorgt dafür, dass alle Ordner bis zum parent von remote_path existieren."""
    import pcloud_bin_lib as pc
    p = pc._norm_remote_path(remote_path)
    parent = p.rsplit("/", 1)[0] or "/"
    if dry:
        return
    pc.ensure_path(cfg, parent)

def upload_json_stub(cfg: dict, remote_path: str, payload: dict, *, dry: bool=False) -> None:
    if dry:
        target = payload.get("object_path") or payload.get("anchor_path") or payload.get("sha256")
        print(f"[dry] stub: {remote_path} -> {target}")
        return
    pc.ensure_parent_dirs(cfg, remote_path)
    pc.write_json_at_path(cfg, remote_path, payload)

def _bytes_to_tempfile(b: bytes) -> str:
    import tempfile, os
    fd, p = tempfile.mkstemp(prefix="pcloud_stub_", suffix=".json")
    with os.fdopen(fd, "wb") as f:
        f.write(b)
    return p

def object_path_for(objects_root: str, sha256: str, ext: Optional[str], layout: str="two-level") -> str:
    """Pfad im Object-Store. layout='two-level' legt /_objects/xx/sha.ext an."""
    sha = (sha256 or "").lower()
    if not sha or len(sha) < 2:
        sub = "zz"
    else:
        sub = sha[:2]
    e = (ext or "").lstrip(".")
    tail = sha if not e else (sha + "." + e)
    return f"{objects_root.rstrip('/')}/{sub}/{tail}"

def snapshot_path_for(snapshots_root: str, snapshot: str, relpath: str) -> str:
    return f"{snapshots_root.rstrip('/')}/{snapshot}/{relpath}".replace("//", "/")

def stub_path_for(snapshots_root: str, snapshot: str, relpath: str) -> str:
    return f"{snapshots_root.rstrip('/')}/{snapshot}/{relpath}.meta.json".replace("//", "/")

def key_from_inode(item: dict) -> Optional[str]:
    """Erzeugt einen Key für Hardlink-Gruppierung; None wenn kein inode."""
    ino = (item.get("inode") or {})
    dev = ino.get("dev"); n = ino.get("ino")
    if dev is None or n is None:
        return None
    return f"{dev}:{n}"

def load_content_index(cfg: dict, snapshots_root: str) -> dict:
    """
    Lädt _snapshots/_index/content_index.json robust.
    - Wenn Datei fehlt/kaputt: leeren Index zurückgeben.
    - Ein 'result'≠0 im JSON gilt als API-Fehler (dann leerer Index).
    - Fehlt 'result' völlig (Normalfall bei echter Index-Datei) → OK.
    """
    import json as _json
    import pcloud_bin_lib as pc

    idx_path = f"{snapshots_root.rstrip('/')}/_index/content_index.json"
    try:
        txt = pc.get_textfile(cfg, path=idx_path)
        j = _json.loads(txt)

        # Nur als API-Fehler werten, wenn 'result' vorhanden *und* != 0
        if isinstance(j, dict) and "result" in j and j.get("result") != 0:
            return {"version": 1, "items": {}}

        if "items" not in j or not isinstance(j["items"], dict):
            j["items"] = {}
        if "version" not in j:
            j["version"] = 1
        return j
    except Exception:
        return {"version": 1, "items": {}}

def save_content_index(cfg: dict, snapshots_root: str, index: dict, *, dry: bool=False) -> None:
    """
    content_index.json effizient schreiben:
    - ohne erneutes ensure()
    - minified JSON
    """
    import pcloud_bin_lib as pc
    idx_dir  = f"{snapshots_root.rstrip('/')}/_index"
    idx_name = "content_index.json"

    if dry:
        print(f"[dry] write index: {idx_dir}/{idx_name} (items={len(index.get('items',{}))})")
        return

    # Ordner muss existieren (wurde vorher per Batch-Ensure angelegt)
    fid = pc.stat_folderid_fast(cfg, idx_dir)
    if not fid:
        # sehr selten: Fallback (legt an und holt folderid)
        fid = pc.ensure_path(cfg, idx_dir)

    pc.write_json_to_folderid(cfg, folderid=int(fid), filename=idx_name, obj=index, minify=True)

def list_remote_snapshot_names(cfg: dict, snapshots_root: str) -> set[str]:
    """Liest die Ordnernamen unter <snapshots_root> (außer '_index')."""
    out: set[str] = set()
    try:
        top = pc.listfolder(cfg, path=snapshots_root, recursive=False, nofiles=True, showpath=False)
        for it in (top.get("metadata", {}) or {}).get("contents", []) or []:
            if it.get("isfolder") and it.get("name") and it.get("name") != "_index":
                out.add(it["name"])
    except Exception:
        pass
    return out

def list_local_snapshot_names(manifest_root: str) -> set[str]:
    """Liest Geschwister-Ordner des gegebenen Snapshot-Roots (RTB-Stil)."""
    import os as _os
    base = _os.path.dirname(_os.path.abspath(manifest_root))  # parent von ".../<snapshot>"
    names = set()
    try:
        for n in _os.listdir(base):
            p = _os.path.join(base, n)
            if _os.path.isdir(p) and n not in ("latest",):
                names.add(n)
    except Exception:
        pass
    return names


def finalize_index_fileids(cfg, snapshots_root):
    """
    Lädt <snapshots_root>/_index/content_index.json und füllt fehlende fileids
    (für Nodes mit anchor_path) via REST /stat nach. Schreibt nur bei Änderungen.
    Return: Anzahl reparierter Einträge.
    """
    import json as _json
    import time, os
    import requests
    import pcloud_bin_lib as pc

    start = time.time()

    idx_path = f"{pc._norm_remote_path(snapshots_root).rstrip('/')}/_index/content_index.json"
    try:
        index = _json.loads(pc.get_textfile(cfg, path=idx_path))
    except Exception:
        return 0
    if not isinstance(index, dict):
        return 0

    items = index.get("items", {})
    if not isinstance(items, dict) or not items:
        return 0

    repaired = 0
    changed  = False

    # Gemeinsamer Cache mit dem Modul-Cache teilen:
    global _fid_cache_shared
    cache = _fid_cache_shared

    for sha, node in list(items.items()):
        if not isinstance(node, dict):
            continue
        if (node.get("fileid") in (None, "")) and node.get("anchor_path"):
            fid = pc.resolve_fileid_cached(cfg, path=node["anchor_path"], cache=cache)
            if fid:
                node["fileid"] = fid
                repaired += 1
                changed = True

    if changed:
        try:
            pc.put_textfile(cfg, path=idx_path, text=_json.dumps(index, ensure_ascii=False, indent=2))
        except Exception:
            pc.write_json_at_path(cfg, path=idx_path, obj=index)

    if os.environ.get("PCLOUD_TIMING") == "1":
        print(f"[timing] finalize_index_fileids: fixed={repaired}, total={time.time()-start:.2f}s")

    return repaired

def _batch_ensure_paths(cfg: dict, paths: list[str], *, dry: bool = False) -> None:
    """
    Batch-Version von ensure_parent_dirs für mehrere Pfade.
    Nutzt createfolderrecursive (ein Call pro Parent-Kette).
    """
    import os
    import pcloud_bin_lib as pc

    if not paths:
        return

    # eindeutige Parents sammeln
    parents = { os.path.dirname(p.rstrip("/")) for p in paths if p }
    # stabile Reihenfolge (kann helfen beim Debug)
    parents = sorted(parents)

    for parent in parents:
        try:
            pc.ensure_path(cfg, parent, dry=dry)
        except Exception:
            # nicht hart abbrechen – idempotent, nächste versuchen
            continue

def _batch_write_stubs(cfg: dict, stubs: list[tuple[str, dict]], *, dry: bool = False) -> None:
    """
    Batch: Stub-JSONs schreiben – einmalig Parents ensuren (extern),
    dann pro Parent-Ordner die folderid ermitteln und direkt hochladen.
    """
    import pcloud_bin_lib as pc
    import concurrent.futures, os

    if not stubs:
        return

    threads = int(os.environ.get("PCLOUD_STUB_THREADS","1") or "1")
    # 1) Parents & Namen sammeln
    parents: dict[str, list[tuple[str, dict]]] = {}
    for fullpath, payload in stubs:
        fullpath = fullpath.replace("//", "/")
        parent = fullpath.rsplit("/", 1)[0]
        name   = fullpath.rsplit("/", 1)[1]
        parents.setdefault(parent, []).append((name, payload))

    if dry:
        for parent, items in parents.items():
            for name, _payload in items:
                print(f"[dry] write stub: {parent}/{name}")
        return

    # 2) Einmalig folderid je Parent ermitteln (ohne anlegen)
    parent_fids: dict[str, int] = {}
    for parent in parents.keys():
        fid = pc.stat_folderid_fast(cfg, parent)
        if not fid:
            # sollte selten sein, da vorher Batch-Ensure lief; fallback:
            fid = pc.ensure_path(cfg, parent)
        parent_fids[parent] = int(fid)

    # 3) Uploads (leicht parallelisierbar)
    threads = 1
    try:
        threads = max(1, int(os.environ.get("PCLOUD_STUB_THREADS", "1")))
    except Exception:
        threads = 1

    def _upload_one(args):
        parent, name, payload = args
        return pc.write_json_to_folderid(cfg, folderid=parent_fids[parent], filename=name, obj=payload, minify=True)

    tasks = [(parent, name, payload) for parent, items in parents.items() for (name, payload) in items]
    if threads > 1 and len(tasks) > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as ex:
            list(ex.map(_upload_one, tasks))
    else:
        for t in tasks:
            _upload_one(t)
    print(f"[timing] stubs: count={len(stubs)} threads={threads}")

# ----------------- Haupt-Logik -----------------

def push_objects_mode(cfg: dict, manifest: dict, dest_root: str, *, dry: bool, objects_layout: str="two-level") -> None:
    """Hash-Object-Store + Stubs in Snapshot."""
    objects_root   = f"{dest_root.rstrip('/')}/_objects"
    snapshots_root = f"{dest_root.rstrip('/')}/_snapshots"
    snapshot       = manifest["snapshot"]
    items          = manifest.get("items") or []

    uploaded = 0; skipped = 0; stubs = 0

    print(f"[plan] objects={objects_root} snapshot={snapshots_root}/{snapshot}")

    # 1) echte Objekte sicherstellen
    for it in items:
        if it.get("type") != "file": continue
        sha = it.get("sha256")
        ext = (it.get("ext") or "").lstrip(".")
        if not sha:
            print(f"[warn] file ohne sha256: {it.get('relpath')}", file=sys.stderr)
            continue

        obj_path = object_path_for(objects_root, sha, ext, layout=objects_layout)
        md = stat_file_safe(cfg, path=obj_path)
        if md:
            skipped += 1
        else:
            if dry:
                print(f"[dry] upload object: {obj_path}  <- {it.get('source_path')}")
            else:
                ensure_parent_dirs(cfg, obj_path, dry=False)
                pc.upload_streaming(cfg, it["source_path"], dest_path=obj_path, filename=os.path.basename(obj_path))
            uploaded += 1

    print(f"objects: uploaded={uploaded} skipped={skipped}")

    # 2) Snapshot-Stubs erzeugen
    for it in items:
        if it.get("type") != "file": continue
        sha = it.get("sha256")
        ext = (it.get("ext") or "").lstrip(".")
        obj_path = object_path_for(objects_root, sha, ext, layout=objects_layout)
        stub_remote = stub_path_for(snapshots_root, snapshot, it["relpath"])
        payload = {
            "type": "link",
            "sha256": sha,
            "size": it.get("size"),
            "mtime": it.get("mtime"),
            "object_path": obj_path,
            "ext": ext or None,
            "inode": it.get("inode"),
            "snapshot": snapshot,
            "relpath": it.get("relpath"),
        }
        upload_json_stub(cfg, stub_remote, payload, dry=dry)
        stubs += 1

    print(f"stubs: {stubs} (snapshot={snapshot})")


def ensure_snapshots_layout(cfg: dict, dest_root: str, *, dry: bool = False) -> None:
    """
    Stellt sicher, dass <dest_root>/_snapshots und _snapshots/_index existieren
    und dass eine leere Index-Datei angelegt werden kann.
    """
    import pcloud_bin_lib as pc
    snapshots_root = f"{pc._norm_remote_path(dest_root).rstrip('/')}/_snapshots"
    index_dir = f"{snapshots_root}/_index"
    if dry:
        print(f"[dry] ensure: {snapshots_root}")
        print(f"[dry] ensure: {index_dir}")
        return
    pc.ensure_path(cfg, snapshots_root)
    pc.ensure_path(cfg, index_dir)

def push_1to1_mode(cfg, manifest, dest_root, *, dry=False, verbose=False):
    """
    1:1-Modus mit Resume-Unterstützung:
      - .upload_started Marker beim Start
      - .upload_complete Marker beim erfolgreichen Abschluss
      - Unvollständige Snapshots werden erkannt und neu gestartet
    """
    import os, json, time, concurrent.futures
    import pcloud_bin_lib as pc

    t_phase_start = time.time()
    ensure_ms = 0.0
    upload_ms = 0.0
    write_ms  = 0.0

    snapshot_name = manifest.get("snapshot") or "SNAPSHOT"
    dest_root = pc._norm_remote_path(dest_root)
    snapshots_root = f"{dest_root.rstrip('/')}/_snapshots"
    dest_snapshot_dir = f"{snapshots_root}/{snapshot_name}"

    # === NEU: Upload-Status-Marker ===
    marker_started = f"{dest_snapshot_dir}/.upload_started"
    marker_complete = f"{dest_snapshot_dir}/.upload_complete"
    
    # Prüfen ob unvollständiger Upload existiert
    incomplete_upload = False
    try:
        pc.stat_file(cfg, path=marker_started, with_checksum=False)
        # Started-Marker existiert
        try:
            pc.stat_file(cfg, path=marker_complete, with_checksum=False)
            # Complete-Marker auch da → Upload war erfolgreich
            print(f"[info] Snapshot {snapshot_name} bereits vollständig hochgeladen")
            return {"uploaded": 0, "stubs": 0, "resumed": False}
        except:
            # Nur Started, kein Complete → unvollständig!
            incomplete_upload = True
            print(f"[warn] Unvollständiger Upload erkannt für {snapshot_name} - starte neu")
    except:
        # Kein Started-Marker → frischer Upload
        pass
    
    # Bei unvollständigem Upload: Snapshot-Ordner löschen
    if incomplete_upload and not dry:
        try:
            print(f"[cleanup] Lösche unvollständigen Snapshot: {dest_snapshot_dir}")
            pc.delete_folder(cfg, path=dest_snapshot_dir, recursive=True)
        except Exception as e:
            print(f"[warn] Cleanup fehlgeschlagen: {e}")
    # === ENDE NEU ===

    print(f"[plan] 1to1 snapshot={dest_snapshot_dir}")

    # --- Einmalig alle Verzeichnisse aus dem Manifest anlegen (Batch) ---
    try:
        dir_paths = [
            f"{dest_snapshot_dir}/{it.get('relpath','').rstrip('/')}"
            for it in (manifest.get('items') or [])
            if it.get('type') == 'dir'
        ]
        _batch_ensure_paths(cfg, dir_paths, dry=dry)  # nutzt createfolderrecursive / ensure_path
    except Exception:
        # best-effort; im Zweifel legt Upload/Stub-Schritt fehlende Parents nochmal an
        pass

    # === NEU: Started-Marker setzen ===
    if not dry:
        try:
            pc.call_with_backoff(pc.ensure_path, cfg, dest_snapshot_dir)
            pc.call_with_backoff(pc.put_textfile, cfg, path=marker_started,
                          text=json.dumps({
                              "snapshot": snapshot_name,
                              "started_at": time.time(),
                              "host": os.uname().nodename
                          }))
        except Exception as e:
            print(f"[warn] Konnte Started-Marker nicht setzen: {e}")
    # === ENDE NEU ===

    # --- kleine Helfer ---
    def _ensure(path: str) -> None:
        nonlocal ensure_ms
        if not path:
            return
        if dry:
            if os.environ.get("PCLOUD_VERBOSE") == "1":
                print(f"[dry] ensure: {path}")
            return
        t0 = time.time()
        pc.call_with_backoff(pc.ensure_path, cfg, path)
        ensure_ms += (time.time() - t0) * 1000.0

    def _delete_if_exists(path: str) -> None:
        if dry:
            if os.environ.get("PCLOUD_VERBOSE") == "1":
                print(f"[dry] delete-if-exists: {path}")
            return
        try:
            md = pc.call_with_backoff(pc.stat_file_safe, cfg, path=path) or {}
            fid = md.get("fileid")
            if fid:
                pc.delete_file(cfg, fileid=int(fid))
        except Exception:
            pass

    # Index laden/initialisieren
    index = load_content_index(cfg, snapshots_root)
    items = index.setdefault("items", {})

    # Anchor-Cache aufbauen
    known_anchors = {}
    for sha, node in items.items():
        ap = node.get("anchor_path")
        fid = node.get("fileid")
        if ap and fid:
            known_anchors[sha] = (ap, fid)
    
    if known_anchors and os.environ.get("PCLOUD_VERBOSE") == "1":
        print(f"[prefetch] {len(known_anchors)} bekannte Anchors gecacht")

    # Hilfstabellen
    seen_inodes: dict[tuple[int,int], str] = {}
    uploaded = 0
    stubs = 0
    index_changed = False
    stubs_to_write: list[tuple[str, dict]] = []

    # --- Upload-Hilfsroutine ---
    def _upload_real_file(abs_src: str, dst_path: str):
        nonlocal upload_ms
        parent = os.path.dirname(dst_path.rstrip("/"))
        if parent:
            _ensure(parent)
        if dry:
            print(f"[dry] upload 1to1: {dst_path}  <- {abs_src}")
            return None
        t0 = time.time()
        res = pc.call_with_backoff(pc.upload_file, cfg, local_path=abs_src, remote_path=dst_path)
        upload_ms += (time.time() - t0) * 1000.0
        try:
            md = (res or {}).get("metadata") or {}
            return md.get("fileid")
        except Exception:
            return None

    # --- Stub sammeln ---
    def _queue_stub(relpath: str, file_item: dict, node: dict) -> None:
        nonlocal stubs, index_changed

        eager = os.environ.get("PCLOUD_EAGER_FILEID", "1") != "0"
        if eager and (not node.get("fileid")) and node.get("anchor_path"):
            fid = pc.resolve_fileid_cached(cfg, path=node["anchor_path"])
            if fid:
                node["fileid"] = fid
                index_changed = True

        meta_path = f"{dest_snapshot_dir}/{relpath}.meta.json"
        payload = {
            "type": "hardlink",
            "sha256": file_item.get("sha256"),
            "size": file_item.get("size"),
            "mtime": file_item.get("mtime"),
            "snapshot": snapshot_name,
            "relpath": relpath,
            "anchor_path": node.get("anchor_path"),
            "fileid": node.get("fileid") if node.get("fileid") is not None else None,
            "inode": file_item.get("inode"),
        }
        if dry:
            print(f"[dry] write stub: {meta_path}")
        else:
            stubs_to_write.append((meta_path, payload))
        stubs += 1

    # --- Hauptschleife: Items des Manifests ---
    for it in manifest.get("items") or []:
        if it.get("type") == "dir":
#            _ensure(f"{dest_snapshot_dir}/{it.get('relpath','').rstrip('/')}")
            continue
        if it.get("type") != "file":
            continue

        relpath = it.get("relpath") or ""
        src_abs = it.get("source_path") or ""
        sha = it.get("sha256") or ""
        inode = it.get("inode") or {}
        dev = int(inode.get("dev") or 0)
        ino = int(inode.get("ino") or 0)
        ino_key = (dev, ino)

        dst_path = f"{dest_snapshot_dir}/{relpath}"

        node = items.setdefault(sha, {"holders": []})
        
        if sha in known_anchors:
            anchor_path, anchor_fid = known_anchors[sha]
            if not node.get("anchor_path"):
                node["anchor_path"] = anchor_path
            if not node.get("fileid"):
                node["fileid"] = anchor_fid
        else:
            anchor_path = node.get("anchor_path") or ""
        
        is_anchor_here = (anchor_path == dst_path)

        if ino_key in seen_inodes:
            if not is_anchor_here:
                _queue_stub(relpath, it, node)
            else:
                _delete_if_exists(f"{dst_path}.meta.json")
            continue

        fid = None
        if not anchor_path:
            fid = _upload_real_file(src_abs, dst_path)
            if node.get("anchor_path") != dst_path:
                node["anchor_path"] = dst_path
                index_changed = True
            if fid and node.get("fileid") != fid:
                node["fileid"] = fid
                index_changed = True
            uploaded += 1
            _delete_if_exists(f"{dst_path}.meta.json")
        else:
            if is_anchor_here:
                _delete_if_exists(f"{dst_path}.meta.json")
            else:
                _queue_stub(relpath, it, node)

        h = {"snapshot": snapshot_name, "relpath": relpath}
        if h not in node["holders"]:
            node["holders"].append(h)
            index_changed = True

        seen_inodes[ino_key] = relpath


    # --- Batch: Stubs & Index schreiben (einmaliges Ensure + Writes) ---
    if not dry and stubs_to_write:
        t0 = time.time()
        _batch_write_stubs(cfg, stubs_to_write, dry=False)  # sorgt intern für 1x Parent-Ensure
        write_ms += (time.time() - t0) * 1000.0


    # Index schreiben
    if dry:
        print(f"[dry] write index: {snapshots_root}/_index/content_index.json (items={len(items)})")
    else:
        if index_changed:
            t0 = time.time()
            save_content_index(cfg, snapshots_root, index, dry=False)
            dt_ms = (time.time() - t0) * 1000.0
            write_ms += dt_ms 
            print(f"[timing] index_write_ms={int(dt_ms)}")
        else:
            print("[info] index unchanged (no write)")

    # FINALIZE
    if not dry:
        do_finalize = (os.environ.get("PCLOUD_SKIP_FINALIZE") in (None, "", "0"))
        if do_finalize and (uploaded > 0 or stubs > 0 or index_changed):
            try:
                finalize_index_fileids(cfg, snapshots_root)
            except Exception:
                pass

    # === NEU: Complete-Marker setzen ===
    if not dry:
        try:
            pc.put_textfile(cfg, path=marker_complete,
                          text=json.dumps({
                              "snapshot": snapshot_name,
                              "completed_at": time.time(),
                              "uploaded": uploaded,
                              "stubs": stubs
                          }))
            print(f"[success] Upload-Complete-Marker gesetzt")
        except Exception as e:
            print(f"[warn] Konnte Complete-Marker nicht setzen: {e}")
    # === ENDE NEU ===

    if os.environ.get("PCLOUD_TIMING") == "1":
        total_ms = (time.time() - t_phase_start) * 1000.0
        print(f"[timing] push_1to1: total={total_ms/1000:.2f}s, ensure={ensure_ms:.0f}ms, upload={upload_ms:.0f}ms, writes={write_ms:.0f}ms")

    print(f"1to1: uploaded={uploaded} stubs={stubs} (snapshot={snapshot_name})")
    return {"uploaded": uploaded, "stubs": stubs, "resumed": incomplete_upload}

def retention_sync_1to1(cfg, dest_root, *, local_snaps=None, dry=False, rewrite_stubs=True):
    """
    Retention/Prune für den 1:1-Modus, index-zentriert.

    Ablauf:
      - Remote-Snapshots unter <dest>/_snapshots mit lokalen (local_snaps) vergleichen.
      - Für jeden entfernten Remote-Snapshot:
          • Holders für gelöschte Snaps entfernen.
          • Liegt Anchor im gelöschten Snap:
              - Gibt es verbleibende Holder -> Anchor serverseitig in Pfad des jüngsten Holders moven,
                Index aktualisieren, Ziel-Stub entfernen, übrige Holder -> Stub (optional).
              - Keine Holder mehr -> Node entfernen.
          • Snapshot-Ordner löschen nur, wenn keine Blocker (z. B. fehlende fileid / Move-Fehler).
      - Index zuletzt schreiben (write-last), aber NUR wenn keine Blocker auftraten.
      - WICHTIG: am Anchor-Pfad gibt es KEINEN Stub; stale Stubs dort werden gelöscht.
    """
    import sys
    import json as _json
    import pcloud_bin_lib as pc

    # --- Hilfsfunktionen -----------------------------------------------------

    def _list_remote_snapshots(snapshots_root: str) -> list[str]:
        try:
            top = pc.listfolder(cfg, path=snapshots_root, recursive=False, nofiles=True, showpath=False) or {}
            contents = (top.get("metadata") or {}).get("contents") or []
            return sorted(c["name"] for c in contents if c.get("isfolder") and c.get("name") != "_index")
        except Exception:
            return []

    def _stat_fileid_safe(path: str):
        try:
            md = pc.stat_file(cfg, path=path, with_checksum=False) or {}
            return md.get("fileid")
        except Exception:
            return None

    def _load_index(snapshots_root: str) -> dict:
        idx_path = f"{snapshots_root}/_index/content_index.json"
        try:
            txt = pc.get_textfile(cfg, path=idx_path)
            j = _json.loads(txt)
            if not isinstance(j, dict):
                j = {"version": 1, "items": {}}
        except Exception:
            j = {"version": 1, "items": {}}
        if "items" not in j or not isinstance(j["items"], dict):
            j["items"] = {}
        if "version" not in j:
            j["version"] = 1
        return j

    def _save_index(snapshots_root: str, idx: dict, simulate: bool):
        nonlocal ret_index_write_ms
        if simulate:
            print(f"[dry] save index: items={len(idx.get('items', {}))}")
        else:
            t0 = time.time()
            save_content_index(cfg, snapshots_root, idx, dry=False)
            dt = (time.time() - t0) * 1000.0
            ret_index_write_ms += dt
            if os.environ.get("PCLOUD_TIMING") == "1":
                print(f"[timing] retention_index_write_ms={int(dt)}")

    def _save_index(snapshots_root: str, idx: dict, simulate: bool):
        nonlocal ret_index_write_ms
        if simulate:
            print(f"[dry] save index: items={len(idx.get('items', {}))}")
        else:
            t0 = time.time()
            save_content_index(cfg, snapshots_root, idx, dry=False)
            dt = (time.time() - t0) * 1000.0
            ret_index_write_ms += dt
            if os.environ.get("PCLOUD_TIMING") == "1":
                print(f"[timing] retention_index_write_ms={int(dt)}")

    def _rewrite_stub(snapshots_root: str, snapshot: str, relpath: str, sha: str, new_anchor_path: str, fileid) -> None:
        """
        Stub-JSON effizient neu schreiben:
          - Parent-Folder per folderid (stat_folderid_fast/ensure_path)
          - Schreiben via write_json_to_folderid(..., minify=True)
          - Vorhandenes Stub-JSON (falls vorhanden) übernehmen/aktualisieren
        """
        # relpath in (Unter)ordner + Basisdatei splitten
        if "/" in relpath:
            stub_dir, base = relpath.rsplit("/", 1)
        else:
            stub_dir, base = "", relpath

        parent_dir = f"{snapshots_root.rstrip('/')}/{snapshot}"
        if stub_dir:
            parent_dir = f"{parent_dir}/{stub_dir}"
        filename = f"{base}.meta.json"
        meta_path = f"{parent_dir}/{filename}"

        if dry:
            print(f"[dry] rewrite stub: {meta_path} -> anchor={new_anchor_path}")
            return

        # 1) Parent-FolderID besorgen (ohne per-File ensure)
        fid = pc.stat_folderid_fast(cfg, parent_dir)
        if not fid:
            fid = pc.ensure_path(cfg, parent_dir)
        fid = int(fid)

        # 2) Vorhandenes Stub-JSON (best effort) laden
        try:
            old_txt = pc.get_textfile(cfg, path=meta_path)
            payload = _json.loads(old_txt)
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}

        # 3) Pflichtfelder setzen/aktualisieren
        payload.setdefault("type", "hardlink")
        payload["sha256"] = sha
        payload["relpath"] = relpath
        payload["snapshot"] = snapshot
        payload["anchor_path"] = new_anchor_path
        payload["fileid"] = fileid if fileid is not None else None

        # 4) Schreiben per folderid (minified)
        t0 = time.time()
        pc.write_json_to_folderid(cfg, folderid=fid, filename=filename, obj=payload, minify=True)
        dt = (time.time() - t0) * 1000.0
        ret_stub_ms += dt
        ret_stub_writes += 1

        if os.environ.get("PCLOUD_TIMING") == "1":
            print(f"[timing] retention_stub_write_ms={int(dt)} file={meta_path}")

    def _delete_file_if_exists(path: str) -> None:
        "Best-effort: löscht Datei (z. B. stale Stub) am Pfad, wenn vorhanden."
        if dry:
            print(f"[dry] delete-if-exists: {path}")
            return
        try:
            fid = _stat_fileid_safe(path)
            if fid:
                pc.delete_file(cfg, fileid=int(fid))
        except Exception:
            pass

    # --- Setup & Daten holen -------------------------------------------------

    ensure_snapshots_layout(cfg, dest_root, dry=dry)
    snapshots_root = f"{pc._norm_remote_path(dest_root).rstrip('/')}/_snapshots"

    remote_snaps = set(_list_remote_snapshots(snapshots_root))
    local_snaps = set(local_snaps or [])
    to_delete = sorted(s for s in remote_snaps if s not in local_snaps)
    keep_snaps = remote_snaps & local_snaps

    if not to_delete:
        if dry:
            print("[dry] retention: nichts zu löschen")
        return

    idx = _load_index(snapshots_root)
    items = idx.setdefault("items", {})
    ret_index_changed = False

    ret_stub_ms = 0.0
    ret_index_write_ms = 0.0
    ret_stub_writes = 0

    promoted = 0
    removed_nodes = 0
    
    # === NEU: Globaler Blocker-Flag ===
    any_blockers = False
    # === ENDE NEU ===

    # --- Hauptlogik pro zu löschendem Snapshot ------------------------------

    for sdel in to_delete:
        del_prefix = f"{snapshots_root}/{sdel}/"
        snapshot_blockers = False  # Pro-Snapshot Blocker-Flag

        for sha, node in list(items.items()):
            if not isinstance(node, dict):
                continue

            holders = list(node.get("holders") or [])
            anchor = node.get("anchor_path") or ""
            anchor_in_deleted = anchor.startswith(del_prefix)

            # (A) Node ohne Holder, Anchor im gelöschten Snapshot -> Node weg
            if not holders and anchor_in_deleted:
                if dry:
                    print(f"[dry] drop node (no holders, anchor in {sdel}): {sha[:8]}…")
                else:
                    del items[sha]
                    removed_nodes += 1
                    ret_index_changed = True
                continue

            # (B) Holder splitten in keep/drop und im Node setzen
            keep_holders = [h for h in holders if h.get("snapshot") in keep_snaps]
            drop_holders = [h for h in holders if h.get("snapshot") in to_delete]
            if drop_holders or anchor_in_deleted:
                node["holders"] = keep_holders
                ret_index_changed = True

            # keine Keeper?
            if not keep_holders:
                if anchor_in_deleted:
                    if dry:
                        print(f"[dry] drop node (no keepers, anchor in {sdel}): {sha[:8]}…")
                    else:
                        del items[sha]
                        removed_nodes += 1
                continue

            # (C) Anchor liegt im gelöschten Snapshot -> Promotion (MOVE)
            if anchor_in_deleted:
                new_holder = max(keep_holders, key=lambda h: h.get("snapshot") or "")
                new_path = f"{snapshots_root}/{new_holder['snapshot']}/{new_holder['relpath']}"

                # No-Op-Guard
                if anchor == new_path:
                    node["anchor_path"] = new_path
                    # am Anchor KEIN Stub: ggf. stale Stub löschen
                    _delete_file_if_exists(f"{new_path}.meta.json")
                    # optional: Stubs der übrigen Holder neu schreiben
                    if rewrite_stubs:
                        for h in keep_holders:
                            if h is new_holder or (h["snapshot"] == new_holder["snapshot"] and h["relpath"] == new_holder["relpath"]):
                                _delete_file_if_exists(f"{snapshots_root}/{h['snapshot']}/{h['relpath']}.meta.json")
                                continue
                            _rewrite_stub(snapshots_root, h["snapshot"], h["relpath"], sha, node["anchor_path"], node.get("fileid"))
                    continue

                if dry:
                    print(f"[dry] promote (move) {sha[:8]}… {anchor} -> {new_path}")
                    node["anchor_path"] = new_path
                    promoted += 1
                else:
                    fid = node.get("fileid") or _stat_fileid_safe(anchor)
                    if not fid:
                        print(f"[warn] retention: fehlende fileid für Anchor {anchor}; Snapshot {sdel} wird NICHT gelöscht.", file=sys.stderr)
                        snapshot_blockers = True
                        any_blockers = True  # === NEU ===
                        continue

                    pc.ensure_parent_dirs(cfg, new_path)
                    # am Ziel darf kein Stub bleiben
                    _delete_file_if_exists(f"{new_path}.meta.json")

                    try:
                        pc.move(cfg, from_fileid=int(fid), to_path=new_path)
                    except Exception as e:
                        print(f"[warn] retention: move failed for fileid={fid} -> {new_path}: {e}", file=sys.stderr)
                        snapshot_blockers = True
                        any_blockers = True  # === NEU ===
                        continue

                    node["anchor_path"] = new_path
                    node["fileid"] = int(fid)
                    promoted += 1
                    ret_index_changed = True

                # übrige Holder: Stubs neu schreiben (Ziel-Holder auslassen)
                if rewrite_stubs:
                    for h in keep_holders:
                        if h is new_holder or (h["snapshot"] == new_holder["snapshot"] and h["relpath"] == new_holder["relpath"]):
                            _delete_file_if_exists(f"{snapshots_root}/{h['snapshot']}/{h['relpath']}.meta.json")
                            continue
                        _rewrite_stub(snapshots_root, h["snapshot"], h["relpath"], sha, node["anchor_path"], node.get("fileid"))

        # Snapshot nur löschen, wenn keine Blocker auftraten
        rmpath = f"{snapshots_root}/{sdel}"
        if snapshot_blockers:
            print(f"[warn] retention: Snapshot {sdel} bleibt bestehen (Blocker vorhanden).")
            continue

        if dry:
            print(f"[dry] delete snapshot dir: {rmpath}")
        else:
            pc.delete_folder(cfg, path=rmpath, recursive=True)

    # === NEU: Index nur schreiben wenn KEINE Blocker ===
    if any_blockers:
        print(f"[warn] retention: Index NICHT geschrieben wegen Blocker(n) in einem oder mehreren Snapshots")
    else:
        if ret_index_changed:
            _save_index(snapshots_root, idx, simulate=dry)
        else:
            print("[retention] no index changes")
    # === ENDE NEU ===

    if os.environ.get("PCLOUD_TIMING") == "1":
        print(f"[timing] retention: stubs_ms={int(ret_stub_ms)} index_ms={int(ret_index_write_ms)} stubs_written={ret_stub_writes}")

    msg = f"[retention] promoted={promoted} removed_nodes={removed_nodes}"
    print(msg if not dry else "[dry] " + msg[1:])

# ----------------- CLI -----------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Pusht ein JSON-Manifest nach pCloud (Object-Store- oder 1:1-Snapshot-Modus).")
    ap.add_argument("--manifest", required=True, help="Pfad zur Manifest-JSON (schema=2)")
    ap.add_argument("--dest-root", required=True, help="Remote-Wurzel, z.B. /Backup/pcloud-snapshots")
    ap.add_argument("--snapshot-mode", choices=["objects","1to1"], default="objects",
                    help="Upload-Strategie: objects (Hash-Object-Store + Stubs) oder 1to1 (Materialisieren + Stubs)")
    ap.add_argument("--objects-layout", choices=["two-level"], default="two-level",
                    help="Layout für Object-Store (aktuell nur two-level).")
    ap.add_argument("--retention-sync", action="store_true",
                    help="Vor dem Upload: entfernte Snapshots, die lokal fehlen, sauber promoten/löschen (nur relevant für --snapshot-mode 1to1).")
    ap.add_argument("--dry-run", action="store_true")


    # pCloud Config
    ap.add_argument("--env-file")
    ap.add_argument("--profile")
    ap.add_argument("--env-dir")
    ap.add_argument("--host")
    ap.add_argument("--port", type=int)
    ap.add_argument("--timeout", type=int)
    ap.add_argument("--device")
    ap.add_argument("--token")

    args = ap.parse_args()

    # Config
    cfg = pc.effective_config(
        env_file=args.env_file,
        overrides={"host": args.host, "port": args.port, "timeout": args.timeout,
                   "device": args.device, "token": args.token},
        profile=args.profile,
        env_dir=args.env_dir
    )

    # --- Neu: Plausibilisierung & Preflight ---
    # Zielpfad normieren (führt führenden "/")
    args.dest_root = pc._norm_remote_path(args.dest_root)
    # ENV-File rein informativ: effective_config hat bereits geprüft, ob Token existiert
    try:
        pc.preflight_or_raise(cfg)   # → raise bei Auth/Quota/API down
    except Exception as e:
        print(f"[preflight][FAIL] {e}", file=sys.stderr)
        sys.exit(12)
    # --- Ende Neu ---

    # Manifest lesen
    with open(args.manifest, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    if int(manifest.get("schema", 0)) < 2:
        print("Manifest schema>=2 erwartet (mit inode/ext/sha256).", file=sys.stderr)
        sys.exit(2)

    dest_root = pc._norm_remote_path(args.dest_root)

    # Optional: Retention-Sync (nur sinnvoll im 1:1-Modus)
    if args.retention_sync and args.snapshot_mode == "1to1":
        local_snaps = list_local_snapshot_names(manifest["root"])
        retention_sync_1to1(cfg, dest_root, local_snaps=local_snaps, dry=bool(args.dry_run))

    if args.snapshot_mode == "objects":
        push_objects_mode(cfg, manifest, dest_root, dry=bool(args.dry_run), objects_layout=args.objects_layout)
    else:
        push_1to1_mode(cfg, manifest, dest_root, dry=bool(args.dry_run))

if __name__ == "__main__":
    main()
