#!/usr/bin/env python3
"""Reorganize existing TelDrive files into media-type sub-folders.

Reads the same telegram2teldrive.toml config and, for each folder_nameN,
moves files that are directly inside the folder (or inside channel_*
sub-folders) into the correct media-type sub-folder:

    /folder_name/audio   – audio files
    /folder_name/video   – video files
    /folder_name/img     – image files
    /folder_name/ebook   – PDF, text, ebook files
    /folder_name/file    – archives and everything else

This avoids having to re-scan Telegram channels.
"""

import argparse
import logging
import mimetypes
import os
import re
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("reorganize_folders")


# ── helpers (shared logic with telegram2teldrive.py) ──────────────────────

def load_toml_config(path):
    with open(path, "rb") as f:
        return tomllib.load(f)


def get_cfg(config, section, key, default=None):
    return config.get(section, {}).get(key, default)


def get_category(file_name, mime_type=None):
    if mime_type is None:
        mime_type, _ = mimetypes.guess_type(file_name)
    mime_type = (mime_type or "").lower()
    if mime_type.startswith("image/"):
        return "image"
    if mime_type.startswith("video/"):
        return "video"
    if mime_type.startswith("audio/"):
        return "audio"
    if mime_type.startswith("text/") or mime_type == "application/pdf":
        return "document"
    ext = os.path.splitext(file_name)[1].lower()
    if mime_type == "application/epub+zip" or ext in {".epub", ".mobi", ".azw", ".azw3", ".djvu", ".fb2"}:
        return "document"
    if mime_type in {"application/zip", "application/x-tar", "application/x-bzip2", "application/x-7z-compressed"}:
        return "archive"
    return "other"


def get_subfolder_name(category):
    mapping = {
        "audio": "audio",
        "video": "video",
        "image": "img",
        "document": "ebook",
        "archive": "file",
        "other": "file",
    }
    return mapping.get(category, "file")


def parse_rule_string(rule_str):
    if not rule_str:
        return None
    rules = {}
    for part in rule_str.split(","):
        if ":" in part:
            ext, sf = part.split(":", 1)
            rules[ext.strip().lower().lstrip(".")] = sf.strip()
    return rules


# ── DB helpers ────────────────────────────────────────────────────────────

def db_connect(dsn):
    conn = psycopg2.connect(dsn, options="-c search_path=teldrive,public")
    conn.autocommit = False
    return conn


def fetch_one(conn, query, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()


def fetch_all(conn, query, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def execute(conn, query, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params)


def get_or_create_folder(conn, user_id, parent_id, folder_name, dry_run=False):
    if dry_run:
        return f"dryrun:{folder_name}"

    row = fetch_one(
        conn,
        """
        SELECT id
        FROM teldrive.files
        WHERE user_id = %s AND type = 'folder' AND name = %s AND parent_id IS NOT DISTINCT FROM %s
        LIMIT 1
        """,
        (user_id, folder_name, parent_id),
    )
    if row:
        return row[0]

    execute(
        conn,
        """
        INSERT INTO teldrive.files (name, type, mime_type, user_id, status, parent_id, encrypted)
        VALUES (%s, 'folder', 'drive/folder', %s, 'active', %s, false)
        ON CONFLICT DO NOTHING
        """,
        (folder_name, user_id, parent_id),
    )
    conn.commit()

    row = fetch_one(
        conn,
        """
        SELECT id
        FROM teldrive.files
        WHERE user_id = %s AND type = 'folder' AND name = %s AND parent_id IS NOT DISTINCT FROM %s
        LIMIT 1
        """,
        (user_id, folder_name, parent_id),
    )
    if not row:
        raise RuntimeError(f"Cannot create/find folder: {folder_name}")
    return row[0]


def resolve_folder_path(conn, user_id, root_id, folder_path, create=False, dry_run=False):
    """Walk a '/'-separated folder path starting from *root_id*.

    For example, '1/channel_1445612186_x' resolves:
        root -> '1' -> 'channel_1445612186_x'

    Returns the folder ID of the final segment, or None if not found
    (and *create* is False).
    """
    parts = [p.strip() for p in folder_path.replace("\\", "/").split("/") if p.strip()]
    if not parts:
        return root_id

    current_id = root_id
    for part in parts:
        if create:
            current_id = get_or_create_folder(conn, user_id, current_id, part, dry_run)
        else:
            row = fetch_one(
                conn,
                """
                SELECT id FROM teldrive.files
                WHERE user_id = %s AND type = 'folder' AND name = %s
                      AND parent_id IS NOT DISTINCT FROM %s AND status = 'active'
                LIMIT 1
                """,
                (user_id, part, current_id),
            )
            if not row:
                return None
            current_id = row[0]
    return current_id


# ── core logic ────────────────────────────────────────────────────────────

def collect_all_files(conn, user_id, folder_id):
    """Return all files (type='file') directly inside *folder_id*."""
    return fetch_all(
        conn,
        """
        SELECT id, name, mime_type
        FROM teldrive.files
        WHERE user_id = %s AND parent_id = %s AND type = 'file' AND status = 'active'
        """,
        (user_id, folder_id),
    )


def collect_sub_folder_ids(conn, user_id, parent_id):
    """Return list of (id, name) for sub-folders under *parent_id*."""
    return fetch_all(
        conn,
        """
        SELECT id, name
        FROM teldrive.files
        WHERE user_id = %s AND parent_id = %s AND type = 'folder' AND status = 'active'
        """,
        (user_id, parent_id),
    )


MEDIA_SUBFOLDER_NAMES = {"audio", "video", "img", "ebook", "file"}


def reorganize_folder(conn, user_id, base_folder_id, base_folder_name, dry_run=False, rule_dict=None):
    """Move files inside *base_folder_id* (and its channel_* sub-folders)
    into media-type sub-folders directly under *base_folder_id*."""

    # 1. Collect files directly in the base folder
    direct_files = collect_all_files(conn, user_id, base_folder_id)

    # 2. Collect files from channel_* sub-folders (and any non-media sub-folder)
    sub_folders = collect_sub_folder_ids(conn, user_id, base_folder_id)
    channel_files = []  # list of (file_id, file_name, mime_type, source_folder_name)
    empty_source_folders = []
    
    # If using custom rules, our valid subfolders include parsed rule target folders
    valid_subfolders = MEDIA_SUBFOLDER_NAMES.copy()
    if rule_dict:
        valid_subfolders.update(set(rule_dict.values()))
        
    for sf_id, sf_name in sub_folders:
        if sf_name in valid_subfolders:
            continue  # skip already-organized sub-folders
        files = collect_all_files(conn, user_id, sf_id)
        for f in files:
            channel_files.append((*f, sf_name))
        if not files:
            logger.info("  Sub-folder '%s' has no files, skipping", sf_name)
        else:
            empty_source_folders.append((sf_id, sf_name, len(files)))

    all_files = [(fid, fname, mime, None) for fid, fname, mime in direct_files] + channel_files
    if not all_files:
        logger.info("Folder '%s': no files to reorganize", base_folder_name)
        return 0

    logger.info("Folder '%s': found %s files to reorganize (%s direct, %s from sub-folders)",
                base_folder_name, len(all_files), len(direct_files), len(channel_files))

    # 3. Group files by target sub-folder for bulk updates
    #    {sf_name: [file_id, ...]}
    subfolder_groups = {}
    for file_id, file_name, mime_type, source in all_files:
        if rule_dict:
            ext = os.path.splitext(file_name)[1].lower().lstrip(".")
            if ext in rule_dict:
                sf_name = rule_dict[ext]
            else:
                sf_name = "."  # indicator for base_folder_id
        else:
            category = get_category(file_name, mime_type)
            sf_name = get_subfolder_name(category)
            
        subfolder_groups.setdefault(sf_name, []).append((file_id, file_name))

    moved = 0
    for sf_name, file_list in subfolder_groups.items():
        if dry_run:
            for _, fname in file_list:
                target_path = base_folder_name if sf_name == "." else f"{base_folder_name}/{sf_name}"
                logger.info("  [DRY-RUN] Would move '%s' -> /%s/", fname, target_path)
            moved += len(file_list)
            continue

        if sf_name == ".":
            target_parent = base_folder_id
        else:
            target_parent = get_or_create_folder(conn, user_id, base_folder_id, sf_name, dry_run=False)
            
        file_ids = [fid for fid, _ in file_list]

        # Bulk update in batches of 500
        batch_size = 500
        for i in range(0, len(file_ids), batch_size):
            batch = file_ids[i:i + batch_size]
            execute(
                conn,
                "UPDATE teldrive.files SET parent_id = %s, updated_at = NOW() WHERE id = ANY(%s::uuid[])",
                (target_parent, batch),
            )
            conn.commit()
            logger.info("  /%s/%s/: batch %s-%s of %s committed",
                        base_folder_name, sf_name, i + 1, min(i + batch_size, len(file_ids)), len(file_ids))

        moved += len(file_ids)
        logger.info("  /%s/%s/: %s files moved", base_folder_name, sf_name, len(file_ids))

    logger.info("Folder '%s': moved %s files into media sub-folders", base_folder_name, moved)

    # 4. Summary of old source folders that can now be cleaned up
    for sf_id, sf_name, count in empty_source_folders:
        remaining = fetch_one(
            conn,
            "SELECT COUNT(*) FROM teldrive.files WHERE parent_id = %s AND status = 'active'",
            (sf_id,),
        )
        remaining_count = remaining[0] if remaining else 0
        if remaining_count == 0 and not dry_run:
            logger.info("  Removing now-empty sub-folder '%s'", sf_name)
            execute(conn, "UPDATE teldrive.files SET status = 'pending_deletion' WHERE id = %s", (sf_id,))
            conn.commit()
        elif remaining_count == 0:
            logger.info("  [DRY-RUN] Would remove now-empty sub-folder '%s'", sf_name)

    return moved


# ── main ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Reorganize existing TelDrive files into media-type sub-folders"
    )
    parser.add_argument("--config", default=None, help="Path to telegram2teldrive.toml")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without changing the DB")
    args = parser.parse_args()

    # Load config
    config = {}
    config_candidates = [Path(args.config)] if args.config else [Path("telegram2teldrive.toml")]
    for config_path in config_candidates:
        if config_path.is_file():
            logger.info("Loading config: %s", config_path)
            config = load_toml_config(config_path)
            break

    dsn = get_cfg(config, "db", "data-source") or get_cfg(config, "database", "data-source")
    if not dsn:
        parser.error("No database connection string found in config")

    # Detect folder names to reorganize
    teldrive_cfg = config.get("teldrive", {})
    folder_names = []

    # Check for numbered pairs
    seen_indices = set()
    for key in teldrive_cfg:
        m = re.match(r"^folder_name(\d+)$", key)
        if m:
            seen_indices.add(m.group(1))
    for idx in sorted(seen_indices, key=int):
        fn = teldrive_cfg.get(f"folder_name{idx}", "")
        tach = teldrive_cfg.get(f"tach{idx}", False)
        rule_str = teldrive_cfg.get(f"rule{idx}", "")
        is_tach = str(tach).strip().lower() == "true" if isinstance(tach, str) else bool(tach)
        if fn:
            folder_names.append((fn, is_tach, rule_str))

    # Fallback to single folder_name
    if not folder_names:
        fn = teldrive_cfg.get("folder_name", "Imported")
        tach_global = teldrive_cfg.get("tach", False)
        rule_global = teldrive_cfg.get("rule", "")
        is_tach_global = str(tach_global).strip().lower() == "true" if isinstance(tach_global, str) else bool(tach_global)
        folder_names.append((fn, is_tach_global, rule_global))

    logger.info("Folders to reorganize: %s", folder_names)

    conn = db_connect(dsn)
    try:
        # Get user_id from the first file owner (single-user assumption)
        row = fetch_one(conn, "SELECT DISTINCT user_id FROM teldrive.files WHERE type = 'folder' AND name = 'root' LIMIT 1")
        if not row:
            logger.error("No root folder found — is the database populated?")
            return
        user_id = row[0]
        logger.info("User ID: %s", user_id)

        # Find root folder
        root_row = fetch_one(
            conn,
            "SELECT id FROM teldrive.files WHERE user_id = %s AND name = 'root' AND type = 'folder' AND parent_id IS NULL LIMIT 1",
            (user_id,),
        )
        if not root_row:
            logger.error("Root folder not found")
            return
        root_id = root_row[0]

        total_moved = 0
        for folder_name, split_media, rule_str in folder_names:
            if not split_media:
                logger.info("Folder '%s': tach=false (or missing), skipping reorganize", folder_name)
                continue

            base_id = resolve_folder_path(conn, user_id, root_id, folder_name)
            if not base_id:
                logger.warning("Folder '%s' not found, skipping", folder_name)
                continue

            moved = reorganize_folder(conn, user_id, base_id, folder_name, args.dry_run, rule_dict=parse_rule_string(rule_str))
            total_moved += moved

        logger.info("Done! Total files moved: %s", total_moved)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
