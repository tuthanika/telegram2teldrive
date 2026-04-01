#!/usr/bin/env python3
"""Import existing Telegram channel documents into Teldrive database.

This script is adapted for the current TelDrive schema:
- Uses PostgreSQL schema `teldrive` (via search_path).
- Writes to teldrive.files / teldrive.channels compatible with current columns.
- Default mode: /root/<folder_name>/channel_<id>_<name>
- With explicit channels: /root/<folder_name>/<media_type>
"""

import argparse
import json
import re
import logging
import mimetypes
import os
from datetime import datetime, timezone
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib

import psycopg2
from telethon import TelegramClient
from telethon.tl.types import DocumentAttributeFilename

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("telegram2teldrive")


def load_toml_config(path):
    with open(path, "rb") as f:
        return tomllib.load(f)


def get_cfg(config, section, key, default=None):
    return config.get(section, {}).get(key, default)


def parse_args():
    parser = argparse.ArgumentParser(description="Import existing Telegram files into TelDrive DB")
    parser.add_argument("--config", default=os.getenv("CONFIG_FILE"), help="Path to telegram2teldrive.toml")
    parser.add_argument("--api-id")
    parser.add_argument("--api-hash")
    parser.add_argument("--phone-number")
    parser.add_argument("--db-host")
    parser.add_argument("--db-port")
    parser.add_argument("--db-data-source", help="Postgres DSN, ex: postgres://user:pass@host:port/db")
    parser.add_argument("--db-name")
    parser.add_argument("--db-user")
    parser.add_argument("--db-password")
    parser.add_argument("--folder-name")
    parser.add_argument("--channels", help="all | comma-separated channel ids")
    parser.add_argument("--filters", help="all | comma-separated categories: document,image,video,audio,archive,other")
    parser.add_argument("--session", help="Telethon session name/path (default: telegram2teldrive)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config = {}
    config_candidates = []
    if args.config:
        config_candidates.append(Path(args.config))
    else:
        config_candidates.extend([Path("telegram2teldrive.toml"), Path("telegram2teldrive.conf")])

    for config_path in config_candidates:
        if config_path.is_file():
            logger.info("Loading config file: %s", config_path)
            config = load_toml_config(config_path)
            break

    args.api_id = args.api_id or os.getenv("API_ID") or get_cfg(config, "telegram", "api-id")
    args.api_hash = args.api_hash or os.getenv("API_HASH") or get_cfg(config, "telegram", "api-hash")
    args.phone_number = args.phone_number or os.getenv("PHONE_NUMBER") or get_cfg(config, "telegram", "phone-number")

    args.db_data_source = (
        args.db_data_source
        or os.getenv("DB_DATA_SOURCE")
        or get_cfg(config, "db", "data-source")
        or get_cfg(config, "database", "data-source")
    )
    args.db_host = args.db_host or os.getenv("DB_HOST") or get_cfg(config, "database", "host", "localhost")
    args.db_port = args.db_port or os.getenv("DB_PORT") or str(get_cfg(config, "database", "port", "5432"))
    args.db_name = args.db_name or os.getenv("DB_NAME") or get_cfg(config, "database", "name")
    args.db_user = args.db_user or os.getenv("DB_USER") or get_cfg(config, "database", "user")
    args.db_password = args.db_password or os.getenv("DB_PASSWORD") or get_cfg(config, "database", "password")

    args.folder_name = args.folder_name or os.getenv("FOLDER_NAME") or get_cfg(config, "teldrive", "folder_name", "Imported")
    args.channels = args.channels or os.getenv("CHANNELS") or get_cfg(config, "teldrive", "channels", "") or get_cfg(config, "telegram", "channels", "")
    args.filters = args.filters or os.getenv("FILTERS") or "all"
    args.session = args.session or os.getenv("SESSION_NAME") or get_cfg(config, "telegram", "session", "telegram2teldrive")

    # ── Detect numbered folder/channel pairs (folder_name1/channels1, …) ──
    teldrive_cfg = config.get("teldrive", {})
    numbered_pairs = []
    seen_indices = set()
    for key in teldrive_cfg:
        m = re.match(r"^folder_name(\d+)$", key)
        if m:
            seen_indices.add(m.group(1))
    for idx in sorted(seen_indices, key=int):
        fn = teldrive_cfg.get(f"folder_name{idx}", "")
        ch = teldrive_cfg.get(f"channels{idx}", "")
        tach = teldrive_cfg.get(f"tach{idx}", False)
        is_tach = str(tach).strip().lower() == "true" if isinstance(tach, str) else bool(tach)
        if fn:
            numbered_pairs.append((fn, ch if ch else None, is_tach))

    if numbered_pairs:
        args.folder_channel_pairs = numbered_pairs
    else:
        # Single-pair mode: use the classic folder_name + optional channels
        ch = args.channels.strip() if args.channels else ""
        tach_global = teldrive_cfg.get("tach", False)
        is_tach_global = str(tach_global).strip().lower() == "true" if isinstance(tach_global, str) else bool(tach_global)
        args.folder_channel_pairs = [(args.folder_name, ch if ch else None, is_tach_global)]

    try:
        args.api_id = int(args.api_id)
    except (TypeError, ValueError):
        pass

    missing = []
    for req_key in ("api_id", "api_hash", "phone_number"):
        if not getattr(args, req_key):
            missing.append(req_key.replace("_", "-"))
    if not args.db_data_source:
        for req_key in ("db_name", "db_user", "db_password"):
            if not getattr(args, req_key):
                missing.append(req_key.replace("_", "-"))
    if missing:
        parser.error(f"missing required settings: {', '.join(missing)}")

    return args


def db_connect(args):
    if args.db_data_source:
        conn = psycopg2.connect(args.db_data_source, options="-c search_path=teldrive,public")
    else:
        conn = psycopg2.connect(
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=args.db_password,
            dbname=args.db_name,
            options="-c search_path=teldrive,public",
        )
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
    """Map file category to media-type sub-folder name."""
    mapping = {
        "audio": "audio",
        "video": "video",
        "image": "img",
        "document": "ebook",
        "archive": "file",
        "other": "file",
    }
    return mapping.get(category, "file")


def parse_filters(filters):
    items = {x.strip().lower() for x in filters.split(",") if x.strip()}
    if not items or "all" in items:
        return None
    allowed = {"document", "image", "video", "audio", "archive", "other"}
    invalid = items - allowed
    if invalid:
        raise ValueError(f"Invalid filters: {', '.join(sorted(invalid))}")
    return items


def ensure_root(conn, user_id):
    row = fetch_one(
        conn,
        "SELECT id FROM teldrive.files WHERE user_id = %s AND name = 'root' AND type = 'folder' AND parent_id IS NULL LIMIT 1",
        (user_id,),
    )
    if row:
        return row[0]

    execute(
        conn,
        """
        INSERT INTO teldrive.files (name, type, mime_type, user_id, status, parent_id, encrypted)
        VALUES ('root', 'folder', 'drive/folder', %s, 'active', NULL, false)
        """,
        (user_id,),
    )
    conn.commit()
    row = fetch_one(
        conn,
        "SELECT id FROM teldrive.files WHERE user_id = %s AND name = 'root' AND type = 'folder' AND parent_id IS NULL LIMIT 1",
        (user_id,),
    )
    return row[0]


def get_or_create_folder(conn, user_id, parent_id, folder_name, dry_run=False):
    # In dry-run mode, skip all DB calls to avoid passing fake IDs as UUIDs
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


def resolve_folder_path(conn, user_id, root_id, folder_path, dry_run=False):
    """Walk a '/'-separated folder path starting from *root_id*,
    creating each segment if it doesn't exist.

    '1/subfolder' resolves:  root -> '1' -> 'subfolder'
    '2'           resolves:  root -> '2'
    """
    parts = [p.strip() for p in folder_path.replace("\\", "/").split("/") if p.strip()]
    if not parts:
        return root_id
    current_id = root_id
    for part in parts:
        current_id = get_or_create_folder(conn, user_id, current_id, part, dry_run)
    return current_id


def file_exists(conn, user_id, channel_id, message_id):
    row = fetch_one(
        conn,
        """
        SELECT 1
        FROM teldrive.files
        WHERE user_id = %s
          AND channel_id = %s
          AND type = 'file'
          AND parts @> %s::jsonb
        LIMIT 1
        """,
        (user_id, channel_id, json.dumps([{"id": message_id}])),
    )
    return row is not None


def ensure_channel(conn, channel_id, channel_name, user_id, dry_run=False):
    if dry_run:
        return
    execute(
        conn,
        """
        INSERT INTO teldrive.channels (channel_id, channel_name, user_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (channel_id) DO NOTHING
        """,
        (channel_id, channel_name, user_id),
    )


def extract_file_name(message):
    if message.file and message.file.name:
        return message.file.name

    attrs = getattr(message.document, "attributes", []) if getattr(message, "document", None) else []
    for attr in attrs:
        if isinstance(attr, DocumentAttributeFilename):
            return attr.file_name

    mime_type = None
    if message.file:
        mime_type = message.file.mime_type
    elif getattr(message, "photo", None):
        mime_type = "image/jpeg"
    ext = mimetypes.guess_extension(mime_type or "") or ""
    return f"telegram_{message.id}{ext}"


def get_message_media_meta(message):
    """
    Return (size, mime_type) for Telegram media messages.
    Handles cases where message.file is missing but document/photo still exists.
    """
    if message.file:
        return getattr(message.file, "size", None), getattr(message.file, "mime_type", None)

    if getattr(message, "document", None):
        doc = message.document
        return getattr(doc, "size", None), getattr(doc, "mime_type", None)

    if getattr(message, "photo", None):
        photo = message.photo
        sizes = getattr(photo, "sizes", None) or []
        size = None
        for p in sizes:
            p_size = getattr(p, "size", None)
            if p_size is not None:
                size = p_size
        return size, "image/jpeg"

    return None, None


def insert_file(conn, user_id, channel_id, parent_id, message, file_name, dry_run=False):
    if dry_run:
        return True

    file_size, mime_type = get_message_media_meta(message)
    mime_type = mime_type or "application/octet-stream"
    category = get_category(file_name, mime_type)
    msg_time = message.date.astimezone(timezone.utc) if message.date else datetime.now(timezone.utc)

    execute(
        conn,
        """
        INSERT INTO teldrive.files (
            name, type, mime_type, size, user_id, status, channel_id,
            parts, encrypted, category, parent_id, created_at, updated_at
        ) VALUES (
            %s, 'file', %s, %s, %s, 'active', %s,
            %s::jsonb, false, %s, %s, %s, %s
        )
        ON CONFLICT DO NOTHING
        """,
        (
            file_name,
            mime_type,
            file_size,
            user_id,
            channel_id,
            json.dumps([{"id": message.id}]),
            category,
            parent_id,
            msg_time,
            datetime.now(timezone.utc),
        ),
    )
    return True


def parse_channel_selection(choice, max_index):
    selected = set()
    for part in choice.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            a, b = int(start), int(end)
            for i in range(a, b + 1):
                if 1 <= i <= max_index:
                    selected.add(i - 1)
        else:
            i = int(part)
            if 1 <= i <= max_index:
                selected.add(i - 1)
    return selected


async def select_channels_interactive(client):
    channels = []
    async for dialog in client.iter_dialogs():
        if dialog.is_channel:
            channels.append((dialog.entity.id, dialog.entity.title or str(dialog.entity.id)))

    channels.sort(key=lambda x: x[0])
    if not channels:
        return []

    print("\n0. all channels")
    for i, (cid, cname) in enumerate(channels, start=1):
        print(f"{i}. {cid} ({cname})")

    choice = input("\nEnter channel numbers (empty=all, ex: 1,3-5): ").strip().lower()
    if choice in {"", "0", "all", "a"}:
        return [c[0] for c in channels]

    idxs = parse_channel_selection(choice, len(channels))
    return [channels[i][0] for i in sorted(idxs)]


async def iter_all_messages(client, entity, batch_size=100):
    """
    Iterate the full history with explicit pagination.
    This avoids Telethon-version differences in iter_messages defaults and
    makes scan boundaries easier to reason about.
    """
    offset_id = 0
    while True:
        messages = await client.get_messages(entity, limit=batch_size, offset_id=offset_id)
        if not messages:
            break

        for message in messages:
            yield message

        last_id = messages[-1].id
        if not last_id or last_id == offset_id:
            break
        offset_id = last_id


async def process_channel(client, conn, user_id, channel_id, base_id, folder_mode, filters, dry_run):
    """Scan a single channel and import files.

    folder_mode can be:
      - 'media_subfolders': sort into /base_id/audio, /base_id/video, etc.
      - 'channel_subfolder': legacy /base_id/channel_<id>_<name>
      - 'direct': insert directly into /base_id
    """
    channel = await client.get_entity(channel_id)
    channel_name = getattr(channel, "title", str(channel_id))
    logger.info("Scanning channel %s (%s)", channel_name, channel_id)

    ensure_channel(conn, channel_id, channel_name, user_id, dry_run)

    # Pre-create / cache media-type sub-folder IDs when needed
    media_folder_cache = {}
    legacy_folder = None
    if folder_mode == "channel_subfolder":
        legacy_folder = get_or_create_folder(
            conn, user_id, base_id,
            f"channel_{channel_id}_{channel_name}"[:240], dry_run,
        )

    imported = 0
    skipped = 0
    processed = 0
    media_seen = 0

    async for message in iter_all_messages(client, channel, batch_size=200):
        processed += 1
        if processed % 100 == 0:
            logger.info("Channel %s progress: scanned=%s media=%s", channel_id, processed, media_seen)

        if not (message.file or getattr(message, "document", None) or getattr(message, "photo", None)):
            skipped += 1
            continue

        media_seen += 1
        file_name = extract_file_name(message)
        if not file_name:
            skipped += 1
            continue

        _, mime_type = get_message_media_meta(message)
        category = get_category(file_name, mime_type)
        if filters is not None and category not in filters:
            skipped += 1
            continue

        if file_exists(conn, user_id, channel_id, message.id):
            skipped += 1
            continue

        # Determine target folder
        if folder_mode == "media_subfolders":
            sf_name = get_subfolder_name(category)
            if sf_name not in media_folder_cache:
                media_folder_cache[sf_name] = get_or_create_folder(
                    conn, user_id, base_id, sf_name, dry_run,
                )
            target_folder = media_folder_cache[sf_name]
        elif folder_mode == "channel_subfolder":
            target_folder = legacy_folder
        else:
            target_folder = base_id

        insert_file(conn, user_id, channel_id, target_folder, message, file_name, dry_run)
        imported += 1

    conn.commit()
    logger.info(
        "Channel %s done: scanned_total=%s, media_total=%s, imported=%s, skipped=%s",
        channel_id, processed, media_seen, imported, skipped,
    )
    return imported, skipped


def parse_channel_ids(channels_str):
    """Parse a channels string that may use ',' or ';' as separator."""
    raw = re.split(r"[,;]", channels_str)
    ids = []
    for part in raw:
        part = part.strip()
        if part:
            ids.append(int(part))
    return ids


async def main():
    args = parse_args()
    filters = parse_filters(args.filters)

    conn = db_connect(args)
    client = TelegramClient(args.session, args.api_id, args.api_hash)
    await client.start(phone=args.phone_number)

    try:
        me = await client.get_me()
        user_id = me.id
        logger.info("User: %s (%s)", me.first_name, user_id)

        root_id = ensure_root(conn, user_id)

        total_imported = 0
        total_skipped = 0

        for pair_idx, (folder_name, channels_str, split_media) in enumerate(args.folder_channel_pairs, 1):
            logger.info("=== Pair %s: folder=%r channels=%r tach=%r ===", pair_idx, folder_name, channels_str or "(interactive)", split_media)
            base_id = resolve_folder_path(conn, user_id, root_id, folder_name, args.dry_run)

            if channels_str is not None:
                folder_mode = "media_subfolders" if split_media else "direct"
                if channels_str.lower() == "all":
                    channel_ids = [d.entity.id async for d in client.iter_dialogs() if d.is_channel]
                else:
                    channel_ids = parse_channel_ids(channels_str)
            else:
                # Interactive mode – no channels specified
                folder_mode = "channel_subfolder"
                channel_ids = await select_channels_interactive(client)
                if not channel_ids:
                    logger.info("No channels selected, fallback to all dialogs")
                    channel_ids = [d.entity.id async for d in client.iter_dialogs() if d.is_channel]

            logger.info("Selected channels: %s", channel_ids)

            for channel_id in channel_ids:
                imp, skp = await process_channel(
                    client, conn, user_id, channel_id, base_id,
                    folder_mode, filters, args.dry_run,
                )
                total_imported += imp
                total_skipped += skp

        logger.info("Finished: imported=%s skipped=%s", total_imported, total_skipped)

    finally:
        conn.close()
        await client.disconnect()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())