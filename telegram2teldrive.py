#!/usr/bin/env python3
"""Import existing Telegram channel documents into Teldrive database.

This script is adapted for the current TelDrive schema:
- Uses PostgreSQL schema `teldrive` (via search_path).
- Writes to teldrive.files / teldrive.channels compatible with current columns.
- Creates folder tree: /root/<folder_name>/channel_<id>_<name>
"""

import argparse
import json
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
from telethon.tl.types import DocumentAttributeFilename, InputMessagesFilterDocument

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
    args.channels = args.channels or os.getenv("CHANNELS") or get_cfg(config, "telegram", "channels", "")
    args.filters = args.filters or os.getenv("FILTERS") or "all"
    args.session = args.session or os.getenv("SESSION_NAME") or get_cfg(config, "telegram", "session", "telegram2teldrive")

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
    if mime_type in {"application/zip", "application/x-tar", "application/x-bzip2", "application/x-7z-compressed"}:
        return "archive"
    return "other"


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

    if dry_run:
        return f"dryrun:{folder_name}"

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
    if not message.file:
        return None
    if message.file.name:
        return message.file.name

    attrs = getattr(message.document, "attributes", []) if getattr(message, "document", None) else []
    for attr in attrs:
        if isinstance(attr, DocumentAttributeFilename):
            return attr.file_name

    ext = mimetypes.guess_extension(message.file.mime_type or "") or ""
    return f"telegram_{message.id}{ext}"


def insert_file(conn, user_id, channel_id, parent_id, message, file_name, dry_run=False):
    if dry_run:
        return True

    file_size = getattr(message.file, "size", None)
    mime_type = getattr(message.file, "mime_type", None) or "application/octet-stream"
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
        base_id = get_or_create_folder(conn, user_id, root_id, args.folder_name, args.dry_run)

        if args.channels and args.channels.lower() != "all":
            channel_ids = [int(x.strip()) for x in args.channels.split(",") if x.strip()]
        else:
            channel_ids = await select_channels_interactive(client)
            if not channel_ids:
                logger.info("No channels selected, fallback to all dialogs")
                channel_ids = [d.entity.id async for d in client.iter_dialogs() if d.is_channel]

        logger.info("Selected channels: %s", channel_ids)

        total_imported = 0
        total_skipped = 0

        for channel_id in channel_ids:
            channel = await client.get_entity(channel_id)
            channel_name = getattr(channel, "title", str(channel_id))
            logger.info("Scanning channel %s (%s)", channel_name, channel_id)

            ensure_channel(conn, channel_id, channel_name, user_id, args.dry_run)
            channel_folder = get_or_create_folder(
                conn,
                user_id,
                base_id,
                f"channel_{channel_id}_{channel_name}"[:240],
                args.dry_run,
            )

            imported = 0
            skipped = 0
            processed = 0

            async for message in client.iter_messages(channel_id, filter=InputMessagesFilterDocument):
                processed += 1
                if processed % 100 == 0:
                    logger.info("Channel %s progress: %s documents", channel_id, processed)

                if not message.file:
                    skipped += 1
                    continue

                file_name = extract_file_name(message)
                if not file_name:
                    skipped += 1
                    continue

                category = get_category(file_name, message.file.mime_type)
                if filters is not None and category not in filters:
                    skipped += 1
                    continue

                if file_exists(conn, user_id, channel_id, message.id):
                    skipped += 1
                    continue

                insert_file(conn, user_id, channel_id, channel_folder, message, file_name, args.dry_run)
                imported += 1

            conn.commit()
            total_imported += imported
            total_skipped += skipped
            logger.info("Channel %s done: imported=%s skipped=%s", channel_id, imported, skipped)

        logger.info("Finished: imported=%s skipped=%s", total_imported, total_skipped)

    finally:
        conn.close()
        await client.disconnect()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
