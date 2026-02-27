import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite


class Database:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self._write_lock = asyncio.Lock()

    async def init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS source_groups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS topics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_group_id INTEGER NOT NULL,
                    topic_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(source_group_id, topic_id),
                    FOREIGN KEY(source_group_id) REFERENCES source_groups(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    is_standby INTEGER NOT NULL DEFAULT 0,
                    in_use INTEGER NOT NULL DEFAULT 0,
                    consumed_at TEXT,
                    admin_check_at TEXT,
                    last_seen_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS topic_bindings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_group_id INTEGER NOT NULL,
                    topic_id INTEGER NOT NULL,
                    channel_chat_id INTEGER NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(source_group_id, topic_id),
                    FOREIGN KEY(source_group_id) REFERENCES source_groups(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS banned_channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_group_id INTEGER NOT NULL,
                    topic_id INTEGER NOT NULL,
                    channel_chat_id INTEGER NOT NULL,
                    reason TEXT,
                    detected_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS recovery_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_group_id INTEGER NOT NULL,
                    topic_id INTEGER NOT NULL,
                    old_channel_chat_id INTEGER NOT NULL,
                    new_channel_chat_id INTEGER,
                    reason TEXT,
                    status TEXT NOT NULL,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    last_cloned_message_id INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            await self._ensure_column(conn, "channels", "admin_check_at", "TEXT")
            await self._ensure_column(
                conn,
                "recovery_queue",
                "last_cloned_message_id",
                "INTEGER NOT NULL DEFAULT 0",
            )
            await conn.commit()

    async def _ensure_column(
        self,
        conn: aiosqlite.Connection,
        table: str,
        column: str,
        definition: str,
    ) -> None:
        cur = await conn.execute(f"PRAGMA table_info({table})")
        rows = await cur.fetchall()
        await cur.close()
        existing_columns = {row[1] for row in rows}
        if column not in existing_columns:
            await conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    async def _fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            cur = await conn.execute(sql, params)
            rows = await cur.fetchall()
            await cur.close()
            return [dict(row) for row in rows]

    async def _fetch_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        async with aiosqlite.connect(self.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            cur = await conn.execute(sql, params)
            row = await cur.fetchone()
            await cur.close()
            return dict(row) if row else None

    async def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        async with self._write_lock:
            async with aiosqlite.connect(self.db_path) as conn:
                cur = await conn.execute(sql, params)
                await conn.commit()
                lastrowid = cur.lastrowid
                await cur.close()
                return lastrowid

    async def _executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        async with self._write_lock:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.executemany(sql, rows)
                await conn.commit()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    async def set_setting(self, key: str, value: str) -> None:
        now = self._now()
        await self._execute(
            """
            INSERT INTO settings(key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            (key, value, now),
        )

    async def get_setting(self, key: str) -> str | None:
        row = await self._fetch_one("SELECT value FROM settings WHERE key=?", (key,))
        return row["value"] if row else None

    async def add_or_update_source_group(self, chat_id: int, title: str) -> dict[str, Any]:
        now = self._now()
        await self._execute(
            """
            INSERT INTO source_groups(chat_id, title, enabled, created_at, updated_at)
            VALUES (?, ?, 1, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title=excluded.title,
                updated_at=excluded.updated_at
            """,
            (chat_id, title, now, now),
        )
        return await self.get_source_group_by_chat_id(chat_id)

    async def get_source_group_by_chat_id(self, chat_id: int) -> dict[str, Any] | None:
        return await self._fetch_one(
            "SELECT * FROM source_groups WHERE chat_id=?",
            (chat_id,),
        )

    async def get_source_group_by_id(self, source_group_id: int) -> dict[str, Any] | None:
        return await self._fetch_one(
            "SELECT * FROM source_groups WHERE id=?",
            (source_group_id,),
        )

    async def list_source_groups(self) -> list[dict[str, Any]]:
        return await self._fetch_all(
            "SELECT * FROM source_groups ORDER BY id DESC"
        )

    async def set_source_group_enabled(self, source_group_id: int, enabled: bool) -> None:
        await self._execute(
            "UPDATE source_groups SET enabled=?, updated_at=? WHERE id=?",
            (1 if enabled else 0, self._now(), source_group_id),
        )

    async def upsert_topics(self, source_group_id: int, topics: list[dict[str, Any]]) -> None:
        now = self._now()
        rows = [
            (
                source_group_id,
                topic["topic_id"],
                topic["title"],
                now,
                now,
            )
            for topic in topics
        ]
        await self._executemany(
            """
            INSERT INTO topics(source_group_id, topic_id, title, enabled, created_at, updated_at)
            VALUES (?, ?, ?, 0, ?, ?)
            ON CONFLICT(source_group_id, topic_id) DO UPDATE SET
                title=excluded.title,
                updated_at=excluded.updated_at
            """,
            rows,
        )

    async def list_topics(self, source_group_id: int | None = None) -> list[dict[str, Any]]:
        if source_group_id is None:
            return await self._fetch_all(
                "SELECT * FROM topics ORDER BY source_group_id ASC, topic_id ASC"
            )
        return await self._fetch_all(
            "SELECT * FROM topics WHERE source_group_id=? ORDER BY topic_id ASC",
            (source_group_id,),
        )

    async def get_topic(self, source_group_id: int, topic_id: int) -> dict[str, Any] | None:
        return await self._fetch_one(
            "SELECT * FROM topics WHERE source_group_id=? AND topic_id=?",
            (source_group_id, topic_id),
        )

    async def set_topic_enabled(self, source_group_id: int, topic_id: int, enabled: bool) -> None:
        await self._execute(
            "UPDATE topics SET enabled=?, updated_at=? WHERE source_group_id=? AND topic_id=?",
            (1 if enabled else 0, self._now(), source_group_id, topic_id),
        )

    async def list_enabled_topics(self, source_group_id: int) -> list[dict[str, Any]]:
        return await self._fetch_all(
            "SELECT * FROM topics WHERE source_group_id=? AND enabled=1 ORDER BY topic_id ASC",
            (source_group_id,),
        )

    async def upsert_binding(self, source_group_id: int, topic_id: int, channel_chat_id: int) -> dict[str, Any]:
        now = self._now()
        await self._execute(
            """
            INSERT INTO topic_bindings(source_group_id, topic_id, channel_chat_id, active, created_at, updated_at)
            VALUES (?, ?, ?, 1, ?, ?)
            ON CONFLICT(source_group_id, topic_id) DO UPDATE SET
                channel_chat_id=excluded.channel_chat_id,
                active=1,
                updated_at=excluded.updated_at
            """,
            (source_group_id, topic_id, channel_chat_id, now, now),
        )
        await self._execute(
            "UPDATE channels SET in_use=1, is_standby=0, updated_at=? WHERE chat_id=?",
            (now, channel_chat_id),
        )
        row = await self.get_binding(source_group_id, topic_id)
        if row is None:
            raise RuntimeError("绑定保存失败")
        return row

    async def get_binding(self, source_group_id: int, topic_id: int) -> dict[str, Any] | None:
        return await self._fetch_one(
            "SELECT * FROM topic_bindings WHERE source_group_id=? AND topic_id=?",
            (source_group_id, topic_id),
        )

    async def list_bindings(self, source_group_id: int | None = None) -> list[dict[str, Any]]:
        sql = """
        SELECT b.*, t.title AS topic_title, s.title AS source_title
        FROM topic_bindings b
        LEFT JOIN topics t ON t.source_group_id=b.source_group_id AND t.topic_id=b.topic_id
        LEFT JOIN source_groups s ON s.id=b.source_group_id
        """
        if source_group_id is None:
            return await self._fetch_all(sql + " ORDER BY b.id DESC")
        return await self._fetch_all(sql + " WHERE b.source_group_id=? ORDER BY b.id DESC", (source_group_id,))

    async def list_active_bindings(self) -> list[dict[str, Any]]:
        return await self._fetch_all(
            """
            SELECT b.*, t.title AS topic_title, s.chat_id AS source_chat_id, s.enabled AS source_enabled, t.enabled AS topic_enabled
            FROM topic_bindings b
            JOIN topics t ON t.source_group_id=b.source_group_id AND t.topic_id=b.topic_id
            JOIN source_groups s ON s.id=b.source_group_id
            WHERE b.active=1
            """
        )

    async def set_binding_active(self, source_group_id: int, topic_id: int, active: bool) -> None:
        await self._execute(
            "UPDATE topic_bindings SET active=?, updated_at=? WHERE source_group_id=? AND topic_id=?",
            (1 if active else 0, self._now(), source_group_id, topic_id),
        )

    async def detach_channel_bindings(self, channel_chat_id: int) -> None:
        await self._execute(
            "UPDATE topic_bindings SET active=0, updated_at=? WHERE channel_chat_id=?",
            (self._now(), channel_chat_id),
        )

    async def upsert_channel(
        self,
        chat_id: int,
        title: str,
        is_standby: bool,
        in_use: bool = False,
        consumed: bool = False,
        admin_check_at: str | None = None,
    ) -> dict[str, Any]:
        now = self._now()
        consumed_at = now if consumed else None
        await self._execute(
            """
            INSERT INTO channels(
                chat_id, title, is_standby, in_use, consumed_at,
                admin_check_at, last_seen_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title=excluded.title,
                is_standby=excluded.is_standby,
                in_use=excluded.in_use,
                admin_check_at=COALESCE(excluded.admin_check_at, channels.admin_check_at),
                last_seen_at=excluded.last_seen_at,
                updated_at=excluded.updated_at
            """,
            (
                chat_id,
                title,
                1 if is_standby else 0,
                1 if in_use else 0,
                consumed_at,
                admin_check_at,
                now,
                now,
                now,
            ),
        )
        return await self.get_channel(chat_id)

    async def get_channel(self, chat_id: int) -> dict[str, Any] | None:
        return await self._fetch_one("SELECT * FROM channels WHERE chat_id=?", (chat_id,))

    async def list_channels(self) -> list[dict[str, Any]]:
        return await self._fetch_all("SELECT * FROM channels ORDER BY id DESC")

    async def list_standby_channels(self) -> list[dict[str, Any]]:
        return await self._fetch_all(
            "SELECT * FROM channels WHERE is_standby=1 AND in_use=0 ORDER BY id ASC"
        )

    async def consume_standby_channel(self, chat_id: int) -> None:
        now = self._now()
        await self._execute(
            "UPDATE channels SET is_standby=0, in_use=1, consumed_at=?, updated_at=? WHERE chat_id=?",
            (now, now, chat_id),
        )

    async def mark_channel_last_seen(self, chat_id: int, title: str | None = None) -> None:
        now = self._now()
        if title:
            await self._execute(
                "UPDATE channels SET title=?, last_seen_at=?, updated_at=? WHERE chat_id=?",
                (title, now, now, chat_id),
            )
        else:
            await self._execute(
                "UPDATE channels SET last_seen_at=?, updated_at=? WHERE chat_id=?",
                (now, now, chat_id),
            )

    async def add_banned_channel(
        self,
        source_group_id: int,
        topic_id: int,
        channel_chat_id: int,
        reason: str,
    ) -> None:
        await self._execute(
            """
            INSERT INTO banned_channels(source_group_id, topic_id, channel_chat_id, reason, detected_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (source_group_id, topic_id, channel_chat_id, reason, self._now()),
        )

    async def list_banned_channels(self) -> list[dict[str, Any]]:
        return await self._fetch_all(
            """
            SELECT b.*, s.title AS source_title, t.title AS topic_title
            FROM banned_channels b
            LEFT JOIN source_groups s ON s.id=b.source_group_id
            LEFT JOIN topics t ON t.source_group_id=b.source_group_id AND t.topic_id=b.topic_id
            ORDER BY b.id DESC
            LIMIT 300
            """
        )

    async def enqueue_recovery(
        self,
        source_group_id: int,
        topic_id: int,
        old_channel_chat_id: int,
        reason: str,
    ) -> int:
        existing = await self._fetch_one(
            """
            SELECT id FROM recovery_queue
            WHERE source_group_id=? AND topic_id=? AND status IN ('pending','running')
            ORDER BY id DESC
            LIMIT 1
            """,
            (source_group_id, topic_id),
        )
        if existing:
            return int(existing["id"])

        now = self._now()
        return await self._execute(
            """
            INSERT INTO recovery_queue(
                source_group_id, topic_id, old_channel_chat_id, reason,
                status, retry_count, last_cloned_message_id, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, 'pending', 0, 0, ?, ?)
            """,
            (source_group_id, topic_id, old_channel_chat_id, reason, now, now),
        )

    async def list_recovery_queue(self) -> list[dict[str, Any]]:
        return await self._fetch_all(
            """
            SELECT q.*, s.title AS source_title, t.title AS topic_title
            FROM recovery_queue q
            LEFT JOIN source_groups s ON s.id=q.source_group_id
            LEFT JOIN topics t ON t.source_group_id=q.source_group_id AND t.topic_id=q.topic_id
            ORDER BY q.id DESC
            LIMIT 500
            """
        )

    async def claim_next_recovery(self) -> dict[str, Any] | None:
        async with self._write_lock:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cur = await conn.execute(
                    """
                    SELECT * FROM recovery_queue
                    WHERE status='pending'
                    ORDER BY id ASC
                    LIMIT 1
                    """
                )
                row = await cur.fetchone()
                await cur.close()
                if not row:
                    return None

                now = self._now()
                await conn.execute(
                    "UPDATE recovery_queue SET status='running', updated_at=? WHERE id=?",
                    (now, row["id"]),
                )
                await conn.commit()
                data = dict(row)
                data["status"] = "running"
                data["updated_at"] = now
                return data

    async def mark_recovery_assigned_channel(self, queue_id: int, new_channel_chat_id: int) -> None:
        await self._execute(
            """
            UPDATE recovery_queue
            SET new_channel_chat_id=?, updated_at=?
            WHERE id=?
            """,
            (new_channel_chat_id, self._now(), queue_id),
        )

    async def update_recovery_progress(self, queue_id: int, last_cloned_message_id: int) -> None:
        await self._execute(
            """
            UPDATE recovery_queue
            SET last_cloned_message_id=?, updated_at=?
            WHERE id=?
            """,
            (last_cloned_message_id, self._now(), queue_id),
        )

    async def mark_recovery_done(
        self,
        queue_id: int,
        new_channel_chat_id: int,
        summary: str = "",
        last_cloned_message_id: int | None = None,
    ) -> None:
        await self._execute(
            """
            UPDATE recovery_queue
            SET status='done',
                new_channel_chat_id=?,
                last_error=?,
                last_cloned_message_id=COALESCE(?, last_cloned_message_id),
                updated_at=?
            WHERE id=?
            """,
            (
                new_channel_chat_id,
                summary,
                last_cloned_message_id,
                self._now(),
                queue_id,
            ),
        )

    async def mark_recovery_failed(
        self,
        queue_id: int,
        retry_count: int,
        error_text: str,
        max_retry: int,
    ) -> None:
        now = self._now()
        if retry_count + 1 < max_retry:
            await self._execute(
                """
                UPDATE recovery_queue
                SET status='pending', retry_count=?, last_error=?, updated_at=?
                WHERE id=?
                """,
                (retry_count + 1, error_text[:500], now, queue_id),
            )
        else:
            await self._execute(
                """
                UPDATE recovery_queue
                SET status='failed', retry_count=?, last_error=?, updated_at=?
                WHERE id=?
                """,
                (retry_count + 1, error_text[:500], now, queue_id),
            )

    async def get_next_available_standby_channel(self) -> dict[str, Any] | None:
        return await self._fetch_one(
            """
            SELECT * FROM channels
            WHERE is_standby=1 AND in_use=0
            ORDER BY id ASC
            LIMIT 1
            """
        )

    async def get_binding_by_channel(self, channel_chat_id: int) -> list[dict[str, Any]]:
        return await self._fetch_all(
            "SELECT * FROM topic_bindings WHERE channel_chat_id=? AND active=1",
            (channel_chat_id,),
        )
