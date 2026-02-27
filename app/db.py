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

    async def delete_source_group(self, source_group_id: int) -> dict[str, int]:
        now = self._now()
        async with self._write_lock:
            async with aiosqlite.connect(self.db_path) as conn:
                await conn.execute("PRAGMA foreign_keys = ON")
                conn.row_factory = aiosqlite.Row

                cur = await conn.execute(
                    "SELECT id FROM source_groups WHERE id=?",
                    (source_group_id,),
                )
                exists = await cur.fetchone()
                await cur.close()
                if not exists:
                    return {
                        "source_groups": 0,
                        "topics": 0,
                        "topic_bindings": 0,
                        "banned_channels": 0,
                        "recovery_queue": 0,
                        "released_channels": 0,
                        "running_jobs": 0,
                    }

                cur = await conn.execute(
                    """
                    SELECT COUNT(1)
                    FROM recovery_queue
                    WHERE source_group_id=? AND status IN ('running','stopping')
                    """,
                    (source_group_id,),
                )
                running_row = await cur.fetchone()
                await cur.close()
                running_count = int((running_row[0] if running_row else 0) or 0)
                if running_count > 0:
                    raise ValueError(f"该任务组存在 {running_count} 个运行中的恢复任务，请先停止后再删除")

                cur = await conn.execute(
                    "SELECT DISTINCT channel_chat_id FROM topic_bindings WHERE source_group_id=?",
                    (source_group_id,),
                )
                bound_rows = await cur.fetchall()
                await cur.close()
                bound_channel_ids = [int(row[0]) for row in bound_rows]

                counts: dict[str, int] = {}
                for table in ("recovery_queue", "banned_channels", "topic_bindings", "topics"):
                    cur = await conn.execute(
                        f"DELETE FROM {table} WHERE source_group_id=?",
                        (source_group_id,),
                    )
                    counts[table] = int(cur.rowcount if cur.rowcount is not None else 0)
                    await cur.close()

                cur = await conn.execute(
                    "DELETE FROM source_groups WHERE id=?",
                    (source_group_id,),
                )
                counts["source_groups"] = int(cur.rowcount if cur.rowcount is not None else 0)
                await cur.close()

                released_channels = 0
                for channel_chat_id in bound_channel_ids:
                    cur = await conn.execute(
                        "SELECT COUNT(1) FROM topic_bindings WHERE channel_chat_id=? AND active=1",
                        (channel_chat_id,),
                    )
                    row = await cur.fetchone()
                    await cur.close()
                    active_count = int((row[0] if row else 0) or 0)
                    if active_count == 0:
                        await conn.execute(
                            "UPDATE channels SET in_use=0, updated_at=? WHERE chat_id=?",
                            (now, channel_chat_id),
                        )
                        released_channels += 1

                await conn.commit()
                return {
                    "source_groups": counts.get("source_groups", 0),
                    "topics": counts.get("topics", 0),
                    "topic_bindings": counts.get("topic_bindings", 0),
                    "banned_channels": counts.get("banned_channels", 0),
                    "recovery_queue": counts.get("recovery_queue", 0),
                    "released_channels": released_channels,
                    "running_jobs": running_count,
                }

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
        SELECT
            b.*,
            t.title AS topic_title,
            s.title AS source_title,
            c.title AS channel_title
        FROM topic_bindings b
        LEFT JOIN topics t ON t.source_group_id=b.source_group_id AND t.topic_id=b.topic_id
        LEFT JOIN source_groups s ON s.id=b.source_group_id
        LEFT JOIN channels c ON c.chat_id=b.channel_chat_id
        """
        if source_group_id is None:
            return await self._fetch_all(sql + " ORDER BY b.id DESC")
        return await self._fetch_all(sql + " WHERE b.source_group_id=? ORDER BY b.id DESC", (source_group_id,))

    async def list_active_bindings(self) -> list[dict[str, Any]]:
        return await self._fetch_all(
            """
            SELECT
                b.*,
                t.title AS topic_title,
                t.enabled AS topic_enabled,
                s.chat_id AS source_chat_id,
                s.title AS source_title,
                s.enabled AS source_enabled,
                c.title AS channel_title
            FROM topic_bindings b
            JOIN topics t ON t.source_group_id=b.source_group_id AND t.topic_id=b.topic_id
            JOIN source_groups s ON s.id=b.source_group_id
            LEFT JOIN channels c ON c.chat_id=b.channel_chat_id
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

    async def reset_available_standby_channels(self) -> None:
        await self._execute(
            "UPDATE channels SET is_standby=0, updated_at=? WHERE in_use=0",
            (self._now(),),
        )

    async def delete_channel(self, chat_id: int) -> None:
        await self._execute("DELETE FROM channels WHERE chat_id=?", (chat_id,))

    async def clear_standby_channels(self) -> None:
        # 清空备用池时同步清理所有未占用频道缓存，避免旧缓存在后续校验中回灌。
        await self._execute("DELETE FROM channels WHERE in_use=0")

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
        now = self._now()
        existing = await self._fetch_all(
            """
            SELECT id
            FROM banned_channels
            WHERE source_group_id=? AND topic_id=? AND channel_chat_id=?
            ORDER BY id DESC
            """,
            (source_group_id, topic_id, channel_chat_id),
        )
        if existing:
            keep_id = int(existing[0]["id"])
            await self._execute(
                """
                UPDATE banned_channels
                SET reason=?, detected_at=?
                WHERE id=?
                """,
                (reason, now, keep_id),
            )
            for row in existing[1:]:
                await self._execute(
                    "DELETE FROM banned_channels WHERE id=?",
                    (int(row["id"]),),
                )
            return

        await self._execute(
            """
            INSERT INTO banned_channels(source_group_id, topic_id, channel_chat_id, reason, detected_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (source_group_id, topic_id, channel_chat_id, reason, now),
        )

    async def list_banned_channels(self) -> list[dict[str, Any]]:
        return await self._fetch_all(
            """
            SELECT b.*, s.title AS source_title, t.title AS topic_title
            FROM banned_channels b
            JOIN (
                SELECT source_group_id, topic_id, channel_chat_id, MAX(id) AS latest_id
                FROM banned_channels
                GROUP BY source_group_id, topic_id, channel_chat_id
            ) latest ON latest.latest_id = b.id
            LEFT JOIN source_groups s ON s.id=b.source_group_id
            LEFT JOIN topics t ON t.source_group_id=b.source_group_id AND t.topic_id=b.topic_id
            ORDER BY b.id DESC
            LIMIT 300
            """
        )

    async def remove_banned_channel(
        self,
        source_group_id: int,
        topic_id: int,
        channel_chat_id: int,
    ) -> int:
        async with self._write_lock:
            async with aiosqlite.connect(self.db_path) as conn:
                cur = await conn.execute(
                    """
                    DELETE FROM banned_channels
                    WHERE source_group_id=? AND topic_id=? AND channel_chat_id=?
                    """,
                    (source_group_id, topic_id, channel_chat_id),
                )
                await conn.commit()
                rowcount = cur.rowcount
                await cur.close()
                return int(rowcount if rowcount is not None and rowcount >= 0 else 0)

    async def clear_banned_channels(self) -> int:
        async with self._write_lock:
            async with aiosqlite.connect(self.db_path) as conn:
                cur = await conn.execute("DELETE FROM banned_channels")
                await conn.commit()
                rowcount = cur.rowcount
                await cur.close()
                return int(rowcount if rowcount is not None and rowcount >= 0 else 0)

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

    async def enqueue_manual_recovery(
        self,
        source_group_id: int,
        topic_id: int,
        channel_chat_id: int,
        reason: str = "手动触发恢复任务",
    ) -> int:
        existing = await self._fetch_one(
            """
            SELECT id FROM recovery_queue
            WHERE source_group_id=? AND topic_id=? AND status IN ('pending','running','stopping')
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
                source_group_id, topic_id, old_channel_chat_id, new_channel_chat_id, reason,
                status, retry_count, last_cloned_message_id, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 'pending', 0, 0, ?, ?)
            """,
            (source_group_id, topic_id, channel_chat_id, channel_chat_id, reason, now, now),
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

    async def get_recovery_by_id(self, queue_id: int) -> dict[str, Any] | None:
        return await self._fetch_one(
            "SELECT * FROM recovery_queue WHERE id=?",
            (queue_id,),
        )

    async def requeue_recovery_task(self, queue_id: int, restart: bool = False) -> dict[str, Any] | None:
        row = await self.get_recovery_by_id(queue_id)
        if not row:
            return None

        status = str(row.get("status") or "")
        if status == "done":
            raise RuntimeError(f"任务 #{queue_id} 已完成，不能继续执行")

        now = self._now()
        retry_count = 0 if restart else int(row.get("retry_count") or 0)
        last_cloned_message_id = 0 if restart else int(row.get("last_cloned_message_id") or 0)
        action_text = "手动重新执行(从头)" if restart else "手动继续执行(断点)"
        await self._execute(
            """
            UPDATE recovery_queue
            SET status='pending',
                retry_count=?,
                last_cloned_message_id=?,
                last_error=?,
                updated_at=?
            WHERE id=?
            """,
            (retry_count, last_cloned_message_id, action_text, now, queue_id),
        )
        return await self.get_recovery_by_id(queue_id)

    async def stop_recovery_task(self, queue_id: int) -> dict[str, Any] | None:
        row = await self.get_recovery_by_id(queue_id)
        if not row:
            return None

        status = str(row.get("status") or "")
        now = self._now()
        if status == "pending":
            await self._execute(
                """
                UPDATE recovery_queue
                SET status='stopped', last_error='手动停止(未执行)', updated_at=?
                WHERE id=?
                """,
                (now, queue_id),
            )
        elif status == "running":
            await self._execute(
                """
                UPDATE recovery_queue
                SET status='stopping', last_error='已请求停止，等待当前步骤结束', updated_at=?
                WHERE id=?
                """,
                (now, queue_id),
            )
        elif status == "stopping":
            pass
        elif status in {"done", "failed", "stopped"}:
            raise RuntimeError(f"任务 #{queue_id} 当前状态为 {status}，无需停止")
        else:
            raise RuntimeError(f"任务 #{queue_id} 状态异常: {status}")

        return await self.get_recovery_by_id(queue_id)

    async def delete_recovery_task(self, queue_id: int) -> bool | None:
        row = await self.get_recovery_by_id(queue_id)
        if not row:
            return None

        status = str(row.get("status") or "")
        if status in {"running", "stopping"}:
            raise RuntimeError(f"任务 #{queue_id} 正在执行/停止中，请先等待停止完成")

        await self._execute("DELETE FROM recovery_queue WHERE id=?", (queue_id,))
        return True

    async def clear_recovery_queue(self, include_running: bool = False) -> dict[str, int]:
        running_count_row = await self._fetch_one(
            "SELECT COUNT(1) AS count FROM recovery_queue WHERE status IN ('running','stopping')"
        )
        running_count = int((running_count_row or {}).get("count") or 0)

        async with self._write_lock:
            async with aiosqlite.connect(self.db_path) as conn:
                if include_running:
                    cur = await conn.execute("DELETE FROM recovery_queue")
                else:
                    cur = await conn.execute(
                        "DELETE FROM recovery_queue WHERE status NOT IN ('running','stopping')"
                    )
                await conn.commit()
                deleted = int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0)
                await cur.close()

        return {
            "deleted": deleted,
            "skipped_running": 0 if include_running else running_count,
        }

    async def is_recovery_stop_requested(self, queue_id: int) -> bool:
        row = await self._fetch_one(
            "SELECT status FROM recovery_queue WHERE id=?",
            (queue_id,),
        )
        if not row:
            return True

        status = str(row.get("status") or "")
        return status in {"stopping", "stopped"}

    async def reset_running_recovery_tasks(self) -> int:
        now = self._now()
        async with self._write_lock:
            async with aiosqlite.connect(self.db_path) as conn:
                cur = await conn.execute(
                    """
                    UPDATE recovery_queue
                    SET status='pending',
                        last_error='手动重置运行中任务为 pending',
                        updated_at=?
                    WHERE status='running'
                    """,
                    (now,),
                )
                await conn.commit()
                rowcount = cur.rowcount
                await cur.close()
                return int(rowcount if rowcount is not None and rowcount >= 0 else 0)

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

    async def claim_recovery_by_id(self, queue_id: int) -> dict[str, Any] | None:
        async with self._write_lock:
            async with aiosqlite.connect(self.db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cur = await conn.execute(
                    "SELECT * FROM recovery_queue WHERE id=?",
                    (queue_id,),
                )
                row = await cur.fetchone()
                await cur.close()
                if not row:
                    return None

                status = str(row["status"])
                if status == "done":
                    return None
                if status == "running":
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

    async def mark_recovery_stopped(
        self,
        queue_id: int,
        summary: str = "",
        last_cloned_message_id: int | None = None,
    ) -> None:
        await self._execute(
            """
            UPDATE recovery_queue
            SET status='stopped',
                last_error=?,
                last_cloned_message_id=COALESCE(?, last_cloned_message_id),
                updated_at=?
            WHERE id=?
            """,
            (
                (summary or "任务已手动停止")[:500],
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
