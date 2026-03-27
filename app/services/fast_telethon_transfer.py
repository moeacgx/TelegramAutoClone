from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)
_warned_unavailable = False
_warned_failed = False


async def fast_upload_file(
    client: Any,
    file_path: str | Path,
    *,
    connections: int | None = None,
    part_size: int = 512 * 1024,
    progress_callback: Callable[[int, int], None] | None = None,
):
    """优先使用 FastTelethon 多连接上传，失败时自动降级到 Telethon 原生上传。"""
    path = Path(file_path)
    if connections is None:
        raw_connections = str(os.environ.get("FAST_TELETHON_CONNECTIONS") or "").strip()
        if raw_connections:
            try:
                connections = max(1, int(raw_connections))
            except Exception:  # noqa: BLE001
                connections = None
    if part_size <= 0:
        part_size = 512 * 1024
    elif part_size == 512 * 1024:
        raw_part_size = str(os.environ.get("FAST_TELETHON_PART_SIZE") or "").strip()
        if raw_part_size:
            try:
                part_size = max(32 * 1024, int(raw_part_size))
            except Exception:  # noqa: BLE001
                part_size = 512 * 1024

    try:
        import FastTelethonhelper  # type: ignore  # noqa: F401
        from FastTelethonhelper.FastTelethon import ParallelTransferrer  # type: ignore
    except Exception:  # noqa: BLE001
        global _warned_unavailable
        if not _warned_unavailable:
            _warned_unavailable = True
            logger.info("FastTelethon 未安装，上传将回退到 Telethon 原生方式")
        return await client.upload_file(str(path))

    try:
        from telethon import helpers
        from telethon.tl.types import InputFile, InputFileBig
    except Exception:  # noqa: BLE001
        return await client.upload_file(str(path))

    try:
        file_size = path.stat().st_size
        file_id = helpers.generate_random_long()
        uploader = ParallelTransferrer(client)
        part_size_kb = max(1, int(part_size) // 1024)
        connection_count = int(connections) if connections else None
        part_size_bytes, part_count, is_large = await uploader.init_upload(
            file_id,
            file_size,
            part_size_kb=part_size_kb,
            connection_count=connection_count,
        )
        part_size_bytes = int(part_size_bytes or part_size)
        part_count = int(part_count)

        uploaded = 0
        md5 = hashlib.md5()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(part_size_bytes)
                if not chunk:
                    break
                await uploader.upload(chunk)
                uploaded += len(chunk)
                if not is_large:
                    md5.update(chunk)
                if progress_callback is not None:
                    try:
                        progress_callback(uploaded, file_size)
                    except Exception:  # noqa: BLE001
                        pass

        await uploader.finish_upload()

        if is_large:
            return InputFileBig(file_id, part_count, path.name)
        return InputFile(file_id, part_count, path.name, md5.hexdigest())
    except Exception as exc:  # noqa: BLE001
        global _warned_failed
        if not _warned_failed:
            _warned_failed = True
            logger.warning("FastTelethon 上传失败，已自动降级到 Telethon 原生方式: %s", exc)
        return await client.upload_file(str(path))
