import logging

from app.config import Settings
from app.db import Database
from app.services.channel_service import ChannelService
from app.services.clone_service import CloneService
from app.services.telegram_manager import TelegramManager

logger = logging.getLogger(__name__)


class RecoveryWorker:
    def __init__(
        self,
        db: Database,
        telegram: TelegramManager,
        clone_service: CloneService,
        channel_service: ChannelService,
        settings: Settings,
    ):
        self.db = db
        self.telegram = telegram
        self.clone_service = clone_service
        self.channel_service = channel_service
        self.settings = settings

    async def _get_channel_title(self, channel_chat_id: int) -> str:
        row = await self.db.get_channel(channel_chat_id)
        title = str((row or {}).get("title") or "").strip()
        if title:
            return title
        try:
            entity = await self.telegram.bot_client.get_entity(channel_chat_id)
            return str(getattr(entity, "title", channel_chat_id))
        except Exception:
            return str(channel_chat_id)

    async def run_once(self, queue_id: int | None = None) -> bool:
        if queue_id is None:
            job = await self.db.claim_next_recovery()
        else:
            job = await self.db.claim_recovery_by_id(queue_id)
        if not job:
            return False

        queue_id = int(job["id"])
        source_group: dict | None = None
        topic: dict | None = None
        try:
            source_group_id = int(job["source_group_id"])
            topic_id = int(job["topic_id"])
            old_channel_id = int(job["old_channel_chat_id"])
            start_message_id = int(job.get("last_cloned_message_id") or 0)

            source_group = await self.db.get_source_group_by_id(source_group_id)
            if not source_group:
                raise RuntimeError("任务组不存在")
            source_title = str(source_group.get("title") or source_group_id)

            topic = await self.db.get_topic(source_group_id, topic_id)
            if not topic:
                raise RuntimeError("话题不存在")
            topic_title = str(topic.get("title") or topic_id)

            assigned_channel = job.get("new_channel_chat_id")
            if assigned_channel:
                new_channel_id = int(assigned_channel)
            else:
                standby_channel = await self.db.get_next_available_standby_channel()
                if not standby_channel:
                    raise RuntimeError("没有可用备用频道")

                new_channel_id = int(standby_channel["chat_id"])
                await self.channel_service.rename_channel(new_channel_id, topic["title"])

                await self.db.consume_standby_channel(new_channel_id)
                await self.db.detach_channel_bindings(old_channel_id)
                await self.db.upsert_binding(
                    source_group_id=source_group_id,
                    topic_id=topic_id,
                    channel_chat_id=new_channel_id,
                )
                await self.db.mark_recovery_assigned_channel(queue_id, new_channel_id)

            old_channel_title = await self._get_channel_title(old_channel_id)
            new_channel_title = await self._get_channel_title(new_channel_id)

            async def check_should_stop() -> bool:
                return await self.db.is_recovery_stop_requested(queue_id)

            async def save_checkpoint(last_message_id: int) -> None:
                if await check_should_stop():
                    raise RuntimeError("任务已手动停止")
                await self.db.update_recovery_progress(queue_id, last_message_id)

            clone_stats = await self.clone_service.clone_topic_history(
                source_chat_id=int(source_group["chat_id"]),
                topic_id=topic_id,
                target_channel=new_channel_id,
                start_message_id=start_message_id,
                progress_hook=save_checkpoint,
                should_stop=check_should_stop,
            )
            resumed_from = int(clone_stats.get("started_min_id") or start_message_id)

            summary = (
                f"恢复完成, cloned={clone_stats['cloned']}, "
                f"total={clone_stats['total']}, skipped={clone_stats['skipped']}, "
                f"resumed_from={resumed_from}"
            )
            await self.db.mark_recovery_done(
                queue_id=queue_id,
                new_channel_chat_id=new_channel_id,
                summary=summary,
                last_cloned_message_id=int(clone_stats.get("last_cloned_message_id") or start_message_id),
            )
            await self.telegram.send_notification(
                f"✅ 频道恢复完成\n"
                f"任务组: {source_title} (id={source_group_id})\n"
                f"话题: {topic_title} (topic_id={topic_id})\n"
                f"旧频道: {old_channel_title} ({old_channel_id})\n"
                f"新频道: {new_channel_title} ({new_channel_id})\n"
                f"克隆统计: {summary}"
            )

            # 恢复成功后自动清理：封禁记录与已完成队列项。
            await self.db.remove_banned_channel(
                source_group_id=source_group_id,
                topic_id=topic_id,
                channel_chat_id=old_channel_id,
            )
            await self.db.delete_recovery_task(queue_id)
            return True

        except Exception as exc:
            error_text = str(exc)
            if ("任务已手动停止" in error_text) or (await self.db.is_recovery_stop_requested(queue_id)):
                row = await self.db.get_recovery_by_id(queue_id)
                last_id = int((row or {}).get("last_cloned_message_id") or 0)
                source_title = str((source_group or {}).get("title") or job["source_group_id"])
                topic_title = str((topic or {}).get("title") or job["topic_id"])
                await self.db.mark_recovery_stopped(
                    queue_id=queue_id,
                    summary=error_text or "任务已手动停止",
                    last_cloned_message_id=last_id,
                )
                await self.telegram.send_notification(
                    f"⏹️ 频道恢复任务已停止\nqueue_id={queue_id}\n"
                    f"任务组: {source_title} (id={job['source_group_id']})\n"
                    f"话题: {topic_title} (topic_id={job['topic_id']})\n"
                    f"checkpoint={last_id}"
                )
                return True

            logger.exception("恢复任务失败(queue_id=%s): %s", queue_id, exc)
            source_title = str((source_group or {}).get("title") or job["source_group_id"])
            topic_title = str((topic or {}).get("title") or job["topic_id"])
            await self.db.mark_recovery_failed(
                queue_id=queue_id,
                retry_count=int(job.get("retry_count", 0)),
                error_text=error_text,
                max_retry=self.settings.recovery_max_retry,
            )
            await self.telegram.send_notification(
                f"❌ 频道恢复失败\nqueue_id={queue_id}\n"
                f"任务组: {source_title} (id={job['source_group_id']})\n"
                f"话题: {topic_title} (topic_id={job['topic_id']})\n"
                f"错误: {error_text[:300]}"
            )
            return True
