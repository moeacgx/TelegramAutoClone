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

    async def run_once(self) -> bool:
        job = await self.db.claim_next_recovery()
        if not job:
            return False

        queue_id = int(job["id"])
        try:
            source_group_id = int(job["source_group_id"])
            topic_id = int(job["topic_id"])
            old_channel_id = int(job["old_channel_chat_id"])
            start_message_id = int(job.get("last_cloned_message_id") or 0)

            source_group = await self.db.get_source_group_by_id(source_group_id)
            if not source_group:
                raise RuntimeError("任务组不存在")

            topic = await self.db.get_topic(source_group_id, topic_id)
            if not topic:
                raise RuntimeError("话题不存在")

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

            async def save_checkpoint(last_message_id: int) -> None:
                await self.db.update_recovery_progress(queue_id, last_message_id)

            clone_stats = await self.clone_service.clone_topic_history(
                source_chat_id=int(source_group["chat_id"]),
                topic_id=topic_id,
                target_channel=new_channel_id,
                start_message_id=start_message_id,
                progress_hook=save_checkpoint,
            )

            summary = (
                f"恢复完成, cloned={clone_stats['cloned']}, "
                f"total={clone_stats['total']}, skipped={clone_stats['skipped']}, "
                f"resumed_from={start_message_id}"
            )
            await self.db.mark_recovery_done(
                queue_id=queue_id,
                new_channel_chat_id=new_channel_id,
                summary=summary,
                last_cloned_message_id=int(clone_stats.get("last_cloned_message_id") or start_message_id),
            )
            await self.telegram.send_notification(
                f"✅ 频道恢复完成\n"
                f"source_group_id={source_group_id} topic_id={topic_id}\n"
                f"旧频道={old_channel_id}\n新频道={new_channel_id}\n"
                f"克隆统计: {summary}"
            )
            return True

        except Exception as exc:
            logger.exception("恢复任务失败(queue_id=%s): %s", queue_id, exc)
            await self.db.mark_recovery_failed(
                queue_id=queue_id,
                retry_count=int(job.get("retry_count", 0)),
                error_text=str(exc),
                max_retry=self.settings.recovery_max_retry,
            )
            await self.telegram.send_notification(
                f"❌ 频道恢复失败\nqueue_id={queue_id}\n"
                f"source_group_id={job['source_group_id']} topic_id={job['topic_id']}\n"
                f"错误: {str(exc)[:300]}"
            )
            return True
