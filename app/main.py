import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import get_settings
from app.db import Database
from app.routers import auth, bindings, channels, dashboard, queue, source_groups, topics
from app.services.channel_service import ChannelService
from app.services.clone_service import CloneService
from app.services.listener_service import ListenerService
from app.services.monitor_service import MonitorService
from app.services.recovery_worker import RecoveryWorker
from app.services.telegram_manager import TelegramManager
from app.services.topic_service import TopicService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    db = Database(settings.database_path)
    await db.init()

    telegram = TelegramManager(settings)
    await telegram.start()

    topic_service = TopicService(db, telegram)
    channel_service = ChannelService(db, telegram)
    clone_service = CloneService(telegram)
    listener_service = ListenerService(db, telegram, clone_service)
    monitor_service = MonitorService(db, telegram, channel_service, settings.monitor_interval_seconds)
    recovery_worker = RecoveryWorker(db, telegram, clone_service, channel_service, settings)

    templates = Jinja2Templates(directory="app/templates")

    app.state.settings = settings
    app.state.db = db
    app.state.telegram = telegram
    app.state.topic_service = topic_service
    app.state.channel_service = channel_service
    app.state.clone_service = clone_service
    app.state.listener_service = listener_service
    app.state.monitor_service = monitor_service
    app.state.recovery_worker = recovery_worker
    app.state.templates = templates

    await listener_service.start()

    async def monitor_loop():
        while True:
            try:
                await telegram.cleanup()
                if await telegram.is_bot_authorized():
                    await monitor_service.scan_once()
            except Exception as exc:
                logger.exception("monitor_loop 异常: %s", exc)
            await asyncio.sleep(settings.monitor_interval_seconds)

    async def standby_loop():
        while True:
            try:
                if await telegram.is_bot_authorized():
                    await channel_service.refresh_standby_channels()
            except Exception as exc:
                logger.exception("standby_loop 异常: %s", exc)
            await asyncio.sleep(settings.standby_refresh_seconds)

    async def recovery_loop():
        while True:
            try:
                if not (await telegram.is_bot_authorized()) or not (await telegram.is_user_authorized()):
                    await asyncio.sleep(2)
                    continue
                processed = await recovery_worker.run_once()
                if not processed:
                    await asyncio.sleep(2)
            except Exception as exc:
                logger.exception("recovery_loop 异常: %s", exc)
                await asyncio.sleep(2)

    tasks = [
        asyncio.create_task(monitor_loop(), name="monitor_loop"),
        asyncio.create_task(standby_loop(), name="standby_loop"),
        asyncio.create_task(recovery_loop(), name="recovery_loop"),
    ]

    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await listener_service.stop()
        await telegram.stop()


app = FastAPI(title="Telegram Auto Clone", lifespan=lifespan)

app.mount("/static", StaticFiles(directory="app/static"), name="static")

app.include_router(dashboard.router)
app.include_router(auth.router)
app.include_router(source_groups.router)
app.include_router(topics.router)
app.include_router(bindings.router)
app.include_router(channels.router)
app.include_router(queue.router)
