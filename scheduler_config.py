from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Explicit zone: never depend on tzlocal reading the container's TZ variable.
# ZoneInfo data comes from the system tzdata package or the pip `tzdata` package.
scheduler = AsyncIOScheduler(timezone=ZoneInfo("Asia/Karachi"))
