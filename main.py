import asyncio
from datetime import datetime, time, date as dt
import logging

from app.controllers.crawler.daily_data.fetch_daily_data import fetch_daily_data
from app.controllers.crawler.historical_checker.downloader import DownloadMissing
from app.controllers.crawler.moment_data.final_ob_getter import set_rt_data
from app.controllers.tools.get_holidays import get_holiday

# Import loggers
from config.logger_setup import daily_task_logger, weekly_task_logger, daily_fetch_logger

async def run_daily_task():
    today = dt.today()
    is_holy = get_holiday(today)  # async call
    now = datetime.now().time()

    start_time = time(8, 57)
    end_time = time(12, 35)

    if not is_holy and start_time <= now <= end_time:
        await set_rt_data()
        daily_task_logger.info("set_rt_data executed successfully.")
    else:
        daily_task_logger.info("Not running now (either holiday or not in time window).")

async def run_daily_task_loop():
    while True:
        now = datetime.now()
        start_time = time(8, 57)
        end_time = time(13, 35)

        if start_time <= now.time() <= end_time:
            await run_daily_task()
            await asyncio.sleep(60)
        else:
            if now.time() > end_time:
                next_day = dt(now.year, now.month, now.day + 1)
            else:
                next_day = dt.today()

            next_run = datetime.combine(next_day, start_time)
            wait_seconds = (next_run - datetime.now()).total_seconds()
            if wait_seconds < 0:
                wait_seconds = 0
            daily_task_logger.info(f"Waiting {wait_seconds} seconds until next run_daily_task.")
            await asyncio.sleep(wait_seconds)

async def run_weekly_task_loop():
    while True:
        now = datetime.now()
        if now.weekday() == 4: #and now.time() >= time(20, 0):
            await fetch_daily_data()
            await DownloadMissing()
            weekly_task_logger.info("DownloadMissing executed successfully.")
            await asyncio.sleep(24 * 3600)
        else:
            weekly_task_logger.info("Not Wednesday 20:00 yet, waiting 60 seconds.")
            await asyncio.sleep(60)

async def run_daily_fetch_data_loop():
    while True:
        now = datetime.now()
        today = dt.today()
        is_holy = get_holiday(today)

        if not is_holy and now.time() >= time(22, 0):
            await fetch_daily_data()
            daily_fetch_logger.info("fetch_daily_data executed successfully.")

            next_day = dt(today.year, today.month, today.day + 1)
            next_run = datetime.combine(next_day, time(22, 0))
            wait_seconds = (next_run - datetime.now()).total_seconds()
            await asyncio.sleep(wait_seconds)
        else:
            daily_fetch_logger.info("Not after 22:00 or market is closed, waiting 60 seconds.")
            await asyncio.sleep(60)

async def main():
    await asyncio.gather(
        run_daily_task_loop(),
        run_weekly_task_loop(),
        run_daily_fetch_data_loop()
    )

if __name__ == "__main__":
    asyncio.run(main())
