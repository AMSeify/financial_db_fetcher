import logging
from logging.handlers import QueueHandler, QueueListener
from queue import Queue

# Queues for each function's logs
daily_task_queue = Queue()
weekly_task_queue = Queue()
daily_fetch_queue = Queue()

# Create loggers
daily_task_logger = logging.getLogger("daily_task_logger")
weekly_task_logger = logging.getLogger("weekly_task_logger")
daily_fetch_logger = logging.getLogger("daily_fetch_logger")

daily_task_logger.setLevel(logging.DEBUG)
weekly_task_logger.setLevel(logging.DEBUG)
daily_fetch_logger.setLevel(logging.DEBUG)

# Create queue handlers
daily_task_handler = QueueHandler(daily_task_queue)
weekly_task_handler = QueueHandler(weekly_task_queue)
daily_fetch_handler = QueueHandler(daily_fetch_queue)

daily_task_logger.addHandler(daily_task_handler)
weekly_task_logger.addHandler(weekly_task_handler)
daily_fetch_logger.addHandler(daily_fetch_handler)
