import logging
import os
import queue
import sys
import time
from logging.handlers import QueueHandler, QueueListener
from typing import Optional

# Global queue for logging
_log_queue: Optional[queue.Queue] = None
_queue_listener: Optional[QueueListener] = None


def setup_logger(
    log_level=logging.INFO,
    log_folder="logs",
    log_file_name=f"scraper_run_{int(time.time())}.log",
    include_stream_handler=True,
):
    """
    Sets up and returns a logger for the web scraper using a queue-based approach
    for thread-safe and reliable logging.

    Args:
        log_level (int): The minimum logging level to capture (e.g., logging.DEBUG, logging.INFO).
                         Defaults to logging.INFO.
        log_folder (str): The folder to save log files in.
        log_file_name (str): The name of the log file.
        include_stream_handler (bool): Whether to output logs to the console.
    """
    global _log_queue, _queue_listener

    logger = logging.getLogger("WebScraper")
    logger.setLevel(log_level)
    logger.propagate = False

    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)
    if _queue_listener:
        _queue_listener.stop()
        _queue_listener = None
    if _log_queue:
        while not _log_queue.empty():
            try:
                _log_queue.get_nowait()
            except queue.Empty:
                break
        _log_queue = None

    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    log_file_path = os.path.join(log_folder, log_file_name)

    # --- Define Formats ---
    debug_format = (
        "%(levelname)-8s [%(asctime)s] [%(threadName)s] "
        "[%(name)s:%(module)s:%(funcName)s:%(lineno)d] -> %(message)s"
    )
    info_format = "%(name)s - %(asctime)s - %(levelname)s: %(message)s"

    chosen_format_string = ""
    if log_level <= logging.DEBUG:
        chosen_format_string = debug_format
    else:
        chosen_format_string = info_format

    formatter = logging.Formatter(chosen_format_string)

    target_handlers = []

    # File Handler
    file_handler = logging.FileHandler(log_file_path, errors="replace")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)
    target_handlers.append(file_handler)

    # Stream Handler (for console output)
    if include_stream_handler:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(log_level)
        target_handlers.append(stream_handler)

    # --- Queue Setup ---
    _log_queue = queue.Queue()
    queue_handler = QueueHandler(_log_queue)
    logger.addHandler(queue_handler)

    # --- Queue Listener Setup ---
    _queue_listener = QueueListener(_log_queue, *target_handlers)
    _queue_listener.start()

    return logger


def stop_queue_listener():
    """Stops the queue listener thread, to be called on application shutdown."""
    global _queue_listener
    if _queue_listener:
        _queue_listener.stop()
        _queue_listener = None
        print("Logger queue listener stopped.")
