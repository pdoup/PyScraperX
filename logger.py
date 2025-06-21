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
    log_folder="logs",
    log_file_name=f"scraper_run_{int(time.time())}.log",
    include_stream_handler=True,
):
    """
    Sets up and returns a logger for the web scraper using a queue-based approach
    for thread-safe and reliable logging.
    """
    global _log_queue, _queue_listener

    logger = logging.getLogger("WebScraper")
    logger.setLevel(logging.INFO)
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

    # Create the logs folder if it doesn't exist
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    log_file_path = os.path.join(log_folder, log_file_name)

    # Define the formatter once
    formatter = logging.Formatter("%(name)s - %(asctime)s - %(levelname)s: %(message)s")

    target_handlers = []

    # File Handler
    file_handler = logging.FileHandler(log_file_path, errors="replace")
    file_handler.setFormatter(formatter)
    target_handlers.append(file_handler)

    # Stream Handler (for console output)
    if include_stream_handler:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        target_handlers.append(stream_handler)

    # --- Queue Setup ---
    _log_queue = queue.Queue()
    queue_handler = QueueHandler(_log_queue)
    logger.addHandler(queue_handler)

    # --- Queue Listener Setup ---
    _queue_listener = QueueListener(_log_queue, *target_handlers)
    _queue_listener.start()  # Start the listener thread

    return logger


def stop_queue_listener():
    """Stops the queue listener thread, to be called on application shutdown."""
    global _queue_listener
    if _queue_listener:
        _queue_listener.stop()
        _queue_listener = None
        print("Logger queue listener stopped.")
