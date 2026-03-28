import traceback
from datetime import datetime
from typing import Optional

from src.log_utils import create_logger

logger = create_logger("abstract_thread")


class AbstractThread:
    def __init__(self):
        self.start_time: datetime = datetime.now()
        self.last_error_time: Optional[datetime] = None
        self.error_count: int = 0

    def _record_error(self):
        self.last_error_time = datetime.now()
        self.error_count += 1

    def handle(self):
        raise NotImplementedError("handle method needs to be implemented")

    def run(self):
        try:
            self.handle()
        except Exception as e:
            logger.error(
                f"Unhandled exception crashed thread {self.__class__.__name__}",
                extra={"exception": e, "traceback": traceback.format_exc()},
            )
            raise
