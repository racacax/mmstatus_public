import traceback

from src.log_utils import create_logger

logger = create_logger("abstract_thread")


class AbstractThread:
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
