import time
import traceback

from src.log_utils import create_logger
from src.services import NadeoCore
from src.threads.abstract_thread import AbstractThread

logger = create_logger("update_main_nadeo_token")


class UpdateMainNadeoTokenThread(AbstractThread):
    def handle(self):
        while True:
            try:
                logger.info("Refreshing Nadeo access token")
                NadeoCore.refresh_token()
                logger.info("Refreshed Nadeo access token successfully")
            except Exception as e:
                logger.error(
                    "General error in the thread",
                    extra={"exception": e, "traceback": traceback.format_exc()},
                )
            time.sleep(42800)
