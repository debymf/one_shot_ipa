from prefect import Task
from loguru import logger
from dynaconf import settings

FILE_LOCATION = settings["txt_location"]


class SampleTask(Task):
    def run(self):
        logger.info("Running Sample Task")
        with open(FILE_LOCATION, "r") as f:
            lines = f.readlines()

        logger.info(lines)
        return lines

