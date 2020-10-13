from prefect import Task
from loguru import logger
from dynaconf import settings
import pandas as pd
import jsonlines
import json
from os import path


class PreprocessPNDataTask(Task):
    def run(self, filename):
        logger.info("**** Preprocessing Data PhraseNode ****")
        dataset = dict()
        if path.exists(filename):
            with jsonlines.open(filename) as reader:
                for obj in reader:
                    dataset[len(dataset)] = obj

        return dataset
