import unittest
from loguru import logger
import sys
from dynaconf import settings
from one_shot_ipa.tasks.preprocessing import PreprocessPNDataTask, PreprocessPNPagesTask
from one_shot_ipa.tasks.filtering import BM25FilterTask


class BM25FilterTest(unittest.TestCase):
    def test_preprocess_data_pn(self):
        FILE_LOCATION = settings["phrasenode_sample"]
        logger.debug("*** Start test for preprocess Phrase Node ***")
        prep_pn_pages = PreprocessPNPagesTask()
        pages = prep_pn_pages.run()
        prep_pn_data = PreprocessPNDataTask()
        result_pages = prep_pn_data.run(FILE_LOCATION)
        bm25_filter_task = BM25FilterTask()
        filtered_values = bm25_filter_task.run(pages, result_pages)

        # logger.info(filtered_values)
