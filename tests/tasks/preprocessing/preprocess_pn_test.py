import unittest
from loguru import logger
import sys
from dynaconf import settings
from one_shot_ipa.tasks.preprocessing import PreprocessPNDataTask, PreprocessPNPagesTask


class PreprocessPNTest(unittest.TestCase):
    def test_preprocess_data_pn(self):
        FILE_LOCATION = settings["phrasenode_sample"]
        logger.debug("*** Start test for preprocess Phrase Node ***")
        prep_pn_pages = PreprocessPNPagesTask()
        pages = prep_pn_pages.run()
        for title, content in pages.items():
            logger.info(f"Title: {title}")
            for t_c, c in content.items():
                logger.info(f"Content: {t_c}")
                logger.info(f"Content: {c}")

                break
        prep_pn_data = PreprocessPNDataTask()
        prep_pn_data.run(FILE_LOCATION)

