from prefect import Task
from loguru import logger
from dynaconf import settings
import pandas as pd
import glob
import os
import json
import datetime
from tqdm import tqdm

FILE_LOCATION = settings["phrasenode_pages"]


class PreprocessPNPagesTask(Task):
    def run(self):
        logger.info("**** Preprocessing Pages from PhraseNode ****")
        pages = dict()
        largest = 0
        average = 0
        html_files = [f for f in glob.glob(f"{FILE_LOCATION}/*.html")]
        for html_file in tqdm(html_files):
            with open(html_file, "r") as f:
                page_name = (
                    os.path.basename(html_file)
                    .replace("info-", "")
                    .replace(".html", "")
                )
                pages_content = json.load(f)
                pages[page_name] = dict()

                ## Add the pages
                for e in pages_content["info"]:
                    if "xid" in e:
                        pages[page_name][e["xid"]] = e
                    else:
                        if "tag" in e and e["tag"] == "BODY":
                            pages[page_name][0] = e

                ## Create parent field
                for id_page, content in tqdm(pages[page_name].items()):
                    if "children" in content:
                        for c in content["children"]:
                            if c in pages[page_name]:
                                pages[page_name][c]["parent"] = id_page

                if len(pages_content["info"]) > largest:
                    largest = len(pages_content["info"])
                average = len(pages_content["info"]) + average

        logger.debug(f"Largest: {largest}")
        logger.debug(f"Average: {average/len(pages)}")
        logger.debug(f"Total pages: {len(pages)}")

        return pages

