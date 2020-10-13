from prefect import Task
from loguru import logger
from dynaconf import settings
import pandas as pd
import jsonlines
import json
from os import path
from one_shot_ipa.util import BM25Fit, BM25Search
from typing import Dict
from tqdm import tqdm
import ray
from one_shot_ipa.tasks.parallel import RayExecutor


class BM25FilterTask(Task):
    @staticmethod
    def run_bm25(pos, input, pages, select_k):
        def clean_page(page):
            pages_string = list()
            for id_p, content_p in page.items():
                pages_string.extend(id_p.split())
                if isinstance(content_p, Dict):
                    for in_id, in_content in content_p.items():
                        pages_string.extend(in_id.split())
                        pages_string.extend(in_content.split())
                else:
                    pages_string.extend(str(content_p).split())

            return pages_string

        filtered_values = dict()
        for id_i, content in tqdm(input.items()):
            if content["webpage"] not in pages:
                logger.error(f"Page {content['webpage']} not found!")
                continue
            page_strings = dict()
            selected_page = pages[content["webpage"]]
            for id_p, content_page in selected_page.items():
                page_strings[id_p] = clean_page(content_page)

            page_strings["phrase"] = content["phrase"].split()
            logger.info(page_strings["phrase"])

            fit_class = BM25Fit()
            ix = fit_class.run(page_strings)
            search_class = BM25Search()

            retrieval_results = search_class.run(
                {"query_phrase": page_strings["phrase"]}, ix, limit=select_k
            )

            filtered_values[id_i] = list(retrieval_results["query_phrase"].keys())[1:]
        return filtered_values

    def run(self, pages, data, select_k=10):
        logger.info(f"*** Running BM25 Filter Task - Selecting {select_k} nodes ****")
        logger.info(f"Pages: {len(pages)}")
        logger.info(f"Data: {len(data)}")
        ray_executor = RayExecutor()
        relevant_values = 0
        total_runs = 0
        filtered_values = ray_executor.run(
            data,
            self.run_bm25,
            fn_args=dict(pages=pages, select_k=select_k,),
            batch_count=4,
            is_parallel=False,
        )

        for id_i, found_values in filtered_values.items():
            if data[id_i]["xid"] in filtered_values[id_i]:
                node_found = 1
            else:
                node_found = 0

            relevant_values = relevant_values + node_found
            total_runs = total_runs + 1

            logger.info(f"Recall at {select_k}: {relevant_values/total_runs}")
        logger.info(f"*** Final Recall at {select_k}: {relevant_values/total_runs} ***")
        return filtered_values
