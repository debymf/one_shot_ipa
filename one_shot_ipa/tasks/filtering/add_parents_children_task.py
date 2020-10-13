from prefect import Task
from loguru import logger
from dynaconf import settings
import pandas as pd
from tqdm import tqdm
from loguru import logger


class AddParentChildrenTask(Task):
    def run(self, dataset, pages, filtered_results):
        logger.info("*** Adding Parent and Children to the BM25 Result ***")
        expanded_results = dict()

        for id_entry, retrieved in tqdm(filtered_results.items()):
            selected_page = dataset[id_entry]["webpage"]
            new_list_results = list(retrieved)
            i = 0
            for r in retrieved:
                i = i + 1
                if r in pages[selected_page]:
                    if "parent" in pages[selected_page][r]:
                        new_list_results.append(pages[selected_page][r]["parent"])
                    if "children" in pages[selected_page][r]:
                        new_list_results.extend(pages[selected_page][r]["children"])

            expanded_results[id_entry] = new_list_results

        relevant_values = 0
        total_runs = 0
        for id_i, found_values in expanded_results.items():
            if dataset[id_i]["xid"] in expanded_results[id_i]:
                node_found = 1
            else:
                node_found = 0

            relevant_values = relevant_values + node_found
            total_runs = total_runs + 1

        logger.info(
            f"*** Final Recall after adding parents/children: {relevant_values/total_runs} ***"
        )
