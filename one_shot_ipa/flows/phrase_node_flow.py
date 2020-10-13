from prefect import Flow, tags
import prefect
from dynaconf import settings
from loguru import logger
from prefect.engine.flow_runner import FlowRunner
from one_shot_ipa.tasks.preprocessing import PreprocessPNDataTask, PreprocessPNPagesTask
from one_shot_ipa.tasks.filtering import BM25FilterTask
import datetime
from prefect.engine.results import LocalResult

cache_args = dict(
    target="{task_name}-{task_tags}.pkl",
    checkpoint=True,
    result=LocalResult(dir=f"./cache/datasets/"),
)

task_pn_data = PreprocessPNDataTask(**cache_args)
task_pn_pages = PreprocessPNPagesTask(**cache_args)
task_filter_bm25 = BM25FilterTask()

TRAIN_PN_LOCATION = settings["phrasenode_train"]
TEST_PN_LOCATION = settings["phrasenode_test"]
DEV_PN_LOCATION = settings["phrasenode_dev"]

SELECTED_K = 10

with Flow("running-phase-node") as f:
    pages = task_pn_pages()
    with tags("train"):
        train = task_pn_data(TRAIN_PN_LOCATION)
    with tags("test"):
        test = task_pn_data(TEST_PN_LOCATION)
    with tags("dev"):
        dev = task_pn_data(DEV_PN_LOCATION)
    task_filter_bm25(pages, train, select_k=SELECTED_K)

f.run()
