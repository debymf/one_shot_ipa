from prefect import Flow
import prefect
from loguru import logger
from prefect.engine.flow_runner import FlowRunner

from sample.tasks.sample_task import SampleTask

sample_task = SampleTask(cache_validator=prefect.engine.cache_validators.all_parameters)

with Flow("Running example flow") as flow1:
    text_result = sample_task()


FlowRunner(flow=flow1).run()

