# code from
import math
import multiprocessing
import time
from functools import reduce

import ray
from loguru import logger
from one_shot_ipa.util.data_manipulation.data_manipulation import create_dict_chunks
from tqdm import tqdm


class RayExecutor:
    def run(self, input, fn, fn_args, is_parallel=True, batch_count=-1, **kwargs):
        start = time.time()
        if is_parallel == True:
            ray.init(ignore_reinit_error=True)
            remote_fn = ray.remote(fn)

            batch_count = (
                multiprocessing.cpu_count() if batch_count == -1 else batch_count
            )
            batch_size = math.ceil(len(input) / batch_count)

            logger.info(f"Batch_size = {batch_size}")
            logger.info(f"Number of batches = {batch_count}")

            batches = create_dict_chunks(input, batch_size)

            logger.info("Running Ray Executor")

            batch_results = ray.get(
                [
                    remote_fn.remote(pos=pos, input=batch, **fn_args)
                    for pos, batch in enumerate(tqdm(batches))
                ]
            )

            combined_results = reduce(lambda x, y: {**x, **y}, batch_results, {})
        else:
            combined_results = fn(pos=0, input=input, **fn_args)
        end = time.time()
        logger.info(f"Processing time: {end-start}")
        return combined_results
