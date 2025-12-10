import sys
import datetime

import asyncio
import httpx
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
import polars as pl

from loguru import logger

from api_load import fetch_all_with_ids

SEMAPHORE_LIMIT = 10


def config_client(): 
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Connection": "close"
    }

    client = httpx.AsyncClient(headers=headers)
    return client

def main():
    # 1422
    id_range = range(1, 200)

    with open("./output/bad_records.log", "w+") as known_errors_f:
        known_errors = {int(id_num) for id_num in known_errors_f}

    endpoint = "https://api.fencing.org.nz/public/results"
    param = "cmpId"


    data, errors = asyncio.run(
        fetch_all_with_ids(
            client=config_client(),
            base_url="https://api.fencing.org.nz/public/results",
            param=param,
            id_range=id_range,
            semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT),
            known_errors=known_errors
    ))
    logger.info(f"Total {len(data)+len(errors)} fetched. Successful records: {len(data)}. Unsuccessful records: {len(errors)}")

    df = pl.DataFrame(data)

    print(df.head)
    
    # Save all records done so far
    df.write_parquet("./output/initial_api_load2.parquet")
    
    # Save new errors found. Append only - we don't go back and undo bad records.
    with open("./output/bad_records.log", "a") as errors_f:
        new_errors = errors.keys() - known_errors
        for i in new_errors:
            errors_f.write(str(i))
            errors_f.write("\n")


if __name__ == "__main__": 
    log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <yellow>Line {line: >4} ({file}):</yellow> <b>{message}</b>"
    logger.add(sys.stderr, level="INFO", format=log_format, colorize=True, backtrace=True, diagnose=True)
    log_file_identifier = datetime.datetime.now().strftime('%m-%d-%Y_%H-%M-%S')
    print(log_file_identifier)
    logger.add(f"./output/run_{log_file_identifier}.log", level="DEBUG", format=log_format, colorize=False, backtrace=True, diagnose=True)

    pl.Config.set_tbl_rows(100)
    import time
    start = time.time()
    main()
    logger.info(f"Took {datetime.timedelta(seconds=time.time() - start)}")
