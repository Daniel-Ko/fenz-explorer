import sys
import asyncio
import httpx
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
import polars as pl

from loguru import logger

from api_load import fetch_all_with_ids

SEMAPHORE_LIMIT = 10

log_level = "DEBUG"
log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS zz}</green> | <level>{level: <8}</level> | <yellow>Line {line: >4} ({file}):</yellow> <b>{message}</b>"
logger.add(sys.stderr, level=log_level, format=log_format, colorize=True, backtrace=True, diagnose=True)
logger.add("./output/batch_api.log", level=log_level, format=log_format, colorize=False, backtrace=True, diagnose=True)

def config_client(): 
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Connection": "close"
    }

    client = httpx.AsyncClient(headers=headers)
    return client

def main():
#    id_range = range(1, 1422)
    id_range = range(1, 50)
    
    known_errors = set()
    with open("./output/bad_records.log", "r") as known_error_ids:
        for id_num in known_error_ids:
            known_errors.add(int(id_num))


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
    
    df.write_parquet("./output/initial_load.parquet")
    
    with open("./output/bad_records.log", "w+") as error_f:
        for i in errors.keys():
            error_f.write(str(i))
            error_f.write("\n")


if __name__ == "__main__":
    pl.Config.set_tbl_rows(100)
    import time
    start = time.time()
    main()
    print(time.time() - start)
