import argparse
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

def main(args_id_range, output_prefix, test=False):
    # 1422 is max as of Dec 1, 2025
    id_range = set(range(*args_id_range))

    with open("./output/bad_records.log", "a+") as known_errors_f:
        # move to start as append mode starts at the end
        known_errors_f.seek(0)

        # for purposes of using the log as a cache
        known_errors = {int(id_num) for id_num in known_errors_f}
        
        # Exclude the errors already
        valid_id_range = id_range - known_errors

        # Set up for the endpoint. This will be later separated into "athlete" and "results". Right now it is "results"
        endpoint = "https://api.fencing.org.nz/public/results"
        param = "cmpId"
        
        # Run concurrently
        data, errors = asyncio.run(
            fetch_all_with_ids(
                client=config_client(),
                base_url="https://api.fencing.org.nz/public/results",
                param=param,
                id_range=valid_id_range,
                semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT),
                known_errors=known_errors
        ))
        # calculating records fetched
        len_existing_errors = len(id_range)-len(valid_id_range)
        logger.info(f"Total {len(data)+len(errors)+len_existing_errors} fetched. Successful records: {len(data)}. Unsuccessful records: {len(errors)+len_existing_errors}")
        
        # print the data to inspect if a test is needed
        if test:
            import json
            logger.debug(json.dumps(data, indent=2))
        
        for json in data:
            json["events"] = str(json["events"])

        df = pl.DataFrame(data)
        logger.info(df.head)
        
        logger.info("Created dataframe")

        # Save all records done so far
        df.write_parquet(f"./output/{output_prefix}_load_{args_id_range[0]}_{args_id_range[1]}.parquet")
        
        # Save new errors found. Append only - we don't go back and undo bad records.
        new_errors = errors.keys() - known_errors
        logger.info(f"Found {len(new_errors)} new errors")

        for i in new_errors:
            known_errors_f.write(str(i))
            known_errors_f.write("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("range", type=int, nargs=2, help="Provide 2 numbers that set the range of ids you want to cover. [bot, top) ** top is exclusive")
    parser.add_argument("file_prefix", help="Provide a prefix for the parquet file this will produce. Format is: {prefix}_load_bot_top.parquet")
    parser.add_argument("--test", "-t", action="store_true")
    args = parser.parse_args()
    
    logger.remove() # Removing all default sinks
    log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <yellow>Line {line: >4} ({file}):</yellow> <b>{message}</b>"
    logger.add(sys.stderr, level="INFO", format=log_format, colorize=True, backtrace=True, diagnose=True)

    log_file_identifier = datetime.datetime.now().strftime('%m-%d-%Y_%H-%M-%S')
    logger.add(f"./output/runlogs/run_{log_file_identifier}.log", level="DEBUG", format=log_format, colorize=False, backtrace=True, diagnose=True)

    pl.Config.set_tbl_rows(100)
    import time
    start = time.time()
    main(args.range, args.file_prefix, args.test)
    logger.info(f"Took {datetime.timedelta(seconds=time.time() - start)}")
