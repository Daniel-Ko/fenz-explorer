import sys
import asyncio
import httpx
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
import polars as pl

from loguru import logger

SEMAPHORE_LIMIT = 10

bad_records = {}

log_level = "DEBUG"
log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS zz}</green> | <level>{level: <8}</level> | <yellow>Line {line: >4} ({file}):</yellow> <b>{message}</b>"
logger.add(sys.stderr, level=log_level, format=log_format, colorize=True, backtrace=True, diagnose=True)
logger.add("./output/batch_api.log", level=log_level, format=log_format, colorize=False, backtrace=True, diagnose=True)

class ApiResponseError(Exception):
    """Custom exception, allowing tenacity's retry to identify when to activate """
    pass

@retry(
    stop=stop_after_attempt(5)
    ,wait=wait_random_exponential(min=1, max=10)
    ,retry=retry_if_exception_type(ApiResponseError)
)
async def fetch_id(client, id_num, endpoint, semaphore):
    """Making an async get request, but with a lot of rate-limit handling and response managing.
    Upon receiving a 400/500 class error, makes 5 attempts with exponential random backoff.
    """
    async with semaphore:
        try:
            response = await client.get(endpoint, timeout=10.0)

            if response.status_code in (429, 500, 502, 503, 504, 508):
                logger.debug(f"Error {response.status_code} with id:{id_num}. Retrying")
                
                await asyncio.sleep(0.1)
                raise ApiResponseError()

            elif response.status_code == 200:
                await asyncio.sleep(0.1)

            return response

        except httpx.RequestError as e:
            logger.exception(f"Network Error - {id_num}: {endpoint}, {e}")
            raise ApiResponseError()

        except Exception as e:
            logger.exception(f"Unexpected error - {id_num}: {endpoint}, {e}")
            # Log this id as a genuine error
            bad_records[id_num] = 0
            raise e

async def fetch_all_with_ids(client, base_url, param, id_range, semaphore):
    async with client:
        try:
            tasks = []
            for id in id_range:
                tasks.append(fetch_id(
                    client=client, 
                    id_num=id, 
                    endpoint=f"{base_url}?{param}={id}", 
                    semaphore=semaphore
                ))
        
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            valid_data = []
            for i, rep in enumerate(responses):
                if isinstance(rep, httpx.Response):
                    if rep.status_code == 200:
                        valid_data.append(rep.json())
                    else:
                        logger.debug(f"Error: Status {rep}")
                else:
                    logger.exception(f"Network or Timeout error: {rep}")
            return valid_data, bad_records
        except Exception as e:
            pass
