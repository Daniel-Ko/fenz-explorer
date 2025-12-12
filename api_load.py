import sys
import asyncio
import httpx
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type, RetryError
import polars as pl

from loguru import logger

SEMAPHORE_LIMIT = 10

bad_records = {}


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
            logger.debug(f"Network Error - {id_num}: {endpoint}, {e}")
            raise ApiResponseError()
        except RetryError as e:
            logger.debug("Last retry for id: {id_num}")
        except Exception as e:
            logger.debug(f"Unexpected error - {id_num}: {endpoint}, {e}")
            # Log this id as a genuine error
            bad_records[id_num] = 0
            return response

async def fetch_all_with_ids(client, base_url, param, id_range, semaphore, known_errors):
    async with client:
        try:
            tasks = []
            for id_num in id_range:
                # skip errors encountered in previous loads
                if id_num not in known_errors:
                    tasks.append(fetch_id(
                        client=client, 
                        id_num=id_num, 
                        endpoint=f"{base_url}?{param}={id_num}", 
                        semaphore=semaphore
                    ))
                else:
                    # skipping a previously logged bad record
                    logger.info(f"Skipping id {id_num}. Previously logged as bad record")
                    bad_records[id] = 0
        
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            valid_data = []
            for i, rep in enumerate(responses):
                if isinstance(rep, httpx.Response):
                    if rep.status_code == 200:
                        data = rep.json()
                        valid_data.append(rep.json())
                    else:
                        logger.debug(f"Error: Status {rep}")
                else:
                    logger.debug(f"Network or Timeout error: {rep}")
            return valid_data, bad_records
        except Exception as e:
            pass
