import asyncio
import httpx
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
import polars as pl

from loguru import logger

SEMAPHORE_LIMIT = 1

class ApiTempError(Exception):
    pass

@retry(
    stop=stop_after_attempt(5)
    ,wait=wait_random_exponential(min=1, max=10)
    ,retry=retry_if_exception_type(httpx.RequestError)
)
async def fetch_id(client, id_num, endpoint, semaphore):
    async with semaphore:
        try:
            response = await client.get(endpoint, timeout=10.0)

            # if response.status_code in (429, 500, 502, 503, 504, 508):
            #    raise ApiTempError(f"{endpoint}")
            return response
        except httpx.RequestError as e:
            logger.exception(f"Network Error - {id_num}: {endpoint}, {e}")
            raise e
        except Exception as e:
            logger.exception(f"Unexpected error - {id_num}: {endpoint}, {e}")
            return e

async def fetch_all_with_ids(endpoint, param, ids):
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    async with httpx.AsyncClient() as client:
        tasks = []
        for id in ids:
            tasks.append(fetch_id(
                client=client, 
                id_num=id, 
                endpoint=f"{endpoint}?{param}={id}", 
                semaphore=semaphore
            ))
    
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        valid_data = []
        for i, rep in enumerate(responses):
            if isinstance(rep, httpx.Response):
                if rep.status_code == 200:
                    valid_data.append(rep.json())
                else:
                    logger.debug(f"Error: Status {rep.status_code} for id {i}")
            else:
                logger.exception(f"Network or Timeout error: {rep.status_code} for id {i}")
        return valid_data

def main():
#    ids = range(1, 1422)
    ids = range(1, 100)
    
    endpoint = "https://api.fencing.org.nz/public/results"
    param = "cmpId"
    data = asyncio.run(fetch_all_with_ids("https://api.fencing.org.nz/public/results",param,ids))
    logger.info(f"Fetched {len(data)} records")

    df = pl.DataFrame(data)

    logger.info(df.head)

if __name__ == "__main__":
    import time
    start = time.time()
    main()
    print(time.time() - start)
