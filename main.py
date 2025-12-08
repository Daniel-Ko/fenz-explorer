import asyncio
import httpx
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
import polars as pl

from loguru import logger

SEMAPHORE_LIMIT = 50

class ApiResponseError(Exception):
    pass

@retry(
    stop=stop_after_attempt(5)
    ,wait=wait_random_exponential(min=1, max=10)
    ,retry=retry_if_exception_type(ApiResponseError)
)
async def fetch_id(client, id_num, endpoint, semaphore):
    async with semaphore:
        try:
            response = await client.get(endpoint, timeout=10.0)

            if response.status_code in (429, 500, 502, 503, 504, 508):
                logger.debug(f"Error {response.status_code} with id:{id_num}. Retrying")
                raise ApiResponseError()
                #raise httpx.HTTPStatusError(
                #    request=f"{endpoint}",
                #    response=response.status_code,
                #    message="Trouble fetching this id. Retrying."
                #)
            return response
        except httpx.RequestError as e:
            logger.exception(f"Network Error - {id_num}: {endpoint}, {e}")
            raise ApiResponseError()
        except Exception as e:
            logger.exception(f"Unexpected error - {id_num}: {endpoint}, {e}")
            raise e

async def fetch_all_with_ids(base_url, param, id_range):
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    
    client = httpx.AsyncClient()

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
        return valid_data
    finally:
        await client.aclose()

def main():
#    ids = range(1, 1422)
    ids = range(1, 10)
    
    endpoint = "https://api.fencing.org.nz/public/results"
    param = "cmpId"
    data = asyncio.run(
        fetch_all_with_ids(
            base_url="https://api.fencing.org.nz/public/results",
            param=param,
            id_range=ids
    ))
    logger.info(f"Fetched {len(data)} records")

    df = pl.DataFrame(data)

    logger.info(df.head)

if __name__ == "__main__":
    import time
    start = time.time()
    main()
    print(time.time() - start)
