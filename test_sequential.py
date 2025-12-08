import httpx
import polars as pl
from loguru import logger

def main():
#    ids = range(1, 1422)
    id_range = range(1, 25)
    
    data = []

    base_url = "https://api.fencing.org.nz/public/results"
    param = "cmpId"
    for id_num in id_range:
        rep = httpx.get(f"{base_url}?cmpId={id_num}")
        if rep.status_code == 200:
           data.append(rep.json())
        else:
            logger.warning(f"ID {id_num} failed with status {rep.status_code}. {rep.text[:100]}")
            pass
    logger.info(f"Fetched {len(data)} records")
    

    df = pl.DataFrame(data).with_row_index(offset=1)

    print(df)

if __name__ == "__main__":
    import time
    start = time.time()
    main()
    print(time.time() - start)

