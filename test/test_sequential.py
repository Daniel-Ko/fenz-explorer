import httpx
import polars as pl
from loguru import logger

data = [0]*25

def fetch_all(base_url, id_range) -> list[int]:
    bad_records = []

    for id_num in id_range:
        rep = httpx.get(f"{base_url}?cmpId={id_num}")
        if rep.status_code == 200:
           data[id_num] = rep.json()
        else:
            bad_records.append(id_num)
            logger.warning(f"ID {id_num} failed with status {rep.status_code}.")
            pass
    return bad_records

def main():
#    ids = range(1, 1422)
    id_range = range(1, 25)

    base_url = "https://api.fencing.org.nz/public/results"

    bad_records = fetch_all(base_url, id_range)
    logger.debug(f"BAD RECORDS: {bad_records}")

    # retry bad records a couple of times, hoping the list sorts itself out slowly.
    i = 5
    while i > 0 and bad_records:
        logger.debug(f"{len(bad_records)} bad records found: {bad_records}. Retrying.")
        fetch_all(base_url, iter(bad_records))
        i -= 1

    logger.info(f"Fetched {len(data)-len(bad_records)} records")
    
    pl.Config.set_tbl_rows(100)
    df = pl.DataFrame(data, strict=False).with_row_index(offset=1)

    print(df)

if __name__ == "__main__":
    import time
    start = time.time()
    main()
    print(time.time() - start)

