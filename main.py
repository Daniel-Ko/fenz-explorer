import asyncio
import httpx
import polars as pl

async def fetch_all(endpoint, ids):
    async with httpx.AsyncClient() as client:
        tasks = []
        for id in ids:
            tasks.append(client.get(f"{endpoint}?cmpId={id}"))
    
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        valid_data = []
        for rep in responses:
            if isinstance(rep, httpx.Response) and rep.status_code == 200:
                valid_data.append(rep.json())
        return valid_data

def main():
    ids = range(1, 1422)

    data = asyncio.run(fetch_all("https://api.fencing.org.nz/public/results",
    ids))
    print(f"Fetched {len(data)} records")

    df = pl.DataFrame(data)

    print(df.head)

if __name__ == "__main__":
    main()
