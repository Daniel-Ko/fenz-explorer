import argparse
import json
import polars as pl 

parser = argparse.ArgumentParser()
parser.add_argument("filename")
parser.add_argument("--schema", action="store_true")
args = parser.parse_args()

df = pl.read_parquet(args.filename)
if args.schema:
    print(json.dumps(df.schema, indent=2))
else:
    print(df)
