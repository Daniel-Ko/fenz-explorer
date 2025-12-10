import argparse
import polars as pl

parser = argparse.ArgumentParser()
parser.add_argument("filename")

args = parser.parse_args()

df = pl.read_parquet(args.filename)
print(df)
