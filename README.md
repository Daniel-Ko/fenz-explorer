This is the repository that will provide a platform to explore and analyse Fencing New Zealand competition and
athletes data.

## Example
Under https://results.fencing.org.nz/view4 (that is, the Results view)
search AFC #3 Perth 2025
The results are displayed.

While the official FENZ site displays results in this manner, there is a lack of
analytics and ease of analytics. I hope to collate the data in a more OLAP
format (lakehouse) so members of the community can contribute their own
exploratory reports.

The data will be exposed as one to do SQL or Python queries in an aggregation.

## Range of the data
From my exploration so far, there are competition records ranging from 1998 to
the present day. As I split the data out, I hope to provide more accurate stats.

Altogether, FENZ has data on around 1400 competitions in its history. How they
record this data is unknown - will need to find an internal expert to ask.
Fencing tourney organisation software has been around for a while but not since
1998. The software I know of is:
- engarde-service
- fencingtimelive (FTL), which only was around perhaps 2010-12 and was not
    adopted in NZ for awhile. It seems to be the NZ standard, even for local
    comps, however.


## Remarks
Fencing NZ is sitting on a lot of data that, while not perfect, can still
provide a good look into its history and be used to look at trends or business
metrics. While the scene in NZ can be called "small" by international standards,
it shouldn't have to also hold the epithets of "out-of-date" or "sleepy." 
Use what resources is available so the regions can be held to account, and
memory will not fade. 
FENZ must understand itself well in order to keep improving.




## What you can expect

There are two parts to the API:
 - competition results (included)
 - athlete profiles (not included yet)
 

1. The scripts to load data from the FENZ results api
2. A terraform instruction to create a S3 bucket
3. A script to load datafiles into this S3 bucket
4. (COMING) Transformations to normalise the schema of the unruly FENZ data
5. (COMING) Publicly accessible reports on this data
6. (COMING) Publicly accessible exploratory environment


## Architecture:
1. API read with Python, chunked as the rate limits of the API are not easy to
   ascertain. Read into parquet files as the schema has also changed across the
   years. There is no easy unified schema available.
2. Load data as-is into S3 to be available to different analyses platforms
3. Currently reading from S3 into databricks to discover what transformations
   can be done to unify the schema.




