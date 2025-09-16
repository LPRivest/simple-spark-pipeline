# Simple Spark Pipeline

## Overview
Simple data pipeline that uses Pyspark and Python to clean, validate, and aggregate a sample utility file.

## Requirements
1) Docker for desktop (see installation details here: https://docs.docker.com/desktop/)
2) Git 
3) A viewing tool for Parquet files (I used this one: https://dataconverter.io/view/parquet/)

If using windows, also include:
4) Windows Subsystem for Linux (WSL) (see details here: https://learn.microsoft.com/en-us/windows/wsl/install)
5) local Ubuntu Virtual machine (see details here: https://ubuntu.com/tutorials/how-to-run-ubuntu-desktop-on-a-virtual-machine-using-virtualbox#1-overview)
6) Bash 

## Process
- Expects a CSV file with a header and with a specific schema
- Drops rows with invalid records
- Aggregates remaining rows
- Writes result as Parquet into 2 zones

## Structure
- `src/pipeline.py` - main pipeline
- `tests/test_pipeline.py` - unit + integration tests
- `requirements.txt` - packages for local runs (packages should also be reflected in the Dockerfile)
- `Dockerfile` - used to run everything in a container
- `config.yaml` - contains paths and names for reusability
- `/data/raw_zone/` - Folder where the input file should be set
- `/data/standard_zone/` - Folder where the input file lands after initial processing
- `/data/consumption_zone/` - Folder where the input file lands after secondary processing
- `/data/rejection_zone/` - Folder where invalid records are rejected

## How to run
Assuming project folder is mounted into container at `/app` (see Dockerfile):

**Build image (only once):**
```bash
docker build -t pyspark-env .
```

**Run data pipeline app:**
```bash
docker run -it --rm -v "${PWD}:/app" pyspark-env python /app/src/pipeline.py
```

**Run test cases:**
```bash
docker run -it --rm -v ${PWD}:/app pyspark-env pytest -v
```

## Methodology: 

**Approach and key decisions**
I wanted to emulate the medallion architecture I used in previous workplaces. This is why I set it up as the raw zone, the standard zone and the consumption zone.
The raw zone acts as the destination for the source file, whether it's loaded from an SFTP or a different process. Files should be unprocessed and history should be maintained.
The standard zone contains records with errors removed, schema enforced, and an additional column "received_at" which tracks when a file was received. This will be important for error tracing.
The consumption zone contains the cleaned, aggregated records, with timestamp fields reformatted.
There is also a rejection zone, where error records caught in the standard zone are quarantined.

In terms of the final result, I noticed that a customer may have multiple records, for example readings at different hours. I wasn't sure how the user would prefer to have this impact the start_time field. Should it be kept as is? Keep only the earliest? Change the timestamp to a date instead?

**Two risks to reliability at scale**
1)
2)

**Next steps for production readiness**
1) Write consumption zone data to an table that can be queried like in Databricks
2) Check with end user that the consumption zone record has the correct schema, correct values
3) Remove all Pyspark actions used for logging to improve performance
4) Change hardcoded file name to a reuseable one
5) Determine and add merge logic into consumption zone (rather than keeping history like in RW and SZ)

**Data quality considerations**
1) Some of the data clean up steps assume that non-null data is valid. So an incorrectly formatted timestamp value, or a field that fails to progress through the clean up steps, might break the logic. This would need to be handled at the row level.
2) 

**CI/CD considerations**

**Metrics considerations**
1) Query performance on the consumption zone records
2) Pipeline processing performance (amount of time/resources required to run)
3) Usage (kwh) data comparison to a reference table