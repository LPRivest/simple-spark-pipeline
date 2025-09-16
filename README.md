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
- `requirements.txt` - packages for local runs
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

**Run app:**
```bash
docker run -it --rm -v "${PWD}:/app" pyspark-env python /app/src/pipeline.py
```

## Methodology: 

**Approach and key decisions**
TBA

**Two risks to reliability at scale**
TBA

**Next steps for production readiness**
TBA

**Data quality considerations**

**CI/CD considerations**

**Metrics considerations**