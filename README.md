# PySpark Pipeline Assignment

## Overview
Simple PySpark pipeline that:
- Reads a CSV (header, inferSchema)
- Drops rows missing required columns
- Optionally filters on a numeric column > threshold
- Writes result as Parquet

## Files
- `src/pipeline.py` - main pipeline (CLI)
- `tests/test_pipeline.py` - unit + integration tests (pytest)
- `requirements.txt` - packages for local runs
- `Dockerfile` - used to run everything in a container (recommended)
- `candidate/` - place your input CSV here (when running in Docker mount)

## How to run (recommended: Docker)
Assuming your project folder is mounted into container at `/app` (see Dockerfile in repo):

**Build image (only once):**
```bash
docker build -t pyspark-env .