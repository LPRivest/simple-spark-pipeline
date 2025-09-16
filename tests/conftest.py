import sys
import os
import pytest
from pyspark.sql import SparkSession

# --- Add project root (one directory above /tests) ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")  # 1 core for testing
        .appName("pytest-usage-pipeline")
        .getOrCreate()
    )

    yield spark
    spark.stop()