import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")  # 1 core for testing
        .appName("pytest_usage_pipeline")
        .getOrCreate()
    )

    yield spark
    spark.stop()