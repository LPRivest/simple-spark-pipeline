from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, isnan
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from datetime import datetime
import os
import yaml

# Load config
with open("/app/config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Set the output file name
current_date = datetime.today().strftime('%Y%m%d')

# Prepare the dataframe for the standard zone
def process_for_standard(spark_session, input_path, output_path):

    expected_schema = StructType([
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("usage_kwh", DoubleType(), nullable=True),
        StructField("start_time", StringType(), nullable=False)
    ])

    df = spark_session.read \
        .option("header", True) \
        .schema(expected_schema) \
        .option("mode", "PERMISSIVE") \
        .csv(input_path)

    df = df.withColumnRenamed("name", "customer_name") \
            .withColumn("received_at", current_timestamp())

    print("Standard zone data frame:")
    df.show(truncate=False)
    df.write.mode("overwrite").parquet(output_path)

    return

# Prepare the dataframe for the consumption zone
# If a record contains invalid records, send it to rejection zone
def process_for_consumption(spark_session, input_path, output_path, rejection_path):
    df = spark_session.read.option("header", True).parquet(input_path)

    # Set condition on which records will be considered errors
    invalid_condition = (
        col("customer_id").isNull() | col("usage_kwh").isNull() | isnan(col("usage_kwh"))
    )

    # Separate invalid records
    invalid_rows = df.filter(invalid_condition)

    if invalid_rows.count() > 0:
        # For debugging purposes only
        print("Rejecton zone data frame:")
        invalid_rows.show(truncate=False)

        invalid_rows.write.mode("overwrite").parquet(rejection_path)

    # Process valid records
    df = df.filter(~(invalid_condition)) \
            .groupBy("customer_id") \
            .sum("usage_kwh") \
            .withColumnRenamed("sum(usage_kwh)", "daily_usage_kwh")

    # For debugging purposes only
    print("Consumption zone data frame:")
    df.show(truncate=False)

    df.write.mode("overwrite").parquet(output_path)

    return

# Initiate pipeline, processing file from raw zone through to consumption zone
def main():
    spark_session = SparkSession.builder.appName(config["app_name"]).getOrCreate()

    # Build input/output paths from config
    input_path = os.path.join(config["paths"]["raw_zone"], config["paths"]["input_file"])
    standard_path = os.path.join(config["paths"]["standard_zone"], config["file_name"], current_date)
    consumption_path = os.path.join(config["paths"]["consumption_zone"], config["file_name"], current_date)
    rejection_path = config["paths"]["rejection_zone"]

    # For logging purposes only
    print("Raw file received:")
    raw_df = spark_session.read.option("header", True).csv(input_path)
    raw_df.show(truncate=False)

    # Process and create parquet file in standard zone
    process_for_standard(spark_session, input_path, standard_path)
    print(f"File written to {standard_path}.")

    # Process and create parquet file in consumption zone
    process_for_consumption(spark_session, standard_path, consumption_path, rejection_path)
    print(f"File written to {consumption_path}.")

    print(f"Pipeline finished.")
    spark_session.stop()

if __name__ == "__main__":
    main()