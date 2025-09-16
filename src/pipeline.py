from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_timestamp, col, isnan
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
def process_for_standard(spark_session, input_path, output_path, rejection_path):

    # Set the expected schema of the incoming file
    expected_schema = StructType([
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("usage_kwh", DoubleType(), nullable=True),
        StructField("start_time", StringType(), nullable=False)
    ])

    # Set condition on which records will be considered errors
    invalid_condition = (
        col("customer_id").isNull() | col("usage_kwh").isNull() | isnan(col("usage_kwh"))
    )

    # Read incoming csv file
    df = spark_session.read \
        .option("header", True) \
        .schema(expected_schema) \
        .option("mode", "PERMISSIVE") \
        .csv(input_path)
    
    # Separate invalid records
    invalid_rows = df.filter(invalid_condition)

    # If any invalid records are found, reject them
    if invalid_rows.count() > 0:
        # For debugging purposes only
        print("Rejection zone data frame:")
        invalid_rows.show(truncate=False)

        invalid_rows.write.mode("overwrite").parquet(rejection_path)

    # Keep valid records and add a timestamp column for traceability
    df = df.filter(~(invalid_condition)) \
            .withColumn("received_at", current_timestamp())

    # For debugging purposes only
    print("Standard zone data frame:")
    df.show(truncate=False)

    # Write to standard zone
    df.write.mode("overwrite").parquet(output_path)

    return

# Prepare the dataframe for the consumption zone
# If a record contains invalid records, send it to rejection zone
def process_for_consumption(spark_session, input_path, output_path):
    df = spark_session.read.option("header", True).parquet(input_path)

    df = df.withColumn("start_time_utc", to_timestamp(col("start_time"), "yyyy-MM-dd'T'HH:mm:ssXXX"))

    # Calculate daily usage for each customer
    df_aggregated = df.groupBy("customer_id") \
                        .sum("usage_kwh") \
                        .withColumnRenamed("sum(usage_kwh)", "daily_usage_kwh")
    
    # Add columns excluded from aggregation back to dataframe
    df = df.join(df_aggregated,"customer_id", "left") \
            .select(
                df.customer_id,
                df.usage_kwh,
                df_aggregated.daily_usage_kwh,
                df.start_time_utc,
                df.received_at
            )

    # For debugging purposes only
    print("Consumption zone data frame:")
    df.show(truncate=False)

    # Write to consumption zone
    df.write.mode("overwrite").parquet(output_path)

    return

# Initiate pipeline, processing file from raw zone through standard zone and finally to consumption zone
def main():
    spark_session = SparkSession.builder.appName(config["names"]["core_app"]).getOrCreate()

    # Build input/output paths from config
    input_path = os.path.join(config["paths"]["raw_zone"], config["names"]["input_file"])
    standard_path = os.path.join(config["paths"]["standard_zone"], config["names"]["file_prefix"], current_date)
    consumption_path = os.path.join(config["paths"]["consumption_zone"], config["names"]["file_prefix"], current_date)
    rejection_path = config["paths"]["rejection_zone"]

    # For logging purposes only
    print("Raw file received:")
    raw_df = spark_session.read.option("header", True).csv(input_path)
    raw_df.show(truncate=False)

    # Process and create parquet file in standard zone
    process_for_standard(spark_session, input_path, standard_path, rejection_path)
    print(f"File written to {standard_path}.")

    # Process and create parquet file in consumption zone
    process_for_consumption(spark_session, standard_path, consumption_path)
    print(f"File written to {consumption_path}.")

    print(f"Pipeline finished.")
    spark_session.stop()

if __name__ == "__main__":
    main()