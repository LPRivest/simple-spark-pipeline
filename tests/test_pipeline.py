from src.pipeline import process_for_standard

def test_process_for_standard(spark):
    # Input DataFrame
    data = [
            ("1", "2.0", "2023-05-12T01:00:00-07:00"),
            ("2", "2.0", "2023-05-12T01:00:00-07:00"),
            ("3", "2.0", "2023-05-12T01:00:00-07:00")
        ]

    df = spark.createDataFrame(data, ["customer_id", "usage_kwh", "start_time"])

    result_df = process_for_standard(df)

    result = result_df.collect()

    assert len(result == 1)
    assert result[0]["customer_id"] == 1
    assert result[0]["usage_kwh"] == 2.0