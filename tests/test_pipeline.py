from src.pipeline import update_timestamp_col
from datetime import datetime

def test_timestamp_update(spark):
    # Test DataFrame
    data = [
            ("123", "5.1", "2023-05-12T01:00:00-07:00"),
            ("456", "4.3", "2023-05-11T01:30:00-07:00"),
            ("789", "0", "2023-05-11T23:00:00-07:00"),
            ("111", "0.1", "")
        ]

    df = spark.createDataFrame(data, ["customer_id", "usage_kwh", "start_time"])

    result_df = update_timestamp_col(df)

    result = result_df.collect()

    assert result[0]["start_time_utc"] == datetime(2023, 5, 12, 8, 0, 0)
    assert result[1]["start_time_utc"] == datetime(2023, 5, 11, 8, 30, 0)
    assert result[2]["start_time_utc"] == datetime(2023, 5, 12, 6, 0, 0)
    assert result[3]["start_time_utc"] is None
    # assert result[4]["start_time_utc"] is None
    # assert result[5]["start_time_utc"] == datetime(2023, 5, 12, 8, 0, 0)