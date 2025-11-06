from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view
def fake_data_table_silver():
    df = spark.read.table("fake_data_table")
    # Convert 'date' column to date type
    df = df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
    # Remove dots from the 'name' column values
    df = df.withColumn("name", F.regexp_replace(F.col("name"), r"\.", ""))
    return df


@dp.table
def fake_data_table_stream_silver():
    return spark.readStream.table("fake_data_table_stream").withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))