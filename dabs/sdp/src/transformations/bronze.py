from pyspark import pipelines as dp
from fake_datasource import FakeDataSource
# Register the custom DataSource from the wheel package
spark.dataSource.register(FakeDataSource)


@dp.materialized_view(
    table_properties={
        "quality": "bronze",
        "layer": "landing",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dp.expect("valid_id", "id < 20")
def fake_data_table():
    return spark.read.format("fake").option("numRows", 100).load()


@dp.table
def fake_data_table_stream():
    return spark.readStream.format("fake").option("numRows", 10).load()


@dp.materialized_view
def fake_static_table():
    """Static reference data with 10 rows"""
    data = [
        {"id": 1, "value_spent": 1500},
        {"id": 2, "value_spent": 2300},
        {"id": 3, "value_spent": 890},
        {"id": 4, "value_spent": 4500},
        {"id": 5, "value_spent": 3200},
        {"id": 6, "value_spent": 1750},
        {"id": 7, "value_spent": 2100},
        {"id": 8, "value_spent": 3600},
        {"id": 9, "value_spent": 5200},
        {"id": 10, "value_spent": 920},
    ]
    return spark.createDataFrame(data)
