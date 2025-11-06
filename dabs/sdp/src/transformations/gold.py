from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utilities.utils import add_one, get_rules_as_list_of_dict, get_rules


@dp.materialized_view
@dp.expect_all(get_rules('validity'))
def fake_data_table_gold():
    silver_df = spark.read.table('fake_data_table_silver')
    static_df = spark.read.table('fake_static_table')
    joined_df = silver_df.join(static_df, on="id")
    return joined_df.withColumn("id_plus_one", add_one(joined_df["id"]))
