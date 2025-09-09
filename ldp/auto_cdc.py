from dlt import *
from pyspark.sql.functions import *





dlt.create_streaming_table(
  name = "users_cdc",
  )

@append_flow(
  target = "users_cdc",
  name = "users_cdc_flow"
)
def users_cdc_flow():
  return (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("sep",",")
    .load("/Volumes/jpg/default/csvs_cdc"))


dlt.create_streaming_table(name="users")

dlt.create_auto_cdc_flow(
  target="users",  # The customer table being materialized
  source="users_cdc",  # the incoming CDC
  keys=["id"],  # what we'll be using to match the rows to upsert
  sequence_by=col("operation_date"),  # de-duplicate by operation date, getting the most recent value
  ignore_null_updates=False,
  apply_as_deletes=expr("operation = 'DELETE'"),  # DELETE condition
  except_column_list=["operation", "operation_date", "_rescued_data"],
)