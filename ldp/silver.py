import dlt
from pyspark.sql.functions import col
from utilities import utils
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField




@dlt.table
def silver_table(name = "jpg.default.silver_table"):
  return (spark.readStream.table("jpg.default.csv_ingestion_no_schema"))