import dlt
from pyspark.sql.functions import col
from utilities import utils
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField

schema = StructType(
  [
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
  ]
)


@dlt.table
def csv_ingestion_no_schema(name = "jpg.default.csv_ingestion_no_schema"):
  return (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("sep",",")
    .load("/Volumes/jpg/default/csvs"))

@dlt.table
def csv_ingestion_with_schema(name = "jpg.default.csv_ingestion_with_schema"):
  return (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .schema(schema)
    .option("sep",",")
    .load("/Volumes/jpg/default/csvs"))
  
@dlt.table
def csv_ingestion_with_schema_hint(name = "jpg.default.csv_ingestion_schema_hint"):
  return (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaHints", "id int, name string, age int")
    .option("sep",",")
    .load("/Volumes/jpg/default/csvs"))
  
