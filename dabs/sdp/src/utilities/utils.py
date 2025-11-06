from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, IntegerType


@udf(returnType=IntegerType())
def add_one(number):
    return number +1
