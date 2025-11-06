from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, IntegerType


@udf(returnType=IntegerType())
def add_one(number):
    return number +1
    
def get_rules_as_list_of_dict():
  return [
    {
      "name": "valid_positive_id",
      "constraint": "id > 0",
      "tag": "validity"
    },
    {
      "name": "valid_date",
      "constraint": "date < '2020-01-01'",
      "tag": "validity"
    }
  ]

def get_rules(tag):
  """
    loads data quality rules from a table
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  return {
    row['name']: row['constraint']
    for row in get_rules_as_list_of_dict()
    if row['tag'] == tag
  }