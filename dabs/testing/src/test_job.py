# Databricks notebook source
# define a widget with a default value
dbutils.widgets.text("my_param", "default_value")
dbutils.widgets.text("catalog", "default_value")

# get the value
param_value = dbutils.widgets.get("my_param")
catalog = dbutils.widgets.get("catalog")

print(f"My parameter is: {param_value}")

# COMMAND ----------

df = spark.read.table(f"{catalog}.information_schema.catalogs")
display(df)


# COMMAND ----------

