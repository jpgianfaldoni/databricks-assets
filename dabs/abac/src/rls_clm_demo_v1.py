# Databricks notebook source
# MAGIC %pip install faker pandas
# MAGIC from faker import Faker
# MAGIC import pandas as pd
# MAGIC import random
# MAGIC import json
# MAGIC

# COMMAND ----------

#set catalog and schema
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Target location: {catalog}.{schema}")


# COMMAND ----------

#generate pii data
fake = Faker()

rows = 10000
data = []
for _ in range(rows):
    data.append({
        "id": _ + 1,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "ssn": fake.ssn(),
        "address": fake.address(),
        "city": fake.city(),
        "state": fake.state(),
        "postal_code": fake.postcode(),
        "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=90),
        "income": round(random.uniform(30000, 150000), 2)
    })

df = pd.DataFrame(data)


# COMMAND ----------

#save table
table_name = f"{catalog}.{schema}.pii_customers_rls_clm_demo"
df_spark = spark.createDataFrame(df)
df_spark.write.format("delta").mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

spark.sql(f"SELECT * FROM {table_name}").display()

# COMMAND ----------

# Create a simple row filter function
filter_name = f"{catalog}.{schema}.state_filter"
spark.sql(f"""
CREATE OR REPLACE FUNCTION {filter_name}(state STRING)
RETURN 
  is_account_group_member('full_access_group')
  OR
  state IN ('Kentucky', 'Texas', 'Indiana')
""")

# COMMAND ----------

# Apply the row filter to the table
spark.sql(f"""
ALTER TABLE {table_name} 
SET ROW FILTER {filter_name} ON (state)
""")

# COMMAND ----------

#Check table after row filter is applied
spark.sql(f"SELECT * FROM {table_name}").display()

# COMMAND ----------

#Remove row filter
spark.sql(f"""
ALTER TABLE {table_name} 
DROP ROW FILTER
""")


# COMMAND ----------

# Create a mapping table for row-level security
mapping_table = f"{catalog}.{schema}.mapping_table_demo"
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {mapping_table} (
  group STRING,
  states ARRAY<STRING>
)
""")

# COMMAND ----------

# Insert sample data into the mapping table
spark.sql(f"""
INSERT INTO {mapping_table} (group, states) VALUES
  ('west', ARRAY('California', 'Oregon', 'Washington')),
  ('east', ARRAY('New York', 'Massachusetts', 'Florida'))
""")

# COMMAND ----------

# Create an advanced row filter function using the mapping table
state_filter_mapping_function = f"{catalog}.{schema}.state_filter_mapping_function"
spark.sql(f"""
CREATE OR REPLACE FUNCTION {state_filter_mapping_function}(state STRING)
RETURN 
  is_account_group_member('full_access_group')
  OR
  EXISTS (
    SELECT 1
    FROM {mapping_table} m
    WHERE 
    array_contains(m.states, state)
    AND is_account_group_member(m.group)
  )
""")

# COMMAND ----------

# Apply the row filter to the table
spark.sql(f"""
ALTER TABLE {table_name} 
SET ROW FILTER {state_filter_mapping_function} ON (state)
""")

# COMMAND ----------

spark.sql(f"SELECT * FROM {table_name}").display()

# COMMAND ----------

#Remove row filter
spark.sql(f"""
ALTER TABLE {table_name} 
DROP ROW FILTER
""")


# COMMAND ----------

# Create a column masking function for email addresses
email_mask_function = f"{catalog}.{schema}.email_mask_function"
spark.sql(f"""
CREATE OR REPLACE FUNCTION {email_mask_function}(email STRING)
RETURN
  IF(
    is_account_group_member('full_access_group'),
    email,
    CONCAT('***@', SPLIT(email, '@')[1])
  )
""")


# COMMAND ----------

# Apply column masking to the email column
spark.sql(f"""
ALTER TABLE {table_name}
ALTER COLUMN email SET MASK {email_mask_function}
""")


# COMMAND ----------

spark.sql(f"SELECT * FROM {table_name}").display()

# COMMAND ----------

# Remove column masking from the email column
spark.sql(f"""
ALTER TABLE {table_name}
ALTER COLUMN email DROP MASK
""")
