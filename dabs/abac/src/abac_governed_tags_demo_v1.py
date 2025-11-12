# Databricks notebook source
# MAGIC %md
# MAGIC # Attribute-Based Access Control (ABAC)
# MAGIC ### What it is?
# MAGIC
# MAGIC ###  Data governance model that uses tags, policies and UDFs to enforce RLS and CLM
# MAGIC - **Complements Unity Catalog's privilege model**
# MAGIC - **Flexible and scalable: Define policies once, apply across many assets**

# COMMAND ----------

# MAGIC %md
# MAGIC # ABAC Architecture
# MAGIC ### 1. Governed Tags
# MAGIC   - Account-level tags 
# MAGIC   - Built-in rules for consistency and control
# MAGIC   - Automatically inherited 
# MAGIC ### 2. User-Defined Functions (UDFs)
# MAGIC   - Custom functions
# MAGIC   - Registered and governed in UC
# MAGIC ### 3. Policies
# MAGIC   - Rules that automatically enforces how data is accessed based on tags and a UDF
# MAGIC   - Row filter
# MAGIC   - Column mask
# MAGIC   - Catalog, Schema or Table level (automatically inherited )
# MAGIC

# COMMAND ----------

# MAGIC %pip install faker pandas
# MAGIC from faker import Faker
# MAGIC import pandas as pd
# MAGIC import random
# MAGIC from databricks.sdk import WorkspaceClient
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
table_name = f"{catalog}.{schema}.pii_customers"
df_spark = spark.createDataFrame(df)
df_spark.write.format("delta").mode("overwrite").saveAsTable(table_name)


# COMMAND ----------

spark.sql(f"SELECT * FROM {table_name}").display()

# COMMAND ----------

# Create a column masking function for email addresses
mask_function = f"{catalog}.{schema}.email_column_mask"
spark.sql(f"""
CREATE OR REPLACE FUNCTION {mask_function}(email STRING)
RETURN
    CONCAT('***@', SPLIT(email, '@')[1])
""")

# COMMAND ----------

# Create governed tag
w = WorkspaceClient()
allowed_values = ["CONFIDENTIAL", "PII", "HIGHLYSENSITIVE", "P", "GB", "HP", "PHI"]
tag_key = "table_sensitivity"
tag_payload = {
    'description' : 'Governed tag to indentify sensitive data on tables',
    'tag_key' : f'{tag_key}',
    "values": [{"name": v} for v in allowed_values]
}


# Delete governed tag if exists
try:
    w.api_client.do(
        "DELETE",
        f"/api/2.1/tag-policies/{tag_key}"
    )
    print(f"Deleted existing tag: {tag_key}")
except Exception as e:
    if "not found" in str(e) or "RESOURCE_DOES_NOT_EXIST" in str(e) or "NotFound" in str(e):
        print(f"No existing tag to delete: {policy_name}")
    else:
        raise

# Create policy 
print(f"Creating new tag: {tag_key}")
w.api_client.do(
    "POST",
    "/api/2.1/tag-policies",
    data=json.dumps(tag_payload)
)

# COMMAND ----------

# Create ABAC policy
securable_type = "CATALOG"
policy_name = "email_masking_policy"

policy_payload={
 'on_securable_type': 'CATALOG',
 'on_securable_fullname': f'{catalog}',
 'name': 'email_masking_policy',
 'comment': 'Masks email addresses',
 'to_principals': ['account users'],
 'except_principals': [],
 'for_securable_type': 'TABLE',
 'policy_type': 'POLICY_TYPE_COLUMN_MASK',
"when_condition": "hasTagValue('table_sensitivity','PII')",
 'column_mask': {'function_name': f'{mask_function}',
  'on_column': 'email'},
 'match_columns': [{'condition': "hasTag('class.email_address')", 'alias': 'email'}]}


# Delete policy if exists
try:
    w.api_client.do(
        "DELETE",
        f"/api/2.1/unity-catalog/policies/{securable_type}/{catalog}/{policy_name}"
    )
    print(f"Deleted existing policy: {policy_name}")
except Exception as e:
    if "does not exist" in str(e) or "RESOURCE_DOES_NOT_EXIST" in str(e) or "NotFound" in str(e):
        print(f"No existing policy to delete: {policy_name}")
    else:
        raise

# Create policy 
print(f"Creating new policy: {policy_name}")
w.api_client.do(
    "POST",
    "/api/2.1/unity-catalog/policies/",
    data=json.dumps(policy_payload)
)

     

# COMMAND ----------

# Set table and column tags
spark.sql(
    f"""ALTER TABLE {table_name}
ALTER COLUMN email
SET TAGS ('class.email_address' = '')"""
);

spark.sql(
    f"""ALTER TABLE {table_name}
SET TAGS ('table_sensitivity' = 'PII')"""
);

# COMMAND ----------

#Display table to show masking
spark.sql(f"SELECT * FROM {table_name}").display()