import dlt
from pyspark.sql.functions import col


# This file defines Delta Live Tables transformations with environment-specific configuration.
# 
# Configuration variables are passed from databricks.yml via the pipeline configuration:
# - data_source_path: Environment-specific data source location
# - catalog: Target catalog (dev_catalog for dev, main for prod)
# - schema: Target schema (job_example_dev for dev, job_example_prod for prod)
#
# Edit the transformations below or add new ones using "+ Add" in the file browser.


@dlt.table
def test_dabs_table():
    # Get the data source path from pipeline configuration
    
    return (
        spark.readStream.table("system.billing.usage")
    )
