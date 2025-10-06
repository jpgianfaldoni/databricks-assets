#!/bin/bash

# Datadog Agent Installation Script for Databricks
# This script installs and configures the Datadog Agent on Databricks cluster nodes

# Exit on error
set -e

echo "Starting Datadog Agent installation..."

# Install Datadog Agent
DD_API_KEY="<YOUR_DATADOG_API_KEY>" \
DD_SITE="datadoghq.com" \
bash -c "$(curl -L https://s3.amazonaws.com/dd-agent/scripts/install_script_agent7.sh)"

# Configure Datadog Agent for Databricks
cat <<EOF > /etc/datadog-agent/conf.d/spark.d/conf.yaml
init_config:

instances:
  - spark_url: http://localhost:4040
    spark_cluster_mode: spark_standalone_mode
    cluster_name: ${DB_CLUSTER_NAME:-databricks-cluster}
EOF

# Configure log collection
cat <<EOF >> /etc/datadog-agent/datadog.yaml

logs_enabled: true
logs_config:
  container_collect_all: true
EOF

# Tag the cluster
cat <<EOF >> /etc/datadog-agent/datadog.yaml

tags:
  - databricks_workspace:$(echo $DB_WORKSPACE_URL | sed 's/https:\/\///')
  - cluster_id:$DB_CLUSTER_ID
  - cluster_name:$DB_CLUSTER_NAME
  - env:${DATABRICKS_ENV:-development}
EOF

# Restart Datadog Agent
sudo systemctl restart datadog-agent

echo "Datadog Agent installation completed successfully!"

