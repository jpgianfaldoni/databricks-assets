# Datadog Integration Init Scripts

## Setup Instructions

### 1. Configure Your Datadog API Key

Edit `datadog_install.sh` and replace `<YOUR_DATADOG_API_KEY>` with your actual Datadog API key:

```bash
DD_API_KEY="your_actual_api_key_here"
```

You can find your API key in Datadog:
- Navigate to **Organization Settings** > **API Keys**

### 2. Update Datadog Site (if needed)

If you're not using the US1 site (datadoghq.com), update the `DD_SITE` variable:

- **US3**: `us3.datadoghq.com`
- **US5**: `us5.datadoghq.com`
- **EU**: `datadoghq.eu`
- **AP1**: `ap1.datadoghq.com`

### 3. Upload Script to Databricks Workspace

The init script needs to be in your Databricks workspace. You have two options:

#### Option A: Upload via Databricks CLI
```bash
# From the databricks-assets directory
databricks workspace upload dabs/job_example/scripts/datadog_install.sh \
  /Workspace/Users/$(databricks current-user me --output json | jq -r .userName)/databricks-assets/scripts/datadog_install.sh
```

#### Option B: Upload via Databricks UI
1. Go to your Databricks workspace
2. Navigate to **Workspace** > **Users** > **Your Username**
3. Create a folder called `databricks-assets/scripts`
4. Click **Import** and upload `datadog_install.sh`

### 4. Deploy Your Bundle

```bash
cd /Users/joao.gianfaldoni/Documents/databricks-assets/dabs/job_example
databricks bundle deploy --target dev
```

### 5. Verify Installation

After running a job with the init script:

1. Check cluster logs:
   - Go to **Compute** in Databricks
   - Click on your cluster
   - Go to **Event log** tab
   - Look for init script execution logs

2. Check Datadog:
   - Wait 5-10 minutes for data to appear
   - Go to **Infrastructure** > **Host Map** in Datadog
   - Search for your Databricks cluster tags

## Using Global Init Scripts Instead

If you want ALL clusters to have Datadog monitoring:

1. Go to **Settings** > **Compute** > **Global init scripts** in Databricks
2. Click **Add** 
3. Name it "datadog-monitoring"
4. Paste the contents of `datadog_install.sh`
5. Click **Add**

**Important**: Global init scripts are stored by Databricks and cannot be viewed as files in your workspace. They're managed entirely through the UI.

## Troubleshooting

### Init Script Fails
- Check cluster event logs for specific error messages
- Ensure the script has proper permissions
- Verify the workspace path is correct

### No Metrics in Datadog
- Verify API key is correct
- Check Datadog site parameter
- Ensure outbound internet access from clusters
- Wait 10-15 minutes for initial data

### Logs Not Appearing
- Verify `logs_enabled: true` in the script
- Check that Spark UI is accessible on port 4040
- Ensure log collection is enabled in Datadog organization settings

