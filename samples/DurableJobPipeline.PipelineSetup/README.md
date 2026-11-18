# Pipeline setup executable

Pipeline setup is deliberately outside the Function App DI container. Run this executable once per environment (or from CI/CD) to persist the pipeline definitions.

Local/Azurite:

```bash
export PIPELINE_STORAGE_CONNECTION_STRING='UseDevelopmentStorage=true'
dotnet run --project samples/DurableJobPipeline.PipelineSetup
```

Azure with managed identity:

```bash
az login
export PIPELINE_TABLE_SERVICE_URI='https://<storage-account>.table.core.windows.net'
dotnet run --project samples/DurableJobPipeline.PipelineSetup
```

This tool is expected to be run by a deployment identity/user or a CI agent that has `Storage Table Data Contributor` access to the pipeline tables.
