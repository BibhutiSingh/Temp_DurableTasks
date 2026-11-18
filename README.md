# DurableJobPipeline

A reusable C# pipeline/job orchestration library built around Durable Task concepts.

## Solution structure

```text
src/
  DurableJobPipeline.Core/
      Pipeline model, DAG builder, storage abstractions, job handlers, retry/capacity options
  DurableJobPipeline.DurableTask/
      Self-hosted Durable Task orchestrator, activities, starter
  DurableJobPipeline.AzureFunctions/
      Azure Functions .NET isolated adapters (orchestrator, activities, HTTP + Queue triggers)
  DurableJobPipeline.AzureStorage/
      Azure Table Storage pipeline/run/job metadata + Azure Blob job output store
  DurableJobPipeline.AzureStorageQueue/
      Queue producer abstraction
  DurableJobPipeline.Sqlite/
      Local/dev storage provider

samples/
  DurableJobPipeline.Functions/
      Azure Functions .NET 8 isolated sample
  DurableJobPipeline.PipelineSetup/
      Explicit setup executable; pipeline setup is deliberately outside DI

tests/
  DurableJobPipeline.Tests/

infra/
  main.bicep
  main.bicepparam.example
  deploy.sh
```

## Design

Pipeline setup is explicit application code:

```csharp
var setup = new PipelineSetup(storage);
await setup.SetupPipeline("DataImport", pipeline =>
{
    pipeline.ShortCircuitOnError();

    var extract = pipeline.AddJob<ExtractJob>("Extract");
    pipeline.AddJob<TransformJob>("Transform", extract);
    pipeline.AddJob<IndependentJob>("Independent");
});
```

The stored definition contains job type keys and dependency ids. It does not serialize delegates or DI registrations.

At runtime, DI is only used to resolve application services and the registered `IJobHandler` implementation.

## Dependency behavior

Independent jobs are scheduled in parallel. A job is eligible only when every dependency has completed successfully.

With `ShortCircuitOnError()`, a failed job stops new work from being scheduled and remaining pending jobs are marked `Skipped`.

Without short-circuiting, unrelated branches continue. Dependents of failed/canceled jobs are skipped.

## Restart

A restart creates a new durable orchestration instance. Previously completed jobs can be reused. If a restart-from-job is supplied, that job and all of its descendants are rerun.

The pipeline run stores its original parameters so a restart can use the same input.

## Cancellation

`CancelPipeline` persists a cancellation request and terminates the durable orchestration. A currently executing activity is not forcibly killed by Durable Functions; application job code should always honor the `CancellationToken` passed to the activity. This is cooperative cancellation by design.

## Azure Functions isolated sample

The Functions sample uses:

- .NET 8 isolated worker
- Durable Functions extension for isolated worker
- HTTP trigger to start/get/cancel/restart runs
- Azure Storage Queue trigger to start runs
- Azure Table Storage for pipeline definitions and run/job state
- Azure Blob Storage for large job output
- Application Insights with Microsoft Entra ID authentication
- Flex Consumption Bicep infrastructure
- System-assigned managed identity
- Azure RBAC for Blob, Queue, Table, and Application Insights telemetry

Microsoft documents the isolated worker model as the recommended model; the Durable Functions isolated extension is `Microsoft.Azure.Functions.Worker.Extensions.DurableTask`. The current NuGet release used here is 1.19.0 (published August 20, 2026). See Microsoft Learn and NuGet for current package details.

### Local run

Prerequisites:

- .NET 8 SDK
- Azure Functions Core Tools v4
- Azurite
- Azure CLI for Azure deployment

Start Azurite:

```bash
docker compose -f docker-compose.yml up -d
```

Copy:

```text
samples/DurableJobPipeline.Functions/local.settings.json.example
```

to:

```text
samples/DurableJobPipeline.Functions/local.settings.json
```

Then:

```bash
dotnet restore
dotnet build
func start --project samples/DurableJobPipeline.Functions
```

Set up the pipeline against Azurite in another terminal:

```bash
export PIPELINE_STORAGE_CONNECTION_STRING='UseDevelopmentStorage=true'
dotnet run --project samples/DurableJobPipeline.PipelineSetup
```

Start a pipeline:

```bash
curl -X POST \
  'http://localhost:7071/api/pipelines/DataImport/runs' \
  -H 'Content-Type: application/json' \
  -d '{"source":"demo","rows":1000}'
```

Check a run:

```bash
curl 'http://localhost:7071/api/runs/<RUN_ID>'
```

Cancel:

```bash
curl -X POST 'http://localhost:7071/api/runs/<RUN_ID>/cancel' \
  -H 'Content-Type: application/json' \
  -d '{"reason":"Operator requested cancellation"}'
```

Restart a failed/canceled run:

```bash
curl -X POST 'http://localhost:7071/api/runs/<RUN_ID>/restart'
```

Restart from a specific job (that job and all descendants are rerun):

```bash
curl -X POST 'http://localhost:7071/api/runs/<RUN_ID>/restart' \
  -H 'Content-Type: application/json' \
  -d '{"restartFromJobId":"<JOB_ID>"}'
```

Queue message format:

```json
{
  "pipelineName": "DataImport",
  "parameters": {
    "source": "queue",
    "rows": 5000
  }
}
```

## Azure deployment

The Bicep deploys:

- Resource group
- Storage account with Blob, Queue, and Table services
- Durable Functions task-hub storage configuration
- Blob container for function deployment
- Blob container for job outputs
- Tables `DjpPipelines`, `DjpRuns`, `DjpJobs`
- Queue `pipeline-runs`
- Log Analytics workspace
- Application Insights
- Flex Consumption plan
- .NET isolated Function App
- System-assigned managed identity
- RBAC for storage and Application Insights telemetry

Microsoft's current Flex Consumption samples use the `FC1`/`FlexConsumption` plan and configure deployment storage plus instance memory/concurrency through `functionAppConfig`. See the Azure Functions Flex Consumption samples.

Deploy infrastructure:

```bash
az login
AZURE_LOCATION=centralindia ENVIRONMENT_NAME=djp-dev ./infra/deploy.sh
```

Get outputs:

```bash
az deployment sub show \
  --name main \
  --query properties.outputs
```

Set the setup tool environment variables from the Bicep output:

```bash
export PIPELINE_TABLE_SERVICE_URI='https://<storage>.table.core.windows.net'
az login
dotnet run --project samples/DurableJobPipeline.PipelineSetup
```

Build and publish the Functions project:

```bash
dotnet publish samples/DurableJobPipeline.Functions \
  -c Release \
  -o ./artifacts/functions
```

Or use the included helper:

```bash
RESOURCE_GROUP=rg-djp-dev FUNCTION_APP_NAME=<function-app-name> ./infra/publish-functions.sh
```

For Flex Consumption, use the deployment mechanism documented for Flex Consumption for your current CLI/tooling. The current Microsoft guidance documents `az functionapp deployment source config-zip` for Flex workflows and also documents that the general App Service zip-push technology differs from Flex's deployment model.

Example:

```bash
cd artifacts/functions
zip -r ../DurableJobPipeline.Functions.zip .
cd ../..

az functionapp deployment source config-zip \
  --resource-group rg-djp-dev \
  --name <function-app-name> \
  --src artifacts/DurableJobPipeline.Functions.zip
```

If your organization uses `azd`, GitHub Actions, or Azure Pipelines, those can be used instead of the CLI deployment step.

## Azure configuration

The Function App uses these important settings:

```text
AzureWebJobsStorage__credential=managedidentity
AzureWebJobsStorage__blobServiceUri=https://<storage>.blob.core.windows.net
AzureWebJobsStorage__queueServiceUri=https://<storage>.queue.core.windows.net
AzureWebJobsStorage__tableServiceUri=https://<storage>.table.core.windows.net
APPLICATIONINSIGHTS_CONNECTION_STRING=<generated by Bicep>
APPLICATIONINSIGHTS_AUTHENTICATION_STRING=Authorization=AAD
PIPELINE_TABLE_SERVICE_URI=https://<storage>.table.core.windows.net
PIPELINE_BLOB_SERVICE_URI=https://<storage>.blob.core.windows.net
PIPELINE_MAX_CONCURRENT_JOBS=4
```

The Function identity gets Storage Blob Data Contributor, Storage Queue Data Contributor, Storage Table Data Contributor, and Monitoring Metrics Publisher. Microsoft documents these built-in roles and their role definition ids.

## Security notes

The sample disables storage account shared-key authentication and uses managed identity in Azure. For local development, Azurite uses the local storage connection string.

The sample keeps deployment/storage resources in one account to reduce setup complexity. For production isolation, use a dedicated durable-state account and a separate application-data/output account, and narrow RBAC scopes to the specific containers/queues/tables.

## Job outputs and large data

Use Azure Blob Storage for large outputs rather than putting large payloads in orchestration history or Table entities. `JobOutput.BlobUri` is intended for this purpose. A job can resolve `IJobOutputStore` from DI and write a stream directly to Blob Storage.

## Important runtime behavior

Durable Functions replays orchestrator code. Orchestrators must remain deterministic and should not perform direct external I/O. In this solution, storage reads/writes are activities; job handlers are also activities.

Do not put SDK calls to SQL, Blob, HTTP, Table, filesystem, environment-dependent clocks, random numbers, or other side effects directly inside the orchestrator function.

## Current package versions

The solution pins current versions verified against NuGet/Microsoft documentation on August 29, 2026:

- `Microsoft.Azure.Functions.Worker.Extensions.DurableTask` 1.19.0
- `Microsoft.Azure.Functions.Worker.Sdk` 2.1.0
- `Microsoft.Azure.Functions.Worker` 2.0.0
- `Azure.Data.Tables` 12.12.0
- `Azure.Storage.Blobs` 12.29.2
- `Microsoft.Azure.Functions.Worker.Extensions.Storage.Queues` 5.5.5
- `Microsoft.Azure.Functions.Worker.ApplicationInsights` 2.51.0

Always review package release notes before production upgrades.

## References

Microsoft Learn: .NET isolated Durable Functions
https://learn.microsoft.com/en-us/azure/azure-functions/durable-functions/durable-functions-dotnet-isolated-overview

Microsoft Learn: .NET isolated process guide
https://learn.microsoft.com/en-us/azure/azure-functions/dotnet-isolated-process-guide

Microsoft Learn: Flex Consumption
https://learn.microsoft.com/en-us/azure/azure-functions/flex-consumption-how-to

Microsoft Learn: Storage roles
https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/storage

Microsoft Learn: Blob Storage .NET SDK
https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-dotnet-get-started
