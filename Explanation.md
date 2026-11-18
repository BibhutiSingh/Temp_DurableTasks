Solution into separate projects and added a complete Azure Functions isolated-worker sample.

### Project structure

```text
DurableJobPipeline/
├── src/
│   ├── DurableJobPipeline.Core
│   ├── DurableJobPipeline.DurableTask
│   ├── DurableJobPipeline.AzureFunctions
│   ├── DurableJobPipeline.AzureStorage
│   ├── DurableJobPipeline.AzureStorageQueue
│   └── DurableJobPipeline.Sqlite
│
├── samples/
│   ├── DurableJobPipeline.Functions
│   └── DurableJobPipeline.PipelineSetup
│
├── tests/
│   └── DurableJobPipeline.Tests
│
└── infra/
    ├── main.bicep
    ├── main.bicepparam.example
    ├── deploy.sh
    └── publish-functions.sh
```

The Functions project uses the current .NET isolated Durable Functions model with `TaskOrchestrationContext` and `DurableTaskClient`, matching Microsoft's current guidance. ([Microsoft Learn][1])

### Included Azure Functions functionality

The sample contains:

```text
HTTP
 ├─ POST /api/pipelines/{pipelineName}/runs
 ├─ GET  /api/runs/{runId}
 ├─ POST /api/runs/{runId}/cancel
 └─ POST /api/runs/{runId}/restart

Azure Storage Queue
 └─ pipeline-runs

Durable Functions
 └─ PipelineOrchestrator
      ├─ LoadPipeline
      ├─ LoadPreviousJobs
      ├─ CreateRun
      ├─ ExecuteJob
      ├─ SetJobStatus
      └─ CompleteRun
```

The HTTP/queue triggers schedule a durable orchestration rather than executing the long-running job directly. `ScheduleNewOrchestrationInstanceAsync` supports explicit instance IDs, which the sample uses for run IDs. ([Microsoft Learn][2])

### Azure storage

The Bicep deployment creates:

```text
Storage Account
 ├── Blob
 │    ├── function deployment container
 │    └── pipeline-job-output
 │
 ├── Tables
 │    ├── DjpPipelines
 │    ├── DjpRuns
 │    └── DjpJobs
 │
 └── Queue
      └── pipeline-runs
```

`AzureTablePipelineStorage` persists pipeline definitions and run/job state, while `AzureBlobJobOutputStore` is used for large job results.

The blob reader was also adjusted to resolve private blobs through the authenticated container client, so managed-identity deployments don't depend on anonymous/public access.

### Cancellation

The cancellation endpoint now:

```text
POST /api/runs/{runId}/cancel
        ↓
persist CancellationRequested
        ↓
mark application run Canceled
        ↓
terminate Durable orchestration
```

Jobs receive the isolated-worker cancellation token:

```csharp
public override async Task<object> ExecuteAsync(
    JobExecutionContext context,
    Parameters parameters,
    CancellationToken cancellationToken)
{
    await ProcessAsync(cancellationToken);
}
```

Azure Functions isolated workers explicitly support `CancellationToken` for graceful shutdown/cancellation. ([Microsoft Learn][3])

Cancellation remains cooperative for an activity that is already executing; the job must propagate the token into HTTP, stream, database, etc. operations.

### Restart

Restart creates a **new Durable orchestration instance**:

```text
Run 100
 ├── Extract     Completed
 ├── Transform   Failed
 ├── Validate    Skipped
 └── Upload      Skipped

             ↓ restart

Run 101
 ├── Extract     reused
 ├── Transform   rerun
 ├── Validate    rerun
 └── Upload      rerun
```

The API also supports explicitly selecting the restart point:

```http
POST /api/runs/{runId}/restart
Content-Type: application/json

{
  "restartFromJobId": "..."
}
```

Completed jobs before that point are reused.

### Application Insights

The Functions sample includes:

```csharp
builder.Services.AddApplicationInsightsTelemetryWorkerService();
builder.Services.ConfigureFunctionsApplicationInsights();
```

and the Bicep deployment provisions Application Insights + Log Analytics. Microsoft's isolated-worker guidance uses these registrations for Application Insights integration. ([Microsoft Learn][4])

The Bicep sample also configures Application Insights authentication through Microsoft Entra ID rather than relying on a secret instrumentation key.

### Managed identity + RBAC

The Function App gets a system-assigned managed identity and the Bicep assigns:

```text
Storage Blob Data Contributor
Storage Queue Data Contributor
Storage Table Data Contributor
Monitoring Metrics Publisher
```

Shared-key authentication on the application storage account is disabled.

### Flex Consumption

The Bicep deploys a Linux `.NET 8 isolated` Function App on the Flex Consumption plan and configures deployment storage plus instance memory/scaling settings through `functionAppConfig`, following Microsoft's current Flex Consumption model.

### Deployment

Infrastructure:

```bash
az login

AZURE_LOCATION=centralindia \
ENVIRONMENT_NAME=djp-dev \
./infra/deploy.sh
```

Then configure the pipeline definitions:

```bash
export PIPELINE_TABLE_SERVICE_URI="https://<storage>.table.core.windows.net"

dotnet run \
  --project samples/DurableJobPipeline.PipelineSetup
```

Publish/deploy the Function App:

```bash
RESOURCE_GROUP=rg-djp-dev \
FUNCTION_APP_NAME=<function-app-name> \
./infra/publish-functions.sh
```

The Bicep creates the storage tables and blob containers as part of infrastructure deployment, so the setup executable only needs to persist the actual pipeline definitions.

### Example pipeline

The setup project demonstrates:

```csharp
var setup = new PipelineSetup(storage);

await setup.SetupPipeline("DataImport", pipeline =>
{
    pipeline.ShortCircuitOnError();

    var extract = pipeline.AddJob<ExtractJob>("Extract");

    pipeline.AddJob<TransformJob>(
        "Transform",
        extract);

    pipeline.AddJob<IndependentJob>(
        "Independent");
});
```

Result:

```text
             ┌── Transform
Extract ─────┤
             │
             └────────────

Independent ─────────────
```

So `Extract` and `Independent` can execute concurrently, while `Transform` waits for `Extract`.

I also corrected the core registration so the abstract `PipelineController` is not incorrectly registered with DI; setup remains outside DI as you requested.

### Validation

I performed static project/XML/JSON/reference/brace consistency checks on the solution and verified the generated ZIP. The environment does **not** have the .NET SDK, Azure Functions Core Tools, Azure CLI, or Bicep CLI installed, so I could not truthfully run `dotnet build`, `dotnet test`, `func start`, or `az deployment`.

Current pinned package versions were checked against current Microsoft/NuGet information on August 29, 2026, including Durable Functions isolated `1.19.0`, Worker SDK `2.1.0`, Azure Tables `12.12.0`, and Azure Blobs `12.29.2`.

[1]: https://learn.microsoft.com/en-us/azure/durable-task/durable-functions/durable-functions-dotnet-isolated-overview?utm_source=chatgpt.com "Overview of Durable Functions in .NET Isolated Worker | Microsoft Learn"
[2]: https://learn.microsoft.com/en-us/dotnet/api/microsoft.durabletask.client.durabletaskclient.scheduleneworchestrationinstanceasync?view=durabletask-dotnet-1.x&utm_source=chatgpt.com "DurableTaskClient.ScheduleNewOrchestrationInstanceAsync Method (Microsoft.DurableTask.Client) | Microsoft Learn"
[3]: https://learn.microsoft.com/en-us/azure/azure-functions/dotnet-isolated-process-guide?utm_source=chatgpt.com "Guide for running C# Azure Functions in an isolated worker process | Microsoft Learn"
[4]: https://learn.microsoft.com/en-us/azure/azure-functions/durable-functions/durable-functions-migrate?utm_source=chatgpt.com "Migrate Durable Functions from in-process to isolated worker (.NET) | Microsoft Learn"
