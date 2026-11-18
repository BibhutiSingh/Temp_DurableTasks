using Azure.Identity;
using DurableJobPipeline;
using DurableJobPipeline.AzureFunctions;
using DurableJobPipeline.AzureStorage;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = FunctionsApplication.CreateBuilder(args);

builder.ConfigureFunctionsWebApplication();
builder.Services.AddApplicationInsightsTelemetryWorkerService();
builder.Services.ConfigureFunctionsApplicationInsights();

var tableUrl = builder.Configuration["PIPELINE_TABLE_SERVICE_URI"];
var blobUrl = builder.Configuration["PIPELINE_BLOB_SERVICE_URI"];
var credential = new DefaultAzureCredential();
var storageConnectionString = builder.Configuration["PIPELINE_STORAGE_CONNECTION_STRING"];

builder.Services.AddDurableJobPipelineFunctions(options =>
{
    options.CapacityMode = WorkerCapacityMode.Functions;
    options.MaxConcurrentJobs = builder.Configuration.GetValue("PIPELINE_MAX_CONCURRENT_JOBS", 4);
});

if (!string.IsNullOrWhiteSpace(storageConnectionString))
{
    builder.Services.AddSingleton<IPipelineStorage>(_ => new AzureTablePipelineStorage(storageConnectionString));
    builder.Services.AddSingleton<IJobOutputStore>(_ => new AzureBlobJobOutputStore(storageConnectionString));
}
else
{
    if (string.IsNullOrWhiteSpace(tableUrl) || string.IsNullOrWhiteSpace(blobUrl))
        throw new InvalidOperationException("PIPELINE_TABLE_SERVICE_URI and PIPELINE_BLOB_SERVICE_URI are required when PIPELINE_STORAGE_CONNECTION_STRING is not set.");
    builder.Services.AddSingleton<IPipelineStorage>(_ => new AzureTablePipelineStorage(new Uri(tableUrl), credential));
    builder.Services.AddSingleton<IJobOutputStore>(_ => new AzureBlobJobOutputStore(new Uri(blobUrl), credential));
}

builder.Services.AddDurableJobHandler<ExtractJob>();
builder.Services.AddDurableJobHandler<TransformJob>();
builder.Services.AddDurableJobHandler<IndependentJob>();

var host = builder.Build();
host.Run();

public sealed record ExtractParameters(string Source, int Rows = 1000);
public sealed record TransformParameters(string Operation = "uppercase");

[JobType("extract")]
public sealed class ExtractJob(IJobOutputStore outputStore) : JobHandler<ExtractParameters, object>
{
    public override string JobType => "extract";

    public override async Task<object> ExecuteAsync(JobExecutionContext context, ExtractParameters parameters, CancellationToken cancellationToken)
    {
        var payload = $$"""{"pipeline":"{{context.PipelineName}}","source":"{{parameters.Source}}","rows":{{parameters.Rows}}} """.TrimEnd();
        await using var input = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(payload));
        var uri = await outputStore.PutAsync(context.RunId, context.JobId, input, "application/json", cancellationToken);
        return new { blobUri = uri.ToString(), rows = parameters.Rows };
    }
}

[JobType("transform")]
public sealed class TransformJob : JobHandler<TransformParameters, object>
{
    public override string JobType => "transform";
    public override Task<object> ExecuteAsync(JobExecutionContext context, TransformParameters parameters, CancellationToken cancellationToken)
    {
        var dependency = context.DependencyOutputs.Values.FirstOrDefault()?.JobOutput;
        return Task.FromResult<object>(new { operation = parameters.Operation, input = dependency });
    }
}

[JobType("independent")]
public sealed class IndependentJob : JobHandler<object, object>
{
    public override string JobType => "independent";
    public override async Task<object> ExecuteAsync(JobExecutionContext context, object parameters, CancellationToken cancellationToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
        return new { message = "Independent branch completed", at = DateTimeOffset.UtcNow };
    }
}
