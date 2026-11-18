using Azure.Identity;
using DurableJobPipeline;
using DurableJobPipeline.AzureStorage;

var connection = Environment.GetEnvironmentVariable("PIPELINE_STORAGE_CONNECTION_STRING");
AzureTablePipelineStorage storage;
if (!string.IsNullOrWhiteSpace(connection))
    storage = new AzureTablePipelineStorage(connection);
else
{
    var tableUrl = Environment.GetEnvironmentVariable("PIPELINE_TABLE_SERVICE_URI")
        ?? throw new InvalidOperationException("PIPELINE_TABLE_SERVICE_URI is required when using managed identity.");
    storage = new AzureTablePipelineStorage(new Uri(tableUrl), new DefaultAzureCredential());
}
await storage.InitializeAsync();

var setup = new PipelineSetup(storage);
var pipeline = await setup.SetupPipeline("DataImport", p =>
{
    p.ShortCircuitOnError();
    var extract = p.AddJob<ExtractJob>("Extract", 3);
    p.AddJob<TransformJob>("Transform", 3, extract);
    p.AddJob<IndependentJob>("Independent", 2);
});

Console.WriteLine($"Pipeline '{pipeline.Name}' saved with id {pipeline.Id}");

[JobType("extract")] public sealed class ExtractJob : JobHandler<object, object> { public override string JobType => "extract"; public override Task<object> ExecuteAsync(JobExecutionContext c, object p, CancellationToken ct) => Task.FromResult<object>(new { ok = true }); }
[JobType("transform")] public sealed class TransformJob : JobHandler<object, object> { public override string JobType => "transform"; public override Task<object> ExecuteAsync(JobExecutionContext c, object p, CancellationToken ct) => Task.FromResult<object>(new { ok = true }); }
[JobType("independent")] public sealed class IndependentJob : JobHandler<object, object> { public override string JobType => "independent"; public override Task<object> ExecuteAsync(JobExecutionContext c, object p, CancellationToken ct) => Task.FromResult<object>(new { ok = true }); }
