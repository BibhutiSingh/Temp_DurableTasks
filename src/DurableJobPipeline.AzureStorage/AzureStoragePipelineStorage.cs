using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using DurableJobPipeline;
using Microsoft.Extensions.DependencyInjection;

namespace DurableJobPipeline.AzureStorage;

public sealed class AzureTablePipelineStorage : IPipelineStorage
{
    private readonly TableServiceClient _service;
    private readonly TableClient _pipelines;
    private readonly TableClient _runs;
    private readonly TableClient _jobs;

    public AzureTablePipelineStorage(string connectionString, string pipelineTable = "DjpPipelines", string runTable = "DjpRuns", string jobTable = "DjpJobs")
        : this(new TableServiceClient(connectionString), pipelineTable, runTable, jobTable) { }

    public AzureTablePipelineStorage(Uri tableServiceUri, Azure.Core.TokenCredential credential, string pipelineTable = "DjpPipelines", string runTable = "DjpRuns", string jobTable = "DjpJobs")
        : this(new TableServiceClient(tableServiceUri, credential), pipelineTable, runTable, jobTable) { }

    private AzureTablePipelineStorage(TableServiceClient service, string pipelineTable, string runTable, string jobTable)
    {
        _service = service;
        _pipelines = service.GetTableClient(pipelineTable);
        _runs = service.GetTableClient(runTable);
        _jobs = service.GetTableClient(jobTable);
    }

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        await _service.CreateTableIfNotExistsAsync(_pipelines.Name, ct);
        await _service.CreateTableIfNotExistsAsync(_runs.Name, ct);
        await _service.CreateTableIfNotExistsAsync(_jobs.Name, ct);
    }

    public async Task SavePipelineAsync(PipelineDefinition pipeline, CancellationToken ct = default)
    {
        PipelineValidator.Validate(pipeline);
        var json = JsonSerializer.Serialize(pipeline, PipelineJson.Options);
        var entity = new TableEntity("Pipeline", pipeline.Id.ToString("N"))
        {
            ["Name"] = pipeline.Name,
            ["Definition"] = json,
            ["UpdatedAt"] = DateTimeOffset.UtcNow
        };
        await _pipelines.UpsertEntityAsync(entity, TableUpdateMode.Replace, ct);

        // Name index, allowing lookup by pipeline name without scanning the table.
        var byName = new TableEntity("Name", pipeline.Name.ToLowerInvariant())
        {
            ["PipelineId"] = pipeline.Id.ToString("N")
        };
        await _pipelines.UpsertEntityAsync(byName, TableUpdateMode.Replace, ct);
    }

    public async Task<PipelineDefinition?> GetPipelineAsync(Guid pipelineId, CancellationToken ct = default)
    {
        try
        {
            var entity = await _pipelines.GetEntityAsync<TableEntity>("Pipeline", pipelineId.ToString("N"), ct);
            return JsonSerializer.Deserialize<PipelineDefinition>(entity.Value["Definition"]?.ToString() ?? "", PipelineJson.Options);
        }
        catch (RequestFailedException ex) when (ex.Status == 404) { return null; }
    }

    public async Task<PipelineDefinition?> GetPipelineByNameAsync(string pipelineName, CancellationToken ct = default)
    {
        try
        {
            var index = await _pipelines.GetEntityAsync<TableEntity>("Name", pipelineName.ToLowerInvariant(), ct);
            if (Guid.TryParse(index.Value["PipelineId"]?.ToString(), out var id)) return await GetPipelineAsync(id, ct);
            return null;
        }
        catch (RequestFailedException ex) when (ex.Status == 404) { return null; }
    }

    public Task CreateRunAsync(PipelineRunRecord run, CancellationToken ct = default)
        => _runs.UpsertEntityAsync(ToEntity(run), TableUpdateMode.Replace, ct);

    public async Task SetPipelineRunStatusAsync(Guid runId, PipelineRunStatus status, DateTimeOffset? completedAt = null, CancellationToken ct = default)
    {
        var existing = await GetPipelineRunAsync(runId, ct) ?? throw new InvalidOperationException($"Run '{runId}' was not found.");
        var updated = existing with { Status = status, CompletedAt = completedAt };
        await _runs.UpsertEntityAsync(ToEntity(updated), TableUpdateMode.Replace, ct);
    }

    public async Task<PipelineRunRecord?> GetPipelineRunAsync(Guid runId, CancellationToken ct = default)
    {
        try
        {
            var e = await _runs.GetEntityAsync<TableEntity>("Run", runId.ToString("N"), ct);
            return FromEntity(e.Value, runId);
        }
        catch (RequestFailedException ex) when (ex.Status == 404) { return null; }
    }

    public async Task SetCancellationRequestedAsync(Guid runId, string? reason = null, CancellationToken ct = default)
    {
        var run = await GetPipelineRunAsync(runId, ct) ?? throw new InvalidOperationException($"Run '{runId}' was not found.");
        await _runs.UpsertEntityAsync(ToEntity(run with { CancellationRequested = true, CancellationReason = reason ?? run.CancellationReason }), TableUpdateMode.Replace, ct);
    }

    public async Task<bool> IsCancellationRequestedAsync(Guid runId, CancellationToken ct = default)
        => (await GetPipelineRunAsync(runId, ct))?.CancellationRequested == true;

    public Task SetJobStatusAsync(JobRunRecord job, CancellationToken ct = default)
        => _jobs.UpsertEntityAsync(ToEntity(job), TableUpdateMode.Replace, ct);

    public async Task<JobRunRecord?> GetJobRunAsync(Guid runId, Guid jobId, CancellationToken ct = default)
    {
        try
        {
            var e = await _jobs.GetEntityAsync<TableEntity>(runId.ToString("N"), jobId.ToString("N"), ct);
            return FromEntity(e.Value, runId, jobId);
        }
        catch (RequestFailedException ex) when (ex.Status == 404) { return null; }
    }

    public async Task<IReadOnlyList<JobRunRecord>> GetJobRunsAsync(Guid runId, CancellationToken ct = default)
    {
        var result = new List<JobRunRecord>();
        await foreach (var e in _jobs.QueryAsync<TableEntity>(x => x.PartitionKey == runId.ToString("N"), cancellationToken: ct))
        {
            if (Guid.TryParse(e.RowKey, out var jobId)) result.Add(FromEntity(e, runId, jobId));
        }
        return result;
    }

    private static TableEntity ToEntity(PipelineRunRecord x) => new("Run", x.RunId.ToString("N"))
    {
        ["PipelineId"] = x.PipelineId.ToString("N"), ["PipelineName"] = x.PipelineName,
        ["Parameters"] = x.Parameters.GetRawText(),
        ["Status"] = (int)x.Status, ["CreatedAt"] = x.CreatedAt, ["CompletedAt"] = x.CompletedAt,
        ["CancellationRequested"] = x.CancellationRequested, ["CancellationReason"] = x.CancellationReason
    };

    private static PipelineRunRecord FromEntity(TableEntity e, Guid id)
    {
        var parameters = e.TryGetValue("Parameters", out var p) && p is not null
            ? JsonSerializer.Deserialize<JsonElement>(p.ToString()!, PipelineJson.Options)
            : JsonSerializer.SerializeToElement(new { });
        return new PipelineRunRecord(
            id, Guid.Parse((string)e["PipelineId"]), (string)e["PipelineName"], (PipelineRunStatus)(int)e["Status"],
            (DateTimeOffset)e["CreatedAt"], e.TryGetValue("CompletedAt", out var c) && c is DateTimeOffset cd ? cd : null,
            parameters,
            e.TryGetValue("CancellationRequested", out var cr) && cr is bool b && b,
            e.TryGetValue("CancellationReason", out var r) ? r?.ToString() : null);
    }

    private static TableEntity ToEntity(JobRunRecord x) => new(x.RunId.ToString("N"), x.JobId.ToString("N"))
    {
        ["Status"] = (int)x.Status, ["Output"] = x.Output?.GetRawText(), ["Error"] = x.Error,
        ["StartedAt"] = x.StartedAt, ["CompletedAt"] = x.CompletedAt, ["BlobUri"] = x.BlobUri
    };

    private static JobRunRecord FromEntity(TableEntity e, Guid runId, Guid jobId)
    {
        JsonElement? output = null;
        if (e.TryGetValue("Output", out var raw) && raw is not null && !string.IsNullOrWhiteSpace(raw.ToString()))
            output = JsonSerializer.Deserialize<JsonElement>(raw.ToString(), PipelineJson.Options);
        DateTimeOffset? Time(string key) => e.TryGetValue(key, out var v) && v is DateTimeOffset dto ? dto : null;
        return new JobRunRecord(runId, jobId, (JobStatus)(int)e["Status"], output,
            e.TryGetValue("Error", out var error) ? error?.ToString() : null, Time("StartedAt"), Time("CompletedAt"),
            e.TryGetValue("BlobUri", out var blob) ? blob?.ToString() : null);
    }
}

public sealed class AzureBlobJobOutputStore : IJobOutputStore
{
    private readonly BlobContainerClient _container;
    public AzureBlobJobOutputStore(string connectionString, string container = "pipeline-job-output")
        => _container = new BlobServiceClient(connectionString).GetBlobContainerClient(container);
    public AzureBlobJobOutputStore(Uri blobServiceUri, Azure.Core.TokenCredential credential, string container = "pipeline-job-output")
        => _container = new BlobServiceClient(blobServiceUri, credential).GetBlobContainerClient(container);

    public async Task InitializeAsync(CancellationToken ct = default) => await _container.CreateIfNotExistsAsync(cancellationToken: ct);

    public async Task<Uri> PutAsync(Guid runId, Guid jobId, Stream content, string contentType, CancellationToken ct = default)
    {
        var blob = _container.GetBlobClient($"{runId:N}/{jobId:N}.json");
        await blob.UploadAsync(content, new BlobUploadOptions { HttpHeaders = new BlobHttpHeaders { ContentType = contentType } }, ct);
        return blob.Uri;
    }

    public async Task<Stream> OpenReadAsync(Uri blobUri, CancellationToken ct = default)
    {
        var path = blobUri.AbsolutePath.TrimStart('/');
        var containerPrefix = _container.Name + "/";
        if (path.StartsWith(containerPrefix, StringComparison.OrdinalIgnoreCase))
            path = path[containerPrefix.Length..];
        var blob = _container.GetBlobClient(path);
        return (await blob.DownloadStreamingAsync(cancellationToken: ct)).Value.Content;
    }
}

public static class AzureStorageServiceCollectionExtensions
{
    public static IServiceCollection AddAzureTablePipelineStorage(this IServiceCollection services, string connectionString)
    {
        services.AddSingleton<IPipelineStorage>(_ => new AzureTablePipelineStorage(connectionString));
        return services;
    }
    public static IServiceCollection AddAzureBlobJobOutputStore(this IServiceCollection services, string connectionString)
    {
        services.AddSingleton<IJobOutputStore>(_ => new AzureBlobJobOutputStore(connectionString));
        return services;
    }
}
