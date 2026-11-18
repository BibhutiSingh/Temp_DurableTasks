namespace DurableJobPipeline;

public interface IPipelineStorage
{
    Task SavePipelineAsync(PipelineDefinition pipeline, CancellationToken cancellationToken = default);
    Task<PipelineDefinition?> GetPipelineByNameAsync(string pipelineName, CancellationToken cancellationToken = default);
    Task<PipelineDefinition?> GetPipelineAsync(Guid pipelineId, CancellationToken cancellationToken = default);

    Task CreateRunAsync(PipelineRunRecord run, CancellationToken cancellationToken = default);
    Task SetPipelineRunStatusAsync(Guid runId, PipelineRunStatus status, DateTimeOffset? completedAt = null, CancellationToken cancellationToken = default);
    Task<PipelineRunRecord?> GetPipelineRunAsync(Guid runId, CancellationToken cancellationToken = default);
    Task SetCancellationRequestedAsync(Guid runId, string? reason = null, CancellationToken cancellationToken = default);
    Task<bool> IsCancellationRequestedAsync(Guid runId, CancellationToken cancellationToken = default);

    Task SetJobStatusAsync(JobRunRecord job, CancellationToken cancellationToken = default);
    Task<JobRunRecord?> GetJobRunAsync(Guid runId, Guid jobId, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<JobRunRecord>> GetJobRunsAsync(Guid runId, CancellationToken cancellationToken = default);
}

public interface IJobOutputStore
{
    Task<Uri> PutAsync(Guid runId, Guid jobId, Stream content, string contentType, CancellationToken cancellationToken = default);
    Task<Stream> OpenReadAsync(Uri blobUri, CancellationToken cancellationToken = default);
}
