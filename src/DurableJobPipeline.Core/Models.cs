using System.Text.Json;

namespace DurableJobPipeline;

public enum JobStatus { Pending, Running, Completed, Failed, Skipped, Canceled }
public enum PipelineRunStatus { Pending, Running, Completed, Failed, Canceled }
public enum PipelineFailureMode { ContinueIndependentBranches, ShortCircuit }
public enum WorkerCapacityMode { Automatic, Manual, Functions }

public sealed record JobDefinition(
    Guid Id,
    string Name,
    string JobType,
    IReadOnlyList<Guid> DependsOn,
    int MaxAttempts = 3);

public sealed record PipelineDefinition(
    Guid Id,
    string Name,
    IReadOnlyList<JobDefinition> Jobs,
    PipelineFailureMode FailureMode = PipelineFailureMode.ContinueIndependentBranches);

public sealed record PipelineTriggerRequest(string PipelineName, JsonElement Parameters)
{
    public static PipelineTriggerRequest From<T>(string pipelineName, T parameters)
        => new(pipelineName, JsonSerializer.SerializeToElement(parameters, PipelineJson.Options));
}

public sealed record PipelineStartInput(
    Guid PipelineId,
    string PipelineName,
    JsonElement Parameters,
    Guid ResumeFromRunId = default,
    Guid RestartFromJobId = default);

public sealed record JobExecutionRequest(
    Guid PipelineId,
    string PipelineName,
    Guid RunId,
    Guid JobId,
    string JobName,
    string JobType,
    JsonElement Parameters,
    IReadOnlyDictionary<Guid, JobOutput> DependencyOutputs);

public sealed record JobOutput(
    Guid PipelineId,
    Guid RunId,
    Guid JobId,
    JobStatus JobStatus,
    JsonElement? JobOutput,
    string? Error = null,
    string? BlobUri = null);

public sealed record PipelineRunResult(
    Guid PipelineId,
    Guid RunId,
    PipelineRunStatus Status,
    IReadOnlyList<JobOutput> Jobs);

public sealed record PipelineRunRecord(
    Guid RunId,
    Guid PipelineId,
    string PipelineName,
    PipelineRunStatus Status,
    DateTimeOffset CreatedAt,
    DateTimeOffset? CompletedAt,
    JsonElement Parameters,
    bool CancellationRequested = false,
    string? CancellationReason = null);

public sealed record JobRunRecord(
    Guid RunId,
    Guid JobId,
    JobStatus Status,
    JsonElement? Output,
    string? Error,
    DateTimeOffset? StartedAt,
    DateTimeOffset? CompletedAt,
    string? BlobUri = null);

public sealed record PipelineControlRequest(Guid RunId, string? Reason = null);

public sealed record PreparedPipeline(
    PipelineDefinition Pipeline,
    Guid RunId,
    JsonElement Parameters,
    IReadOnlyDictionary<Guid, JobOutput> ResumeOutputs);

public sealed record JobExecutionResult(object? Output, string? BlobUri = null)
{
    public static JobExecutionResult From(object? output) => new(output);
    public static JobExecutionResult FromBlob(string blobUri, object? metadata = null) => new(metadata, blobUri);
}
