using System.Text.Json;

namespace DurableJobPipeline;

public interface IPipelineRunStarter
{
    Task<Guid> StartAsync(PipelineTriggerRequest request, CancellationToken cancellationToken = default);
    Task<Guid> StartAsync(Guid pipelineId, JsonElement parameters, CancellationToken cancellationToken = default);
    Task<Guid> RestartAsync(Guid failedRunId, Guid? failedJobId = null, CancellationToken cancellationToken = default);
    Task CancelAsync(Guid runId, string? reason = null, CancellationToken cancellationToken = default);
}

public abstract class PipelineController(IPipelineStorage storage)
{
    public virtual async Task CancelAsync(Guid runId, string? reason = null, CancellationToken cancellationToken = default)
    {
        await storage.SetCancellationRequestedAsync(runId, reason, cancellationToken).ConfigureAwait(false);
    }

    protected async Task<PipelineStartInput> BuildRestartInputAsync(Guid failedRunId, Guid? failedJobId, CancellationToken cancellationToken)
    {
        var run = await storage.GetPipelineRunAsync(failedRunId, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"Run '{failedRunId}' was not found.");
        if (run.Status is not PipelineRunStatus.Failed and not PipelineRunStatus.Canceled)
            throw new InvalidOperationException("Only failed or canceled runs can be restarted.");
        if (failedJobId is null)
        {
            var jobs = await storage.GetJobRunsAsync(failedRunId, cancellationToken).ConfigureAwait(false);
            failedJobId = jobs.FirstOrDefault(x => x.Status == JobStatus.Failed)?.JobId;
        }
        return new PipelineStartInput(run.PipelineId, run.PipelineName,
            run.Parameters.Clone(), failedRunId, failedJobId ?? Guid.Empty);
    }
}
