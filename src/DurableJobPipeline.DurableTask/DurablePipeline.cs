using System.Text.Json;
using DurableJobPipeline;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace DurableJobPipeline.DurableTask;

[DurableTask(nameof(PipelineOrchestrator))]
public sealed class PipelineOrchestrator : TaskOrchestrator<PipelineStartInput, PipelineRunResult>
{
    public override async Task<PipelineRunResult> RunAsync(TaskOrchestrationContext context, PipelineStartInput input)
    {
        var prepared = await context.CallActivityAsync<PreparedPipeline>(nameof(PreparePipelineActivity), input);
        var pipeline = prepared.Pipeline;
        var outputs = new Dictionary<Guid, JobOutput>(prepared.ResumeOutputs);
        var pending = pipeline.Jobs.Where(j => !outputs.ContainsKey(j.Id)).ToDictionary(j => j.Id);
        var shortCircuit = false;

        while (pending.Count > 0)
        {
            var ready = pending.Values.Where(j => j.DependsOn.All(outputs.ContainsKey)).OrderBy(j => j.Name, StringComparer.Ordinal).ToList();
            if (ready.Count == 0) throw new InvalidOperationException($"Pipeline '{pipeline.Name}' contains an unsatisfied dependency graph.");

            foreach (var job in ready.Where(j => j.DependsOn.Any(d => outputs[d].JobStatus == JobStatus.Canceled)).ToList())
            {
                var skipped = new JobOutput(pipeline.Id, prepared.RunId, job.Id, JobStatus.Skipped, null, "A dependency was canceled.");
                await context.CallActivityAsync<string>(nameof(UpdateJobStatusActivity), new JobRunRecord(skipped.RunId, skipped.JobId, skipped.JobStatus, null, skipped.Error, null, context.CurrentUtcDateTime));
                outputs[job.Id] = skipped; pending.Remove(job.Id);
            }

            foreach (var job in ready.Where(j => !j.DependsOn.Any(d => outputs[d].JobStatus == JobStatus.Canceled)
                                               && j.DependsOn.Any(d => outputs[d].JobStatus != JobStatus.Completed)).ToList())
            {
                var skipped = new JobOutput(pipeline.Id, prepared.RunId, job.Id, JobStatus.Skipped, null, "A dependency did not complete successfully.");
                await context.CallActivityAsync<string>(nameof(UpdateJobStatusActivity), new JobRunRecord(skipped.RunId, skipped.JobId, skipped.JobStatus, null, skipped.Error, null, context.CurrentUtcDateTime));
                outputs[job.Id] = skipped; pending.Remove(job.Id);
            }

            if (shortCircuit)
            {
                foreach (var job in pending.Values.ToList())
                {
                    var skipped = new JobOutput(pipeline.Id, prepared.RunId, job.Id, JobStatus.Skipped, null, "Pipeline was short-circuited.");
                    await context.CallActivityAsync<string>(nameof(UpdateJobStatusActivity), new JobRunRecord(skipped.RunId, skipped.JobId, skipped.JobStatus, null, skipped.Error, null, context.CurrentUtcDateTime));
                    outputs[job.Id] = skipped; pending.Remove(job.Id);
                }
                break;
            }

            var runnable = pending.Values.Where(j => j.DependsOn.All(d => outputs[d].JobStatus == JobStatus.Completed)
                                                     && ready.Contains(j)).ToList();
            if (runnable.Count == 0) continue;

            var tasks = runnable.Select(job =>
            {
                var request = new JobExecutionRequest(pipeline.Id, pipeline.Name, prepared.RunId, job.Id, job.Name, job.JobType,
                    prepared.Parameters.Clone(), job.DependsOn.ToDictionary(d => d, d => outputs[d]));
                var retry = TaskRetryOptions.FromRetryPolicy(new RetryPolicy(job.MaxAttempts, TimeSpan.FromSeconds(2), 2, TimeSpan.FromMinutes(2)));
                return context.CallActivityAsync<JobOutput>(nameof(ExecuteJobActivity), request, new TaskOptions(retry));
            }).ToArray();

            var results = new List<JobOutput>(tasks.Length);
            for (var i = 0; i < tasks.Length; i++)
            {
                try { results.Add(await tasks[i]); }
                catch (Exception ex)
                {
                    results.Add(new JobOutput(pipeline.Id, prepared.RunId, runnable[i].Id, JobStatus.Failed, null, ex.ToString()));
                }
            }

            foreach (var result in results)
            {
                outputs[result.JobId] = result;
                pending.Remove(result.JobId);
                if (result.JobStatus == JobStatus.Canceled ||
                    (result.JobStatus == JobStatus.Failed && pipeline.FailureMode == PipelineFailureMode.ShortCircuit))
                    shortCircuit = true;
            }
        }

        var status = outputs.Values.Any(x => x.JobStatus == JobStatus.Canceled) ? PipelineRunStatus.Canceled
            : outputs.Values.Any(x => x.JobStatus == JobStatus.Failed) ? PipelineRunStatus.Failed
            : PipelineRunStatus.Completed;

        await context.CallActivityAsync<string>(nameof(CompletePipelineActivity), new CompletePipelineRequest(prepared.RunId, status));
        return new PipelineRunResult(pipeline.Id, prepared.RunId, status, pipeline.Jobs.Select(j => outputs[j.Id]).ToArray());
    }
}

[DurableTask(nameof(PreparePipelineActivity))]
public sealed class PreparePipelineActivity(IPipelineStorage storage) : TaskActivity<PipelineStartInput, PreparedPipeline>
{
    public override async Task<PreparedPipeline> RunAsync(TaskActivityContext context, PipelineStartInput input)
    {
        var pipeline = input.PipelineId != Guid.Empty
            ? await storage.GetPipelineAsync(input.PipelineId)
            : await storage.GetPipelineByNameAsync(input.PipelineName);
        pipeline ??= throw new InvalidOperationException($"Pipeline '{input.PipelineName}' was not found.");
        PipelineValidator.Validate(pipeline);
        var runId = Guid.Parse(context.InstanceId);
        await storage.CreateRunAsync(new PipelineRunRecord(runId, pipeline.Id, pipeline.Name, PipelineRunStatus.Running, DateTimeOffset.UtcNow, null, input.Parameters.Clone()));

        var resume = new Dictionary<Guid, JobOutput>();
        if (input.ResumeFromRunId != Guid.Empty)
        {
            var prior = await storage.GetJobRunsAsync(input.ResumeFromRunId);
            var restartRoot = input.RestartFromJobId;
            var descendants = restartRoot == Guid.Empty ? [] : GetDescendants(pipeline, restartRoot.Value);
            foreach (var job in prior.Where(x => x.Status == JobStatus.Completed && (restartRoot == Guid.Empty || !descendants.Contains(x.JobId))))
                resume[job.JobId] = new JobOutput(pipeline.Id, runId, job.JobId, JobStatus.Completed, job.Output, job.Error, job.BlobUri);
        }
        return new PreparedPipeline(pipeline, runId, input.Parameters.Clone(), resume);
    }

    private static HashSet<Guid> GetDescendants(PipelineDefinition pipeline, Guid root)
    {
        var set = new HashSet<Guid> { root };
        var changed = true;
        while (changed)
        {
            changed = false;
            foreach (var job in pipeline.Jobs)
                if (!set.Contains(job.Id) && job.DependsOn.Any(set.Contains) && set.Add(job.Id)) changed = true;
        }
        return set;
    }
}

[DurableTask(nameof(ExecuteJobActivity))]
public sealed class ExecuteJobActivity(
    IJobHandlerRegistry handlers,
    IServiceScopeFactory scopeFactory,
    IPipelineStorage storage,
    JobExecutionLimiter limiter,
    IJobOutputStore? outputStore = null)
    : TaskActivity<JobExecutionRequest, JobOutput>
{
    public override async Task<JobOutput> RunAsync(TaskActivityContext context, JobExecutionRequest input)
    {
        var started = DateTimeOffset.UtcNow;
        await storage.SetJobStatusAsync(new JobRunRecord(input.RunId, input.JobId, JobStatus.Running, null, null, started, null));
        await limiter.WaitAsync(CancellationToken.None);
        try
        {
            await using var scope = scopeFactory.CreateAsyncScope();
            var handler = handlers.Create(input.JobType, scope.ServiceProvider);
            using var timeout = new CancellationTokenSource();
            // Cancellation is polled through storage by the adapter layer. A host shutdown token is also honored by DI hosts.
            var token = timeout.Token;
            var result = await handler.ExecuteAsync(new JobExecutionContext
            {
                PipelineId = input.PipelineId, PipelineName = input.PipelineName, RunId = input.RunId,
                JobId = input.JobId, JobName = input.JobName, Parameters = input.Parameters,
                DependencyOutputs = input.DependencyOutputs
            }, token);
            var output = result is null ? null : JsonSerializer.SerializeToElement(result, PipelineJson.Options);
            await storage.SetJobStatusAsync(new JobRunRecord(input.RunId, input.JobId, JobStatus.Completed, output, null, started, DateTimeOffset.UtcNow));
            return new JobOutput(input.PipelineId, input.RunId, input.JobId, JobStatus.Completed, output);
        }
        catch (OperationCanceledException)
        {
            var canceled = new JobOutput(input.PipelineId, input.RunId, input.JobId, JobStatus.Canceled, null, "Job canceled.");
            await storage.SetJobStatusAsync(new JobRunRecord(input.RunId, input.JobId, JobStatus.Canceled, null, canceled.Error, started, DateTimeOffset.UtcNow));
            return canceled;
        }
        catch (Exception ex)
        {
            await storage.SetJobStatusAsync(new JobRunRecord(input.RunId, input.JobId, JobStatus.Failed, null, ex.ToString(), started, DateTimeOffset.UtcNow));
            throw;
        }
        finally { limiter.Release(); }
    }
}

[DurableTask(nameof(UpdateJobStatusActivity))]
public sealed class UpdateJobStatusActivity(IPipelineStorage storage) : TaskActivity<JobRunRecord, string>
{
    public override async Task<string> RunAsync(TaskActivityContext context, JobRunRecord input)
    {
        await storage.SetJobStatusAsync(input); return input.Status.ToString();
    }
}

[DurableTask(nameof(CompletePipelineActivity))]
public sealed class CompletePipelineActivity(IPipelineStorage storage) : TaskActivity<CompletePipelineRequest, string>
{
    public override async Task<string> RunAsync(TaskActivityContext context, CompletePipelineRequest input)
    {
        var status = await storage.IsCancellationRequestedAsync(input.RunId) ? PipelineRunStatus.Canceled : input.Status;
        await storage.SetPipelineRunStatusAsync(input.RunId, status, DateTimeOffset.UtcNow); return status.ToString();
    }
}

public sealed record CompletePipelineRequest(Guid RunId, PipelineRunStatus Status);

public sealed class DurablePipelineStarter(DurableTaskClient client, IPipelineStorage storage) : IPipelineRunStarter
{
    public async Task<Guid> StartAsync(PipelineTriggerRequest request, CancellationToken ct = default)
    {
        var runId = Guid.NewGuid();
        await client.ScheduleNewOrchestrationInstanceAsync(new TaskName(nameof(PipelineOrchestrator)),
            new PipelineStartInput(Guid.Empty, request.PipelineName, request.Parameters.Clone()),
            new StartOrchestrationOptions(runId.ToString("N")), ct);
        return runId;
    }
    public async Task<Guid> StartAsync(Guid pipelineId, System.Text.Json.JsonElement parameters, CancellationToken ct = default)
    {
        var runId = Guid.NewGuid();
        await client.ScheduleNewOrchestrationInstanceAsync(new TaskName(nameof(PipelineOrchestrator)),
            new PipelineStartInput(pipelineId, string.Empty, parameters.Clone()), new StartOrchestrationOptions(runId.ToString("N")), ct);
        return runId;
    }
    public async Task<Guid> RestartAsync(Guid failedRunId, Guid? failedJobId = null, CancellationToken ct = default)
    {
        var run = await storage.GetPipelineRunAsync(failedRunId, ct) ?? throw new InvalidOperationException("Source run was not found.");
        if (run.Status is not PipelineRunStatus.Failed and not PipelineRunStatus.Canceled) throw new InvalidOperationException("Only failed/canceled runs can restart.");
        if (failedJobId is null)
        {
            var jobs = await storage.GetJobRunsAsync(failedRunId, ct);
            failedJobId = jobs.FirstOrDefault(j => j.Status == JobStatus.Failed)?.JobId;
        }
        var newId = Guid.NewGuid();
        await client.ScheduleNewOrchestrationInstanceAsync(new TaskName(nameof(PipelineOrchestrator)),
            new PipelineStartInput(run.PipelineId, run.PipelineName, run.Parameters.Clone(), failedRunId, failedJobId ?? Guid.Empty),
            new StartOrchestrationOptions(newId.ToString("N")), ct);
        return newId;
    }
    public Task CancelAsync(Guid runId, string? reason = null, CancellationToken ct = default)
        => storage.SetCancellationRequestedAsync(runId, reason, ct);
}

public static class DurableTaskServiceCollectionExtensions
{
    public static IServiceCollection AddDurableJobPipelineDurableTask(this IServiceCollection services, Action<DurablePipelineWorkerOptions>? configure = null)
    {
        services.AddDurableJobPipelineCore(configure);
        services.TryAddSingleton<IPipelineRunStarter, DurablePipelineStarter>();
        services.TryAddSingleton(sp => (PipelineController)sp.GetRequiredService<PipelineController>());
        return services;
    }

    public static IDurableTaskWorkerBuilder AddDurableJobPipelineTasks(this IDurableTaskWorkerBuilder builder)
        => builder.AddTasks(registry =>
        {
            registry.AddOrchestrator<PipelineOrchestrator>();
            registry.AddActivity<PreparePipelineActivity>();
            registry.AddActivity<ExecuteJobActivity>();
            registry.AddActivity<UpdateJobStatusActivity>();
            registry.AddActivity<CompletePipelineActivity>();
        });
}
