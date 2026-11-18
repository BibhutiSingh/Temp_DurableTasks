using System.Net;
using System.Text.Json;
using DurableJobPipeline;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DurableJobPipeline.AzureFunctions;

public static class PipelineOrchestratorFunctions
{
    [Function("PipelineOrchestrator")]
    public static async Task<PipelineRunResult> Run(
        [OrchestrationTrigger] TaskOrchestrationContext context,
        PipelineStartInput input)
    {
        var pipeline = await context.CallActivityAsync<PipelineDefinition>("LoadPipeline", input);
        var runId = Guid.Parse(context.InstanceId);
        var prior = input.ResumeFromRunId == Guid.Empty
            ? Array.Empty<JobRunRecord>()
            : await context.CallActivityAsync<JobRunRecord[]>("LoadPreviousJobs", input.ResumeFromRunId);

        var resume = BuildResume(pipeline, prior, input.RestartFromJobId, runId);
        await context.CallActivityAsync("CreateRun", new PipelineRunRecord(runId, pipeline.Id, pipeline.Name, PipelineRunStatus.Running, context.CurrentUtcDateTime, null, input.Parameters.Clone()));

        var outputs = new Dictionary<Guid, JobOutput>(resume);
        var pending = pipeline.Jobs.Where(j => !outputs.ContainsKey(j.Id)).ToDictionary(j => j.Id);
        var shortCircuit = false;

        while (pending.Count > 0)
        {
            var ready = pending.Values.Where(j => j.DependsOn.All(outputs.ContainsKey)).OrderBy(j => j.Name, StringComparer.Ordinal).ToList();
            if (ready.Count == 0) throw new InvalidOperationException($"Pipeline '{pipeline.Name}' contains an unsatisfied dependency graph.");

            foreach (var job in ready.Where(j => j.DependsOn.Any(d => outputs[d].JobStatus == JobStatus.Canceled)).ToList())
            {
                var skipped = new JobOutput(pipeline.Id, runId, job.Id, JobStatus.Skipped, null, "A dependency was canceled.");
                await context.CallActivityAsync("SetJobStatus", new JobRunRecord(runId, job.Id, skipped.JobStatus, null, skipped.Error, context.CurrentUtcDateTime, context.CurrentUtcDateTime));
                outputs[job.Id] = skipped; pending.Remove(job.Id);
            }

            foreach (var job in ready.Where(j => pending.ContainsKey(j.Id) && j.DependsOn.Any(d => outputs[d].JobStatus != JobStatus.Completed)).ToList())
            {
                var skipped = new JobOutput(pipeline.Id, runId, job.Id, JobStatus.Skipped, null, "A dependency did not complete successfully.");
                await context.CallActivityAsync("SetJobStatus", new JobRunRecord(runId, job.Id, skipped.JobStatus, null, skipped.Error, context.CurrentUtcDateTime, context.CurrentUtcDateTime));
                outputs[job.Id] = skipped; pending.Remove(job.Id);
            }

            if (shortCircuit)
            {
                foreach (var job in pending.Values.ToList())
                {
                    var skipped = new JobOutput(pipeline.Id, runId, job.Id, JobStatus.Skipped, null, "Pipeline was short-circuited.");
                    await context.CallActivityAsync("SetJobStatus", new JobRunRecord(runId, job.Id, skipped.JobStatus, null, skipped.Error, context.CurrentUtcDateTime, context.CurrentUtcDateTime));
                    outputs[job.Id] = skipped; pending.Remove(job.Id);
                }
                break;
            }

            var runnable = pending.Values.Where(j => ready.Contains(j) && j.DependsOn.All(d => outputs[d].JobStatus == JobStatus.Completed)).ToList();
            if (runnable.Count == 0) continue;

            var tasks = runnable.Select(job =>
                context.CallActivityAsync<JobOutput>("ExecuteJob", new JobExecutionRequest(
                    pipeline.Id, pipeline.Name, runId, job.Id, job.Name, job.JobType,
                    input.Parameters.Clone(), job.DependsOn.ToDictionary(d => d, d => outputs[d])))).ToArray();

            for (var i = 0; i < tasks.Length; i++)
            {
                JobOutput result;
                try { result = await tasks[i]; }
                catch (Exception ex) { result = new JobOutput(pipeline.Id, runId, runnable[i].Id, JobStatus.Failed, null, ex.ToString()); }
                outputs[result.JobId] = result;
                pending.Remove(result.JobId);
                if (result.JobStatus == JobStatus.Canceled || (result.JobStatus == JobStatus.Failed && pipeline.FailureMode == PipelineFailureMode.ShortCircuit)) shortCircuit = true;
            }
        }

        var status = outputs.Values.Any(x => x.JobStatus == JobStatus.Canceled) ? PipelineRunStatus.Canceled
            : outputs.Values.Any(x => x.JobStatus == JobStatus.Failed) ? PipelineRunStatus.Failed : PipelineRunStatus.Completed;
        await context.CallActivityAsync("CompleteRun", new CompletePipelineRequest(runId, status));
        return new PipelineRunResult(pipeline.Id, runId, status, pipeline.Jobs.Select(j => outputs[j.Id]).ToArray());
    }

    private static Dictionary<Guid, JobOutput> BuildResume(PipelineDefinition pipeline, IEnumerable<JobRunRecord> prior, Guid restartRoot, Guid newRunId)
    {
        var descendants = restartRoot == Guid.Empty ? [] : GetDescendants(pipeline, restartRoot);
        return prior.Where(x => x.Status == JobStatus.Completed && (restartRoot == Guid.Empty || !descendants.Contains(x.JobId)))
            .ToDictionary(x => x.JobId, x => new JobOutput(pipeline.Id, newRunId, x.JobId, JobStatus.Completed, x.Output, x.Error, x.BlobUri));
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

public sealed class PipelineActivityFunctions(
    IPipelineStorage storage,
    IJobHandlerRegistry handlers,
    IServiceScopeFactory scopeFactory,
    JobExecutionLimiter limiter,
    ILogger<PipelineActivityFunctions> logger)
{
    [Function("LoadPipeline")]
    public async Task<PipelineDefinition> LoadPipeline([ActivityTrigger] PipelineStartInput input)
    {
        var pipeline = input.PipelineId != Guid.Empty
            ? await storage.GetPipelineAsync(input.PipelineId)
            : await storage.GetPipelineByNameAsync(input.PipelineName);
        pipeline ??= throw new InvalidOperationException($"Pipeline '{input.PipelineName}' was not found.");
        PipelineValidator.Validate(pipeline);
        return pipeline;
    }

    [Function("LoadPreviousJobs")]
    public async Task<JobRunRecord[]> LoadPreviousJobs([ActivityTrigger] Guid runId)
        => (await storage.GetJobRunsAsync(runId)).ToArray();

    [Function("CreateRun")]
    public Task CreateRun([ActivityTrigger] PipelineRunRecord run) => storage.CreateRunAsync(run);

    [Function("SetJobStatus")]
    public Task SetJobStatus([ActivityTrigger] JobRunRecord job) => storage.SetJobStatusAsync(job);

    [Function("CompleteRun")]
    public async Task CompleteRun([ActivityTrigger] CompletePipelineRequest request)
    {
        var status = await storage.IsCancellationRequestedAsync(request.RunId) ? PipelineRunStatus.Canceled : request.Status;
        await storage.SetPipelineRunStatusAsync(request.RunId, status, DateTimeOffset.UtcNow);
    }

    [Function("ExecuteJob")]
    public async Task<JobOutput> ExecuteJob([ActivityTrigger] JobExecutionRequest input, CancellationToken cancellationToken)
    {
        var started = DateTimeOffset.UtcNow;
        await storage.SetJobStatusAsync(new JobRunRecord(input.RunId, input.JobId, JobStatus.Running, null, null, started, null));
        await limiter.WaitAsync(cancellationToken);
        try
        {
            await using var scope = scopeFactory.CreateAsyncScope();
            var handler = handlers.Create(input.JobType, scope.ServiceProvider);
            var result = await handler.ExecuteAsync(new JobExecutionContext
            {
                PipelineId = input.PipelineId, PipelineName = input.PipelineName, RunId = input.RunId,
                JobId = input.JobId, JobName = input.JobName, Parameters = input.Parameters,
                DependencyOutputs = input.DependencyOutputs
            }, cancellationToken);
            var output = result is null ? null : JsonSerializer.SerializeToElement(result, PipelineJson.Options);
            await storage.SetJobStatusAsync(new JobRunRecord(input.RunId, input.JobId, JobStatus.Completed, output, null, started, DateTimeOffset.UtcNow));
            return new JobOutput(input.PipelineId, input.RunId, input.JobId, JobStatus.Completed, output);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            const string error = "Activity canceled by host or platform.";
            await storage.SetJobStatusAsync(new JobRunRecord(input.RunId, input.JobId, JobStatus.Canceled, null, error, started, DateTimeOffset.UtcNow));
            return new JobOutput(input.PipelineId, input.RunId, input.JobId, JobStatus.Canceled, null, error);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Job {JobId} in run {RunId} failed", input.JobId, input.RunId);
            await storage.SetJobStatusAsync(new JobRunRecord(input.RunId, input.JobId, JobStatus.Failed, null, ex.ToString(), started, DateTimeOffset.UtcNow));
            throw;
        }
        finally { limiter.Release(); }
    }
}

public sealed class PipelineTriggerFunctions(IPipelineStorage storage)
{
    [Function("StartPipeline")]
    public async Task<HttpResponseData> StartPipeline(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "pipelines/{pipelineName}/runs")] HttpRequestData req,
        string pipelineName,
        [DurableClient] DurableTaskClient client,
        CancellationToken ct)
    {
        var parameters = await ReadJsonAsync(req, ct);
        var runId = Guid.NewGuid();
        await client.ScheduleNewOrchestrationInstanceAsync(new TaskName("PipelineOrchestrator"),
            new PipelineStartInput(Guid.Empty, pipelineName, parameters), new StartOrchestrationOptions(runId.ToString("N")), ct);
        var response = req.CreateResponse(HttpStatusCode.Accepted);
        await response.WriteAsJsonAsync(new { pipelineName, runId, status = "Pending" }, ct);
        return response;
    }

    [Function("QueueStartPipeline")]
    public async Task QueueStartPipeline(
        [QueueTrigger("pipeline-runs", Connection = "AzureWebJobsStorage")] string message,
        [DurableClient] DurableTaskClient client,
        CancellationToken ct)
    {
        var request = JsonSerializer.Deserialize<PipelineTriggerRequest>(message, PipelineJson.Options)
            ?? throw new InvalidOperationException("Invalid pipeline queue message.");
        var runId = Guid.NewGuid();
        await client.ScheduleNewOrchestrationInstanceAsync(new TaskName("PipelineOrchestrator"),
            new PipelineStartInput(Guid.Empty, request.PipelineName, request.Parameters), new StartOrchestrationOptions(runId.ToString("N")), ct);
    }

    [Function("GetRun")]
    public async Task<HttpResponseData> GetRun(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "runs/{runId:guid}")] HttpRequestData req,
        Guid runId,
        CancellationToken ct)
    {
        var run = await storage.GetPipelineRunAsync(runId, ct);
        if (run is null) return req.CreateResponse(HttpStatusCode.NotFound);
        var jobs = await storage.GetJobRunsAsync(runId, ct);
        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new { run, jobs }, ct);
        return response;
    }

    [Function("CancelPipeline")]
    public async Task<HttpResponseData> CancelPipeline(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "runs/{runId:guid}/cancel")] HttpRequestData req,
        Guid runId,
        [DurableClient] DurableTaskClient client,
        CancellationToken ct)
    {
        PipelineControlRequest? control = null;
        if (req.Body.CanRead)
        {
            try { control = await JsonSerializer.DeserializeAsync<PipelineControlRequest>(req.Body, PipelineJson.Options, ct); }
            catch (JsonException) { }
        }
        await storage.SetCancellationRequestedAsync(runId, control?.Reason, ct);
        await storage.SetPipelineRunStatusAsync(runId, PipelineRunStatus.Canceled, DateTimeOffset.UtcNow, ct);
        await client.TerminateInstanceAsync(runId.ToString("N"), "Cancellation requested", ct);
        var response = req.CreateResponse(HttpStatusCode.Accepted);
        await response.WriteAsJsonAsync(new { runId, status = "CancellationRequested" }, ct);
        return response;
    }

    [Function("RestartPipeline")]
    public async Task<HttpResponseData> RestartPipeline(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "runs/{runId:guid}/restart")] HttpRequestData req,
        Guid runId,
        [DurableClient] DurableTaskClient client,
        CancellationToken ct)
    {
        var run = await storage.GetPipelineRunAsync(runId, ct) ?? throw new InvalidOperationException("Run was not found.");
        if (run.Status is not PipelineRunStatus.Failed and not PipelineRunStatus.Canceled)
            return req.CreateResponse(HttpStatusCode.Conflict);
        Guid? requestedJob = null;
        if (req.Body.CanRead)
        {
            try
            {
                var restartRequest = await JsonSerializer.DeserializeAsync<RestartPipelineRequest>(req.Body, PipelineJson.Options, ct);
                requestedJob = restartRequest?.RestartFromJobId;
            }
            catch (JsonException) { }
        }
        var restartJob = requestedJob
            ?? (await storage.GetJobRunsAsync(runId, ct)).FirstOrDefault(x => x.Status == JobStatus.Failed)?.JobId
            ?? Guid.Empty;
        var newId = Guid.NewGuid();
        await client.ScheduleNewOrchestrationInstanceAsync(new TaskName("PipelineOrchestrator"),
            new PipelineStartInput(run.PipelineId, run.PipelineName, run.Parameters.Clone(), runId, restartJob),
            new StartOrchestrationOptions(newId.ToString("N")), ct);
        var response = req.CreateResponse(HttpStatusCode.Accepted);
        await response.WriteAsJsonAsync(new { sourceRunId = runId, newRunId = newId, status = "Pending" }, ct);
        return response;
    }

    private static async Task<JsonElement> ReadJsonAsync(HttpRequestData req, CancellationToken ct)
    {
        if (req.Body.CanSeek && req.Body.Length == 0) return JsonSerializer.SerializeToElement(new { });
        try
        {
            var body = await JsonSerializer.DeserializeAsync<JsonElement>(req.Body, PipelineJson.Options, ct);
            return body.ValueKind == JsonValueKind.Undefined ? JsonSerializer.SerializeToElement(new { }) : body;
        }
        catch (JsonException) { return JsonSerializer.SerializeToElement(new { }); }
    }
}

public sealed record CompletePipelineRequest(Guid RunId, PipelineRunStatus Status);
public sealed record RestartPipelineRequest(Guid? RestartFromJobId = null);
