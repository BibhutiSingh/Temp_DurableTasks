using DurableJobPipeline;

namespace DurableJobPipeline.Tests;

public class PipelineBuilderTests
{
    [Fact]
    public async Task SetupPipeline_PreservesDependencies()
    {
        var storage = new InMemoryPipelineStorage();
        var setup = new PipelineSetup(storage);
        var pipeline = await setup.SetupPipeline("p", p =>
        {
            var a = p.AddJob<TestJob>("A");
            p.AddJob<TestJob2>("B", a);
            p.AddJob<TestJob2>("C");
        });
        Assert.Equal(3, pipeline.Jobs.Count);
        Assert.Single(pipeline.Jobs.Single(x => x.Name == "B").DependsOn);
        Assert.Empty(pipeline.Jobs.Single(x => x.Name == "C").DependsOn);
    }

    [Fact]
    public async Task SetupPipeline_ReusesExistingPipelineId()
    {
        var storage = new InMemoryPipelineStorage();
        var setup = new PipelineSetup(storage);
        var first = await setup.SetupPipeline("p", p => p.AddJob<TestJob>("A"));
        var second = await setup.SetupPipeline("p", p => p.AddJob<TestJob>("A"));
        Assert.Equal(first.Id, second.Id);
    }

    private sealed class TestJob : JobHandler<object, object>
    {
        public override string JobType => "test";
        public override Task<object> ExecuteAsync(JobExecutionContext context, object parameters, CancellationToken cancellationToken)
            => Task.FromResult<object>(new { ok = true });
    }
    private sealed class TestJob2 : JobHandler<object, object>
    {
        public override string JobType => "test2";
        public override Task<object> ExecuteAsync(JobExecutionContext context, object parameters, CancellationToken cancellationToken)
            => Task.FromResult<object>(new { ok = true });
    }

    private sealed class InMemoryPipelineStorage : IPipelineStorage
    {
        private PipelineDefinition? _pipeline;
        public Task SavePipelineAsync(PipelineDefinition pipeline, CancellationToken cancellationToken = default) { _pipeline = pipeline; return Task.CompletedTask; }
        public Task<PipelineDefinition?> GetPipelineByNameAsync(string pipelineName, CancellationToken cancellationToken = default) => Task.FromResult(_pipeline?.Name == pipelineName ? _pipeline : null);
        public Task<PipelineDefinition?> GetPipelineAsync(Guid pipelineId, CancellationToken cancellationToken = default) => Task.FromResult(_pipeline?.Id == pipelineId ? _pipeline : null);
        public Task CreateRunAsync(PipelineRunRecord run, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task SetPipelineRunStatusAsync(Guid runId, PipelineRunStatus status, DateTimeOffset? completedAt = null, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<PipelineRunRecord?> GetPipelineRunAsync(Guid runId, CancellationToken cancellationToken = default) => Task.FromResult<PipelineRunRecord?>(null);
        public Task SetCancellationRequestedAsync(Guid runId, string? reason = null, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<bool> IsCancellationRequestedAsync(Guid runId, CancellationToken cancellationToken = default) => Task.FromResult(false);
        public Task SetJobStatusAsync(JobRunRecord job, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<JobRunRecord?> GetJobRunAsync(Guid runId, Guid jobId, CancellationToken cancellationToken = default) => Task.FromResult<JobRunRecord?>(null);
        public Task<IReadOnlyList<JobRunRecord>> GetJobRunsAsync(Guid runId, CancellationToken cancellationToken = default) => Task.FromResult<IReadOnlyList<JobRunRecord>>([]);
    }
}
