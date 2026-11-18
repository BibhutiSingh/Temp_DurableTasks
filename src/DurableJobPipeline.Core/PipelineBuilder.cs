namespace DurableJobPipeline;

public sealed class PipelineBuilder
{
    private readonly Guid _id;
    private readonly string _name;
    private readonly Dictionary<Guid, JobDefinitionBuilder> _jobs = new();

    internal PipelineBuilder(Guid id, string name) { _id = id; _name = name; }

    public JobDefinitionBuilder AddJob(string name, string jobType, params JobDefinitionBuilder[] dependsOn)
        => AddJob(name, jobType, 3, dependsOn);

    public JobDefinitionBuilder AddJob(string name, string jobType, int maxAttempts, params JobDefinitionBuilder[] dependsOn)
    {
        var job = new JobDefinitionBuilder(Guid.NewGuid(), name, jobType, dependsOn ?? []);
        job.WithMaxAttempts(maxAttempts);
        _jobs.Add(job.Id, job);
        return job;
    }

    public JobDefinitionBuilder AddJob<TJob>(string name, params JobDefinitionBuilder[] dependsOn)
        where TJob : IJobHandler => AddJob(name, JobTypeResolver.Resolve<TJob>(), dependsOn);

    public JobDefinitionBuilder AddJob<TJob>(string name, int maxAttempts, params JobDefinitionBuilder[] dependsOn)
        where TJob : IJobHandler => AddJob(name, JobTypeResolver.Resolve<TJob>(), maxAttempts, dependsOn);

    public PipelineBuilder ShortCircuitOnError() { FailureMode = PipelineFailureMode.ShortCircuit; return this; }
    public PipelineBuilder ContinueIndependentBranches() { FailureMode = PipelineFailureMode.ContinueIndependentBranches; return this; }

    internal PipelineFailureMode FailureMode { get; private set; } = PipelineFailureMode.ContinueIndependentBranches;

    internal PipelineDefinition Build()
    {
        var definitions = _jobs.Values.Select(j => new JobDefinition(j.Id, j.Name, j.JobType, j.DependsOn.Select(x => x.Id).ToArray(), j.MaxAttempts)).ToArray();
        var pipeline = new PipelineDefinition(_id, _name, definitions, FailureMode);
        PipelineValidator.Validate(pipeline);
        return pipeline;
    }
}

public sealed class JobDefinitionBuilder
{
    private readonly JobDefinitionBuilder[] _dependsOn;
    internal JobDefinitionBuilder(Guid id, string name, string jobType, JobDefinitionBuilder[] dependsOn)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Job name is required.", nameof(name));
        if (string.IsNullOrWhiteSpace(jobType)) throw new ArgumentException("Job type is required.", nameof(jobType));
        Id = id; Name = name; JobType = jobType; _dependsOn = dependsOn;
    }
    public Guid Id { get; }
    public string Name { get; }
    public string JobType { get; }
    public int MaxAttempts { get; private set; } = 3;
    public IReadOnlyList<JobDefinitionBuilder> DependsOn => _dependsOn;
    public JobDefinitionBuilder WithMaxAttempts(int maxAttempts)
    {
        if (maxAttempts < 1) throw new ArgumentOutOfRangeException(nameof(maxAttempts));
        MaxAttempts = maxAttempts; return this;
    }
}

public sealed class PipelineSetup(IPipelineStorage storage)
{
    public async Task<PipelineDefinition> SetupPipelineAsync(string name, Action<PipelineBuilder> setup, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Pipeline name is required.", nameof(name));
        ArgumentNullException.ThrowIfNull(setup);
        var existing = await storage.GetPipelineByNameAsync(name, cancellationToken).ConfigureAwait(false);
        return await SetupPipelineAsync(existing?.Id ?? Guid.NewGuid(), name, setup, cancellationToken).ConfigureAwait(false);
    }

    public Task<PipelineDefinition> SetupPipeline(string name, Action<PipelineBuilder> setup, CancellationToken cancellationToken = default)
        => SetupPipelineAsync(name, setup, cancellationToken);

    public async Task<PipelineDefinition> SetupPipelineAsync(Guid pipelineId, string name, Action<PipelineBuilder> setup, CancellationToken cancellationToken = default)
    {
        if (pipelineId == Guid.Empty) throw new ArgumentException("Pipeline id cannot be empty.", nameof(pipelineId));
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Pipeline name is required.", nameof(name));
        ArgumentNullException.ThrowIfNull(setup);
        var builder = new PipelineBuilder(pipelineId, name);
        setup(builder);
        var pipeline = builder.Build();
        await storage.SavePipelineAsync(pipeline, cancellationToken).ConfigureAwait(false);
        return pipeline;
    }
}

public static class PipelineFactory
{
    public static PipelineSetup CreateSetup(IPipelineStorage storage) => new(storage);
}
