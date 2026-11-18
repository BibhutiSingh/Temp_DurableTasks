using System.Text.Json;

namespace DurableJobPipeline;

public interface IJobHandler
{
    string JobType { get; }
    Task<object?> ExecuteAsync(JobExecutionContext context, CancellationToken cancellationToken);
}

public abstract class JobHandler<TParameters, TOutput> : IJobHandler
{
    public abstract string JobType { get; }

    public abstract Task<TOutput> ExecuteAsync(
        JobExecutionContext context,
        TParameters parameters,
        CancellationToken cancellationToken);

    async Task<object?> IJobHandler.ExecuteAsync(JobExecutionContext context, CancellationToken cancellationToken)
    {
        var parameters = context.Parameters.Deserialize<TParameters>(PipelineJson.Options)
            ?? throw new InvalidOperationException($"Unable to deserialize parameters for job type '{JobType}'.");
        return await ExecuteAsync(context, parameters, cancellationToken).ConfigureAwait(false);
    }
}

public sealed class JobExecutionContext
{
    public required Guid PipelineId { get; init; }
    public required string PipelineName { get; init; }
    public required Guid RunId { get; init; }
    public required Guid JobId { get; init; }
    public required string JobName { get; init; }
    public required JsonElement Parameters { get; init; }
    public required IReadOnlyDictionary<Guid, JobOutput> DependencyOutputs { get; init; }
}

public sealed class JobTypeAttribute(string name) : Attribute
{
    public string Name { get; } = !string.IsNullOrWhiteSpace(name)
        ? name
        : throw new ArgumentException("Job type name is required.", nameof(name));
}

internal static class JobTypeResolver
{
    public static string Resolve<TJob>() where TJob : IJobHandler
    {
        var attribute = typeof(TJob).GetCustomAttributes(typeof(JobTypeAttribute), true)
            .OfType<JobTypeAttribute>().SingleOrDefault();
        return attribute?.Name ?? typeof(TJob).Name;
    }
}
