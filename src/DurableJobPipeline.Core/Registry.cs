using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace DurableJobPipeline;

public sealed record JobHandlerRegistration(string JobType, Type HandlerType);

public interface IJobHandlerRegistry
{
    IJobHandler Create(string jobType, IServiceProvider services);
}

public sealed class JobHandlerRegistry(IEnumerable<JobHandlerRegistration> registrations) : IJobHandlerRegistry
{
    private readonly IReadOnlyDictionary<string, Type> _types = registrations
        .GroupBy(x => x.JobType, StringComparer.OrdinalIgnoreCase)
        .ToDictionary(x => x.Key, x => x.Last().HandlerType, StringComparer.OrdinalIgnoreCase);

    public IJobHandler Create(string jobType, IServiceProvider services)
    {
        if (!_types.TryGetValue(jobType, out var type)) throw new InvalidOperationException($"No job handler registered for '{jobType}'.");
        return (IJobHandler)(services.GetService(type) ?? ActivatorUtilities.CreateInstance(services, type));
    }
}

public sealed class DurablePipelineWorkerOptions
{
    public WorkerCapacityMode CapacityMode { get; set; } = WorkerCapacityMode.Automatic;
    public int MaxConcurrentJobs { get; set; } = 0;
    public int ReserveCpuCores { get; set; } = 1;
    public int MemoryPerJobMb { get; set; } = 512;
    public int IoMultiplier { get; set; } = 4;
    public int MaxAutomaticConcurrency { get; set; } = 32;
    public int BlobOutputThresholdBytes { get; set; } = 1024 * 1024;
}

public sealed class JobExecutionLimiter(IOptions<DurablePipelineWorkerOptions> options)
{
    private readonly SemaphoreSlim _gate = new(Calculate(options.Value));
    private static int Calculate(DurablePipelineWorkerOptions options)
    {
        if (options.MaxConcurrentJobs > 0 || options.CapacityMode == WorkerCapacityMode.Manual)
            return Math.Max(1, options.MaxConcurrentJobs);
        if (options.CapacityMode == WorkerCapacityMode.Functions)
            return Math.Max(1, options.MaxConcurrentJobs > 0 ? options.MaxConcurrentJobs : 4);
        var cpu = Math.Max(1, Environment.ProcessorCount - options.ReserveCpuCores) * Math.Max(1, options.IoMultiplier);
        var memoryMb = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes / (1024 * 1024);
        var memory = options.MemoryPerJobMb <= 0 || memoryMb <= 0 ? int.MaxValue : (int)Math.Max(1, memoryMb / options.MemoryPerJobMb);
        return Math.Max(1, Math.Min(options.MaxAutomaticConcurrency, Math.Min(cpu, memory)));
    }
    public Task WaitAsync(CancellationToken ct) => _gate.WaitAsync(ct);
    public void Release() => _gate.Release();
}

public static class CoreServiceRegistration
{
    public static IServiceCollection AddDurableJobPipelineCore(this IServiceCollection services, Action<DurablePipelineWorkerOptions>? configure = null)
    {
        services.TryAddSingleton<IJobHandlerRegistry, JobHandlerRegistry>();
        services.AddOptions<DurablePipelineWorkerOptions>();
        if (configure is not null) services.Configure(configure);
        services.TryAddSingleton<JobExecutionLimiter>();
        return services;
    }

    public static IServiceCollection AddDurableJobHandler<THandler>(this IServiceCollection services) where THandler : class, IJobHandler
        => services.AddDurableJobHandler<THandler>(JobTypeResolver.Resolve<THandler>());

    public static IServiceCollection AddDurableJobHandler<THandler>(this IServiceCollection services, string jobType) where THandler : class, IJobHandler
    {
        services.AddScoped<THandler>();
        services.AddSingleton(new JobHandlerRegistration(jobType, typeof(THandler)));
        return services;
    }
}
