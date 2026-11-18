using Microsoft.Extensions.DependencyInjection;

namespace DurableJobPipeline.AzureFunctions;

public static class FunctionsRegistration
{
    public static IServiceCollection AddDurableJobPipelineFunctions(this IServiceCollection services, Action<DurablePipelineWorkerOptions>? configure = null)
    {
        services.AddDurableJobPipelineCore(configure);
        return services;
    }
}
