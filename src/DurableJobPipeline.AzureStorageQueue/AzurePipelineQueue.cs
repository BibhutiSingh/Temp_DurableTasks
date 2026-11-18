using System.Text.Json;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using DurableJobPipeline;

namespace DurableJobPipeline.AzureStorageQueue;

public sealed class AzurePipelineQueue
{
    private readonly QueueClient _queue;
    public AzurePipelineQueue(string connectionString, string queueName = "pipeline-runs")
        => _queue = new QueueClient(connectionString, queueName);
    public AzurePipelineQueue(Uri queueUri, Azure.Core.TokenCredential credential)
        => _queue = new QueueClient(queueUri, credential);
    public Task EnsureCreatedAsync(CancellationToken ct = default) => _queue.CreateIfNotExistsAsync(cancellationToken: ct);
    public Task EnqueueAsync(PipelineTriggerRequest request, CancellationToken ct = default)
        => _queue.SendMessageAsync(JsonSerializer.Serialize(request, PipelineJson.Options), cancellationToken: ct);
}
