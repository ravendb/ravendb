using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.QueueSink;

[CollectionDefinition(Name, DisableParallelization = true)]
public class AzureServiceBusTestCollection
{
    public const string Name = "AzureServiceBus";
}

internal sealed class AzureServiceBusTestHelper : IAsyncDisposable
{
    private static readonly Lazy<ServiceBusAdministrationClient> SharedAdminClient = new(
        () => new ServiceBusAdministrationClient(AzureServiceBusHelper.GetAdminConnectionString()));

    private static readonly Lazy<ServiceBusClient> SharedClient = new(
        () => new ServiceBusClient(AzureServiceBusHelper.GetConnectionString()));

    private static ServiceBusAdministrationClient AdminClient => SharedAdminClient.Value;

    private readonly HashSet<string> _definedQueues = new();
    private readonly HashSet<string> _definedTopics = new();

    public ServiceBusClient CreateClient() => SharedClient.Value;

    public async Task<ServiceBusSender> CreateProducerAsync(ServiceBusClient client, params string[] queuesToCreate)
    {
        foreach (var queue in queuesToCreate)
        {
            if (await AdminClient.QueueExistsAsync(queue) == false)
            {
                await AdminClient.CreateQueueAsync(queue);
            }

            _definedQueues.Add(queue);
        }

        return queuesToCreate.Length > 0 ? client.CreateSender(queuesToCreate[0]) : null;
    }

    public async Task<ServiceBusSender> CreateTopicProducerAsync(ServiceBusClient client, string topic, params string[] subscriptionsToCreate)
    {
        if (await AdminClient.TopicExistsAsync(topic) == false)
        {
            await AdminClient.CreateTopicAsync(topic);
        }

        _definedTopics.Add(topic);

        foreach (var subscription in subscriptionsToCreate)
        {
            if (await AdminClient.SubscriptionExistsAsync(topic, subscription) == false)
            {
                await AdminClient.CreateSubscriptionAsync(topic, subscription);
            }
        }

        return client.CreateSender(topic);
    }

    public async ValueTask DisposeAsync()
    {
        if (_definedQueues.Count == 0 && _definedTopics.Count == 0)
            return;

        if (string.IsNullOrEmpty(AzureServiceBusHelper.GetAdminConnectionString()))
            return;

        foreach (var queue in _definedQueues)
        {
            try
            {
                if (await AdminClient.QueueExistsAsync(queue))
                    await AdminClient.DeleteQueueAsync(queue);
            }
            catch
            {
                // ignore cleanup errors
            }
        }

        foreach (var topic in _definedTopics)
        {
            try
            {
                if (await AdminClient.TopicExistsAsync(topic))
                    await AdminClient.DeleteTopicAsync(topic);
            }
            catch
            {
                // ignore cleanup errors
            }
        }
    }
}
