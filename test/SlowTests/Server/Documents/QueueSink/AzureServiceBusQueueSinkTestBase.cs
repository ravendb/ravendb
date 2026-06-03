using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.QueueSink;

[Trait("Category", "QueueSink")]
[Collection(AzureServiceBusTestCollection.Name)]
public abstract class AzureServiceBusQueueSinkTestBase : QueueSinkTestBase
{
    private readonly AzureServiceBusTestHelper _serviceBusHelper = new();

    protected AzureServiceBusQueueSinkTestBase(ITestOutputHelper output) : base(output)
    {
    }

    protected static string GetConnectionString() => AzureServiceBusHelper.GetConnectionString();

    protected static string GetAdminConnectionString() => AzureServiceBusHelper.GetAdminConnectionString();

    protected QueueSinkConfiguration SetupAzureServiceBusQueueSink(DocumentStore store, string script, List<string> queues,
        string configurationName = null, string transformationName = null, bool disabled = false)
    {
        var connectionStringName = $"AzureServiceBus to {store.Database}";

        var queueSinkScript = new QueueSinkScript
        {
            Name = transformationName ?? $"Queue Sink : {connectionStringName}",
            Queues = new List<string>(queues),
            Script = script,
        };

        var config = new QueueSinkConfiguration
        {
            Name = configurationName ?? connectionStringName,
            ConnectionStringName = connectionStringName,
            Scripts = { queueSinkScript },
            BrokerType = QueueBrokerType.AzureServiceBus,
            Disabled = disabled
        };

        AddQueueSink(store, config,
            new QueueConnectionString
            {
                Name = connectionStringName,
                BrokerType = QueueBrokerType.AzureServiceBus,
                AzureServiceBusConnectionSettings = new AzureServiceBusConnectionSettings
                {
                    ConnectionString = GetConnectionString()
                }
            });

        return config;
    }

    protected ServiceBusClient CreateAzureServiceBusClient() => _serviceBusHelper.CreateClient();

    protected Task<ServiceBusSender> CreateAzureServiceBusProducerAsync(ServiceBusClient client, params string[] queuesToCreate)
        => _serviceBusHelper.CreateProducerAsync(client, queuesToCreate);

    protected Task<ServiceBusSender> CreateAzureServiceBusTopicProducerAsync(ServiceBusClient client, string topic, params string[] subscriptionsToCreate)
        => _serviceBusHelper.CreateTopicProducerAsync(client, topic, subscriptionsToCreate);

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await _serviceBusHelper.DisposeAsync();
    }
}
