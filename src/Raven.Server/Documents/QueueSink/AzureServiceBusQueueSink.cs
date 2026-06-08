using Raven.Client.Documents.Operations.QueueSink;
using Raven.Server.Documents.ETL.Providers.Queue;

namespace Raven.Server.Documents.QueueSink;

public sealed class AzureServiceBusQueueSink : QueueSinkProcess
{
    public AzureServiceBusQueueSink(QueueSinkConfiguration configuration, QueueSinkScript script,
        DocumentDatabase database, string tag) : base(configuration, script, database, tag)
    {
    }

    protected override IQueueSinkConsumer CreateConsumer()
    {
        var client = QueueBrokerConnectionHelper.CreateAzureServiceBusClient($"RavenDB-{Database.Name}-{Configuration.Name}", Configuration.Connection.AzureServiceBusConnectionSettings);
        var consumer = new AzureServiceBusSinkConsumer(client, Logger, CancellationToken);

        try
        {
            foreach (var entry in Script.Queues)
            {
                if (AzureServiceBusSinkSource.TryParseSubscription(entry, out var topic, out var subscription))
                    consumer.SubscribeToSubscription(topic, subscription);
                else
                    consumer.SubscribeToQueue(entry);
            }

            return consumer;
        }
        catch
        {
            consumer.Dispose();
            throw;
        }
    }
}
