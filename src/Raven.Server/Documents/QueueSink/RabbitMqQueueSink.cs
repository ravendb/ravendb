using System;
using RabbitMQ.Client;
using Raven.Client.Documents.Operations.QueueSink;

namespace Raven.Server.Documents.QueueSink;

public sealed class RabbitMqQueueSink : QueueSinkProcess
{
    public RabbitMqQueueSink(QueueSinkConfiguration configuration, QueueSinkScript script, DocumentDatabase database,
        string tag) : base(configuration, script, database, tag)
    {
    }

    protected override IQueueSinkConsumer CreateConsumer()
    {
        var channel = CreateRabbitMqChannel();
        var consumer = new RabbitMqSinkConsumer(channel);

        try
        {
            foreach (string queue in Script.Queues)
            {
                channel.BasicConsumeAsync(queue: queue, autoAck: false, consumer).GetAwaiter().GetResult();
            }
        }
        catch
        {
            consumer.Dispose();
            throw;
        }

        return consumer;
    }

    private IChannel CreateRabbitMqChannel()
    {
        var connectionFactory = new ConnectionFactory { Uri = new Uri(Configuration.Connection.RabbitMqConnectionSettings.ConnectionString) };
        var connection = connectionFactory.CreateConnectionAsync().GetAwaiter().GetResult();
        var channel = connection.CreateChannelAsync().GetAwaiter().GetResult();

        return channel;
    }
}
