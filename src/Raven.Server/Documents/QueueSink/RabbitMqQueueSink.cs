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
                channel.BasicConsume(queue: queue, autoAck: false, consumer);
            }
        }
        catch
        {
            consumer.Dispose();
            throw;
        }

        return consumer;
    }

    private IModel CreateRabbitMqChannel()
    {
        var connectionFactory = new ConnectionFactory { Uri = new Uri(Configuration.Connection.RabbitMqConnectionSettings.ConnectionString) };
        var connection = connectionFactory.CreateConnection();
        var channel = connection.CreateModel();

        return channel;
    }
}
