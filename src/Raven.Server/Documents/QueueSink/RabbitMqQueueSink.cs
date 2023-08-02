using System;
using System.Threading;
using RabbitMQ.Client;
using Raven.Client.Documents.Operations.QueueSink;

namespace Raven.Server.Documents.QueueSink;

public class RabbitMqQueueSink : QueueSinkProcess
{
    public RabbitMqQueueSink(QueueSinkConfiguration configuration, QueueSinkScript script, DocumentDatabase database,
        string tag, CancellationToken shutdown) : base(configuration, script, database, tag, shutdown)
    {
    }

    protected override IQueueSinkConsumer CreateConsumer()
    {
        var channel = CreateRabbitMqChannel();
        var consumer = new RabbitMqSinkConsumer(channel);
        foreach (string queue in Script.Queues)
        {
            channel.BasicConsume(queue: queue, autoAck: false, consumer);
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
