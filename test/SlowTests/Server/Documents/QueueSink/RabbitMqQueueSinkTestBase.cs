using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using System;
using Tests.Infrastructure.ConnectionString;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using System.Collections.Generic;

namespace SlowTests.Server.Documents.QueueSink;

[Trait("Category", "QueueSink")]
public abstract class RabbitMqQueueSinkTestBase : QueueSinkTestBase
{
    private readonly HashSet<string> _definedQueues = new();

    protected RabbitMqQueueSinkTestBase(ITestOutputHelper output) : base(output)
    {
    }

    protected IModel CreateRabbitMqProducer(params string[] queuesToDeclare)
    {
        var connectionFactory = new ConnectionFactory() { Uri = new Uri(RabbitMqConnectionString.Instance.VerifiedConnectionString.Value) };
        var connection = connectionFactory.CreateConnection();
        var producer = connection.CreateModel();

        foreach (string queue in queuesToDeclare)
        {
            producer.QueueDeclare(queue, exclusive: false);
            _definedQueues.Add(queue);
        }

        return producer;
    }

    private void CleanupQueues()
    {
        if (_definedQueues.Count == 0 || RequiresRabbitMqRetryFactAttribute.CanConnect == false)
            return;

        using var channel = CreateRabbitMqProducer();
        var consumer = new EventingBasicConsumer(channel);

        foreach (string definedExchangeAndQueue in _definedQueues)
        {
            consumer.Model.QueueDelete(definedExchangeAndQueue);
        }
    }

    public override void Dispose()
    {
        base.Dispose();
        CleanupQueues();
    }
}
