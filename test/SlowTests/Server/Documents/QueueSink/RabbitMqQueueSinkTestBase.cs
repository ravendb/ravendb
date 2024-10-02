using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using System;
using Tests.Infrastructure.ConnectionString;
using Xunit;
using Xunit.Abstractions;
using System.Collections.Generic;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;

namespace SlowTests.Server.Documents.QueueSink;

[Trait("Category", "QueueSink")]
public abstract class RabbitMqQueueSinkTestBase : QueueSinkTestBase
{
    private readonly HashSet<string> _definedQueues = new();

    protected RabbitMqQueueSinkTestBase(ITestOutputHelper output) : base(output)
    {
    }

    protected QueueSinkConfiguration SetupRabbitMqQueueSink(DocumentStore store, string script, List<string> queues,
        string configurationName = null, string transformationName = null, bool disabled = false)
    {
        var connectionStringName = $"RabbitMQ to {store.Database}";

        QueueSinkScript queueSinkScript = new QueueSinkScript
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
            BrokerType = QueueBrokerType.RabbitMq,
            Disabled = disabled
        };

        AddQueueSink(store, config,
            new QueueConnectionString
            {
                Name = connectionStringName,
                BrokerType = QueueBrokerType.RabbitMq,
                RabbitMqConnectionSettings = new RabbitMqConnectionSettings
                {
                    ConnectionString = RabbitMqConnectionString.Instance.VerifiedConnectionString
                }
            });

        return config;
    }

    protected IModel CreateRabbitMqProducer(params string[] queuesToDeclare)
    {
        var connectionFactory = new ConnectionFactory() { Uri = new Uri(RabbitMqConnectionString.Instance.VerifiedConnectionString) };
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
        if (_definedQueues.Count == 0 || RabbitMqConnectionString.Instance.CanConnect == false)
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
