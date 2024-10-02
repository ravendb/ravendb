using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Tests.Infrastructure.ConnectionString;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue;

public class RabbitMqEtlTestBase : QueueEtlTestBase
{
    protected class TestRabbitMqConsumer : DefaultBasicConsumer
    {
        private readonly BlockingCollection<(byte[] Body, IBasicProperties Properties)> _deliveries = new();

        public TestRabbitMqConsumer(IModel model) : base(model)
        {
        }

        public override void HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey, IBasicProperties properties, ReadOnlyMemory<byte> body)
        {
            _deliveries.Add((body.ToArray(), properties));
        }

        public (byte[] Body, IBasicProperties Properties) Consume()
        {
            var result = _deliveries.TryTake(out var delivery, Timeout.Infinite, new CancellationToken());

            Assert.True(result, "Failed to consume message");

            return delivery;
        }
    }

    private readonly HashSet<string> _definedExchangesAndQueues = new();

    protected RabbitMqEtlTestBase(ITestOutputHelper output) : base(output)
    {
        ExchangeSuffix = Guid.NewGuid().ToString().Replace("-", string.Empty);
    }

    protected string ExchangeSuffix { get; }

    protected string OrdersExchangeName => $"Orders{ExchangeSuffix}";

    protected readonly string[] DefaultCollections = { "Orders" };

    protected List<EtlQueue> DefaultExchanges => new() { new EtlQueue { Name = OrdersExchangeName } };

    protected string DefaultScript => @"
var orderData = {
    Id: id(this),
    OrderLinesCount: this.OrderLines.length,
    TotalCost: 0
};

for (var i = 0; i < this.OrderLines.length; i++) {
    var line = this.OrderLines[i];
    orderData.TotalCost += line.Cost*line.Quantity;    
}
loadToOrders" + ExchangeSuffix + @"(orderData);
";

    protected QueueEtlConfiguration SetupQueueEtlToRabbitMq(DocumentStore store, string script,
        IEnumerable<string> collections, IEnumerable<EtlQueue> queues = null, bool applyToAllDocuments = false, string configurationName = null,
        string transformationName = null,
        Dictionary<string, string> configuration = null, string connectionString = null, bool skipAutomaticQueueDeclaration = false)
    {
        var connectionStringName = $"{store.Database}@{store.Urls.First()} to RabbitMq";

        Transformation transformation = new Transformation
        {
            Name = transformationName ?? $"ETL : {connectionStringName}",
            Collections = new List<string>(collections),
            Script = script,
            ApplyToAllDocuments = applyToAllDocuments
        };
        var config = new QueueEtlConfiguration
        {
            Name = configurationName ?? connectionStringName,
            ConnectionStringName = connectionStringName,
            Transforms =
            {
                transformation
            },
            Queues = queues?.ToList(),
            BrokerType = QueueBrokerType.RabbitMq,
            SkipAutomaticQueueDeclaration = skipAutomaticQueueDeclaration
        };

        foreach (var queue in queues?.Select(x => x.Name).ToArray() ?? transformation.GetCollectionsFromScript())
        {
            _definedExchangesAndQueues.Add(queue);
        }

        Etl.AddEtl(store, config,
            new QueueConnectionString
            {
                Name = connectionStringName,
                BrokerType = QueueBrokerType.RabbitMq,
                RabbitMqConnectionSettings = new RabbitMqConnectionSettings(){ConnectionString = connectionString ?? RabbitMqConnectionString.Instance.VerifiedConnectionString}
            });
        return config;
    }

    protected IModel CreateRabbitMqChannel() => RabbitMqConnectionString.Instance.CreateModel();

    private void CleanupExchangesAndQueues()
    {
        if (_definedExchangesAndQueues.Count == 0 || RabbitMqConnectionString.Instance.CanConnect == false)
            return;

        using var channel = CreateRabbitMqChannel();
        var consumer = new EventingBasicConsumer(channel);

        foreach (string definedExchangeAndQueue in _definedExchangesAndQueues)
        {
            consumer.Model.ExchangeDelete(definedExchangeAndQueue);
            consumer.Model.QueueDelete(definedExchangeAndQueue);
        }
    }

    public override void Dispose()
    {
        base.Dispose();
        CleanupExchangesAndQueues();
    }
}
