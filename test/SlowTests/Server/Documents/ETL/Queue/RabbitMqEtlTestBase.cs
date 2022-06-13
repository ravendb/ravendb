using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Tests.Infrastructure.ConnectionString;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue;

public class RabbitMqEtlTestBase : QueueEtlTestBase
{
    private readonly HashSet<string> _definedTopics = new();
    protected readonly string DefaultConnectionString = "amqp://guest:guest@localhost:5672/";

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
        Dictionary<string, string> configuration = null)
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
            BrokerType = QueueBroker.RabbitMq
        };

        foreach (var queue in queues?.Select(x => x.Name).ToArray() ?? transformation.GetCollectionsFromScript())
        {
            _definedTopics.Add(queue);
        }

        AddEtl(store, config,
            new QueueConnectionString
            {
                Name = connectionStringName,
                BrokerType = QueueBroker.RabbitMq,
                RabbitMqConnectionSettings = new RabbitMqConnectionSettings(){ConnectionString = DefaultConnectionString}
            });
        return config;
    }

    protected IModel CreateRabbitMqConsumer()
    {
        var connectionFactory = new ConnectionFactory() { Uri = new Uri(DefaultConnectionString)};
        var connection = connectionFactory.CreateConnection();
        var channel = connection.CreateModel();

        return channel;
    }
    
    private void CleanupQueues()
    {
        var channel = CreateRabbitMqConsumer();
        var consumer = new EventingBasicConsumer(channel);

        foreach (string definedTopic in _definedTopics)
        {
            consumer.Model.QueueDelete(definedTopic);
        }
    }

    public override void Dispose()
    {
        base.Dispose();
        CleanupQueues();
    }
}
