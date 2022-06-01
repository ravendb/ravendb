using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Tests.Infrastructure.ConnectionString;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue;

public class KafkaEtlTestBase : QueueEtlTestBase
{
    private readonly HashSet<string> _definedTopics = new HashSet<string>();

    protected KafkaEtlTestBase(ITestOutputHelper output) : base(output)
    {
        TopicSuffix = Guid.NewGuid().ToString().Replace("-", string.Empty);
    }

    protected string TopicSuffix { get; }

    protected string OrdersTopicName => $"Orders{TopicSuffix}";

    protected readonly string[] DefaultCollections = { "Orders" };

    protected List<EtlQueue> DefaultTopics => new() { new EtlQueue { Name = OrdersTopicName } };

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
loadToOrders" + TopicSuffix + @"(orderData);
";

    protected QueueEtlConfiguration SetupQueueEtlToKafka(DocumentStore store, string script,
        IEnumerable<string> collections, IEnumerable<EtlQueue> queues = null, bool applyToAllDocuments = false, string configurationName = null,
        string transformationName = null,
        Dictionary<string, string> configuration = null)
    {
        var connectionStringName = $"{store.Database}@{store.Urls.First()} to Kafka";

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
            Queues = queues?.ToList()
        };

        foreach (var queue in queues?.Select(x => x.Name).ToArray() ?? transformation.GetCollectionsFromScript())
        {
            _definedTopics.Add(queue);
        }

        AddEtl(store, config,
            new QueueConnectionString
            {
                Name = connectionStringName,
                BrokerType = QueueBroker.Kafka,
                KafkaConnectionSettings = new KafkaConnectionSettings() { ConnectionOptions = configuration, Url = KafkaConnectionString.Instance.VerifiedUrl.Value }
            });
        return config;
    }

    protected IConsumer<string, byte[]> CreateKafkaConsumer(IEnumerable<string> topics)
    {
        var consumerConfig = new ConsumerConfig()
        {
            BootstrapServers = KafkaConnectionString.Instance.VerifiedUrl.Value,
            GroupId = "test",
            IsolationLevel = IsolationLevel.ReadCommitted,
            EnablePartitionEof = true,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();
        consumer.Subscribe(topics);
        return consumer;
    }

    private void CleanupTopic()
    {
        if (_definedTopics.Count == 0)
            return;

        var config = new AdminClientConfig { BootstrapServers = KafkaConnectionString.Instance.VerifiedUrl.Value };
        var adminClient = new AdminClientBuilder(config).Build();

        try
        {
            adminClient.DeleteTopicsAsync(_definedTopics).Wait();
        }
        catch (Exception e)
        {
            throw new InvalidOperationException($"Failed to cleanup topics: {string.Join(", ", _definedTopics)}. Check inner exceptions for details", e);
        }
    }

    public override void Dispose()
    {
        base.Dispose();
        CleanupTopic();
    }
}
