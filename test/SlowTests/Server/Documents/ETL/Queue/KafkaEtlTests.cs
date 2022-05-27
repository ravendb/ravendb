using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.ETL.Providers.Queue;
using Raven.Server.Documents.ETL.Providers.Queue.Test;
using Raven.Server.ServerWide.Context;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue;

public class KafkaEtlTests : EtlTestBase
{
    public KafkaEtlTests(ITestOutputHelper output) : base(output)
    {
        TopicSuffix = Guid.NewGuid().ToString().Replace("-", string.Empty);
    }

    private string TopicSuffix { get; set; }
    private string OrdersTopicName => $"Orders{TopicSuffix}";
    private string[] DefaultCollections = { "Orders" };
    private List<EtlQueue> DefaultTopics => new() { new EtlQueue { Name = OrdersTopicName } };
    private string DefaultKafkaUrl = "localhost:29092";

    private string DefaultScript => @"
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

    [Fact]
    public void SimpleScript()
    {
        using (var store = GetDocumentStore())
        {
            var config = SetupQueueEtl(store, DefaultScript, DefaultTopics, DefaultCollections, url: DefaultKafkaUrl);
            var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

            using (var session = store.OpenSession())
            {
                session.Store(new Order
                {
                    Id = "orders/1-A",
                    OrderLines = new List<OrderLine>
                    {
                        new OrderLine { Cost = 3, Product = "Milk", Quantity = 2 },
                        new OrderLine { Cost = 4, Product = "Bear", Quantity = 1 },
                    }
                });
                session.SaveChanges();
            }

            etlDone.Wait(TimeSpan.FromMinutes(1));
            AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

            using IConsumer<string, byte[]> consumer = CreateKafkaConsumer(DefaultTopics.Select(x => x.Name));

            var consumeResult = consumer.Consume();
            var bytesAsString = Encoding.UTF8.GetString(consumeResult.Message.Value);
            var order = JsonConvert.DeserializeObject<OrderData>(bytesAsString);

            Assert.NotNull(order);
            Assert.Equal(order.Id, "orders/1-A");
            Assert.Equal(order.OrderLinesCount, 2);
            Assert.Equal(order.TotalCost, 10);
            
            consumer.Close();
            etlDone.Reset();
        }
    }

    private IConsumer<string, byte[]> CreateKafkaConsumer(IEnumerable<string> topics)
    {
        var consumerConfig = new ConsumerConfig()
        {
            BootstrapServers = DefaultKafkaUrl,
            GroupId = "test",
            IsolationLevel = IsolationLevel.ReadCommitted,
            EnablePartitionEof = true,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();
        consumer.Subscribe(topics);
        return consumer;
    }

    [Fact]
    public void SimpleScriptWithManyDocuments()
    {
        using var store = GetDocumentStore();

        var numberOfOrders = 10;
        var numberOfLinesPerOrder = 2;

        var config = SetupQueueEtl(store, DefaultScript, DefaultTopics, DefaultCollections, url: DefaultKafkaUrl);
        var etlDone = WaitForEtl(store, (n, statistics) => statistics.LastProcessedEtag >= numberOfOrders);

        for (int i = 0; i < numberOfOrders; i++)
        {
            using (var session = store.OpenSession())
            {
                Order order = new Order { OrderLines = new List<OrderLine>() };

                for (int j = 0; j < numberOfLinesPerOrder; j++)
                {
                    order.OrderLines.Add(new OrderLine { Cost = j + 1, Product = "foos/" + j, Quantity = (i * j) % 10 });
                }

                session.Store(order, "orders/" + i);

                session.SaveChanges();
            }
        }

        AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

        using IConsumer<string, byte[]> consumer = CreateKafkaConsumer(DefaultTopics.Select(x => x.Name));

        var ordersList = new List<OrderData>();
        while (ordersList.Count < numberOfOrders)
        {
            try
            {
                var consumeResult = consumer.Consume();
                var bytesAsString = Encoding.UTF8.GetString(consumeResult.Message.Value);
                var order = JsonConvert.DeserializeObject<OrderData>(bytesAsString);
                ordersList.Add(order);
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Consume error: {e.Error.Reason}");
            }
        }

        Assert.Equal(ordersList.Count, 10);

        for (int i = 0; i <= ordersList.Count; i++)
        {
            var order = ordersList.FirstOrDefault(x => x.Id == $"orders/{i}");
            Assert.NotNull(order);
            Assert.Equal(order.OrderLinesCount, 2);
            Assert.Equal(order.TotalCost, i * 2);
        }
    }

    [Fact]
    public void Docs_from_two_collections_loaded_to_single_one()
    {
        using var store = GetDocumentStore();

        var config = SetupQueueEtl(store,
            @"var userData = { UserId: id(this), Name: this.Name }; loadToUsers" + TopicSuffix + @"(userData)",
            new List<EtlQueue> { new() { Name = $"Users{TopicSuffix}" } }, new[] { "Users", "People" }, url: DefaultKafkaUrl);
        var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

        using (var session = store.OpenSession())
        {
            session.Store(new User { Name = "Joe Doe" }, "users/1");
            session.Store(new Person { Name = "James Smith" }, "people/1");
            session.SaveChanges();
        }

        AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

        using IConsumer<string, byte[]> consumer = CreateKafkaConsumer(new List<string> { $"Users{TopicSuffix}" });

        var usersList = new List<UserData>();
        while (usersList.Count < 2)
        {
            try
            {
                var consumeResult = consumer.Consume();
                var bytesAsString = Encoding.UTF8.GetString(consumeResult.Message.Value);
                var user = JsonConvert.DeserializeObject<UserData>(bytesAsString);
                usersList.Add(user);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Consume error: {e.Message}");
            }
        }

        Assert.Equal(usersList.Count, 2);
        Assert.NotNull(usersList.FirstOrDefault(x => x.UserId == "users/1"));
        Assert.NotNull(usersList.FirstOrDefault(x => x.UserId == "people/1"));
    }

    [Fact]
    public void Error_if_script_does_not_contain_any_loadTo_method()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Order
                {
                    OrderLines = new List<OrderLine>
                    {
                        new OrderLine { Cost = 3, Product = "Cheese", Quantity = 3 },
                        new OrderLine { Cost = 4, Product = "Bear", Quantity = 2 },
                    }
                });
                session.SaveChanges();
            }

            var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

            var config = new QueueEtlConfiguration
            {
                Name = "test",
                ConnectionStringName = "test",
                Transforms = { new Transformation { Name = "test", Collections = { "Orders" }, Script = @"this.TotalCost = 10;" } }
            };

            config.Initialize(new QueueConnectionString
            {
                Name = "Foo",
                BrokerType = QueueBroker.Kafka,
                KafkaConnectionSettings = new KafkaConnectionSettings() { ConnectionOptions = new Dictionary<string, string> { }, Url = DefaultKafkaUrl }
            });

            List<string> errors;
            config.Validate(out errors);

            Assert.Equal(1, errors.Count);

            Assert.Equal("No `loadTo<QueueName>()` method call found in 'test' script", errors[0]);
        }
    }

    [Fact]
    public async Task CanTestScript()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order
                {
                    OrderLines = new List<OrderLine>
                    {
                        new OrderLine { Cost = 3, Product = "Milk", Quantity = 3 },
                        new OrderLine { Cost = 4, Product = "Bear", Quantity = 2 },
                    }
                });
                await session.SaveChangesAsync();
            }

            var result1 = store.Maintenance.Send(new PutConnectionStringOperation<QueueConnectionString>(new QueueConnectionString
            {
                Name = "simulate", BrokerType = QueueBroker.Kafka, KafkaConnectionSettings = new KafkaConnectionSettings() { Url = DefaultKafkaUrl }
            }));
            Assert.NotNull(result1.RaftCommandIndex);

            var database = GetDatabase(store.Database).Result;

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (QueueEtl.TestScript(
                           new TestQueueEtlScript
                           {
                               DocumentId = "orders/1-A",
                               Configuration = new QueueEtlConfiguration
                               {
                                   Name = "simulate",
                                   ConnectionStringName = "simulate",
                                   EtlQueues = { new EtlQueue() { Name = "Orders" } },
                                   Transforms =
                                   {
                                       new Transformation
                                       {
                                           Collections = { "Orders" },
                                           Name = "Orders",
                                           Script = @"
var orderData = {
    Id: id(this),
    OrderLinesCount: this.OrderLines.length,
    TotalCost: 0
};

for (var i = 0; i < this.OrderLines.length; i++) {
    var line = this.OrderLines[i];
    var cost = (line.Quantity * line.PricePerUnit) *  ( 1 - line.Discount);
    orderData.TotalCost += line.Cost * line.Quantity;    
}

loadToOrders(orderData);

output('test output')"
                                       }
                                   }
                               }
                           }, database, database.ServerStore, context, out var testResult))
                {
                    var result = (QueueEtlTestScriptResult)testResult;

                    Assert.Equal(0, result.TransformationErrors.Count);

                    Assert.Equal(1, result.Summary.Count);

                    Assert.Equal("test output", result.DebugOutput[0]);
                }
            }
        }
    }

    private QueueEtlConfiguration SetupQueueEtl(DocumentStore store, string script, IEnumerable<EtlQueue> queues,
        IEnumerable<string> collections, bool applyToAllDocuments = false, string configurationName = null,
        string transformationName = null,
        Dictionary<string, string> configuration = null, string url = null)
    {
        var connectionStringName = $"{store.Database}@{store.Urls.First()} to Queue";

        var config = new QueueEtlConfiguration
        {
            Name = configurationName ?? connectionStringName,
            ConnectionStringName = connectionStringName,
            EtlQueues = queues.ToList(),
            Transforms =
            {
                new Transformation
                {
                    Name = transformationName ?? $"ETL : {connectionStringName}",
                    Collections = new List<string>(collections),
                    Script = script,
                    ApplyToAllDocuments = applyToAllDocuments
                }
            }
        };

        AddEtl(store, config,
            new QueueConnectionString
            {
                Name = connectionStringName,
                BrokerType = QueueBroker.Kafka,
                KafkaConnectionSettings = new KafkaConnectionSettings() { ConnectionOptions = configuration, Url = url }
            });
        return config;
    }

    private void AssertEtlDone(ManualResetEventSlim etlDone, TimeSpan timeout, string databaseName, QueueEtlConfiguration config)
    {
        if (etlDone.Wait(timeout) == false)
        {
            TryGetLoadError(databaseName, config, out var loadError);
            TryGetTransformationError(databaseName, config, out var transformationError);

            Assert.True(false, $"ETL wasn't done. Load error: {loadError?.Error}. Transformation error: {transformationError?.Error}");
        }
    }

    private void CleanupTopic(IEnumerable<string> topics)
    {
        var config = new AdminClientConfig() { BootstrapServers = DefaultKafkaUrl };
        var adminClient = new AdminClientBuilder(config).Build();

        try
        {
            adminClient.DeleteTopicsAsync(topics).Wait();
        }
        catch (Exception e)
        {
            
        }
    }

    public override void Dispose()
    {
        base.Dispose();
        CleanupTopic(DefaultTopics.Select(x => x.Name));
    }

    private class Order
    {
        public string Id { get; set; }
        public List<OrderLine> OrderLines { get; set; }
    }

    private class OrderData
    {
        public string Id { get; set; }
        public int OrderLinesCount { get; set; }
        public int TotalCost { get; set; }
    }

    private class OrderLine
    {
        public string Product { get; set; }
        public int Quantity { get; set; }
        public int Cost { get; set; }
    }

    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Person
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class UserData
    {
        public string UserId { get; set; }
        public string Name { get; set; }
    }
}
