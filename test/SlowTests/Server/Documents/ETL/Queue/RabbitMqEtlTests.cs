using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.ETL.Providers.Queue;
using Raven.Server.Documents.ETL.Providers.Queue.Test;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue;

public class RabbitMqEtlTests : RabbitMqEtlTestBase
{
    public RabbitMqEtlTests(ITestOutputHelper output) : base(output)
    {
    }

    [RequiresRabbitMqFact]
    public void SimpleScript()
    {
        using (var store = GetDocumentStore())
        {
            var config = SetupQueueEtlToRabbitMq(store, DefaultScript, DefaultCollections);
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

            AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

            var channel = CreateRabbitMqConsumer();
            var consumer = new EventingBasicConsumer(channel);

            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var bytesAsString = Encoding.UTF8.GetString(body);
                
                var order = JsonConvert.DeserializeObject<OrderData>(bytesAsString);

                Assert.NotNull(order);
                Assert.Equal(order.Id, "orders/1-A");
                Assert.Equal(order.OrderLinesCount, 2);
                Assert.Equal(order.TotalCost, 10);
            };

            channel.BasicConsume(queue: DefaultExchanges.First().Name,
                autoAck: true,
                consumer: consumer);

            etlDone.Reset();
        }
    }

    [RequiresRabbitMqFact]
    public void SimpleScriptWithManyDocuments()
    {
        using var store = GetDocumentStore();

        var numberOfOrders = 10;
        var numberOfLinesPerOrder = 2;

        var config = SetupQueueEtlToRabbitMq(store, DefaultScript, DefaultCollections);
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
        
        var channel = CreateRabbitMqConsumer();
        var consumer = new EventingBasicConsumer(channel);
        var counter = 0;

        consumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var bytesAsString = Encoding.UTF8.GetString(body);
                
            var order = JsonConvert.DeserializeObject<OrderData>(bytesAsString);

            Assert.NotNull(order);
            Assert.Equal(order.Id, $"orders/{counter}");
            Assert.Equal(order.OrderLinesCount, 2);
            Assert.Equal(order.TotalCost, counter * 2);
            
            counter++;
        };

        channel.BasicConsume(queue: DefaultExchanges.First().Name, autoAck: true, consumer: consumer);
    }

    [RequiresRabbitMqFact]
    public void Docs_from_two_collections_loaded_to_single_one()
    {
        using var store = GetDocumentStore();

        var config = SetupQueueEtlToRabbitMq(store,
            @"var userData = { UserId: id(this), Name: this.Name }; loadToUsers" + ExchangeSuffix + @"(userData)",
            new[] { "Users", "People" });
        var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

        using (var session = store.OpenSession())
        {
            session.Store(new User { Name = "Joe Doe" }, "users/1");
            session.Store(new Person { Name = "James Smith" }, "people/1");
            session.SaveChanges();
        }

        AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);
        
        var channel = CreateRabbitMqConsumer();
        var consumer = new   EventingBasicConsumer(channel);

        consumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var bytesAsString = Encoding.UTF8.GetString(body);
                
            var user = JsonConvert.DeserializeObject<UserData>(bytesAsString);
            
            Assert.NotNull(user);
            Assert.Contains(user.UserId, new List<string>() { "users/1", "people/1" });
            Assert.Contains(user.Name, new List<string>() { "John Doe", "James Smith" });
        };

        channel.BasicConsume(queue: $"Users{ExchangeSuffix}", autoAck: true, consumer: consumer);
    }

    [Fact]
    public void Error_if_script_does_not_contain_any_loadTo_method()
    {
        var config = new QueueEtlConfiguration
        {
            Name = "test",
            ConnectionStringName = "test",
            BrokerType = QueueBroker.RabbitMq,
            Transforms = { new Transformation { Name = "test", Collections = { "Orders" }, Script = @"this.TotalCost = 10;" } }
        };

        config.Initialize(new QueueConnectionString
        {
            Name = "Foo",
            BrokerType = QueueBroker.RabbitMq,
            RabbitMqConnectionSettings = 
                new RabbitMqConnectionSettings() { ConnectionString = "amqp://guest:guest@localhost:5672/" }
        });

        List<string> errors;
        config.Validate(out errors);

        Assert.Equal(1, errors.Count);

        Assert.Equal("No `loadTo<QueueName>()` method call found in 'test' script", errors[0]);
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
                Name = "simulate",
                BrokerType = QueueBroker.Kafka,
                KafkaConnectionSettings = new KafkaConnectionSettings() { Url = "amqp://guest:guest@localhost:5672/" }
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
                                   Queues = { new EtlQueue() { Name = "Orders" } },
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

loadToOrders(orderData, {
                                                            Id: id(this),
                                                            PartitionKey: id(this),
                                                            Type: 'com.github.users',
                                                            Source: '/registrations/direct-signup',
                                                            Exchange: 'users-topic',
                                                            ExchangeType: 'topic',
                                                     });

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

    [RequiresRabbitMqFact]
    public void CanPassOptionsToLoadToMethod()
    {
        using (var store = GetDocumentStore())
        {
            var config = SetupQueueEtlToRabbitMq(store,
                @$"loadToUsers{ExchangeSuffix}(this, {{
                                                            Id: id(this),
                                                            PartitionKey: id(this),
                                                            Type: 'com.github.users',
                                                            Source: '/registrations/direct-signup',
                                                            Exchange: 'users-topic',
                                                            ExchangeType: 'topic',
                                                     }})", new[] { "Users" });

            var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Arek" });
                session.SaveChanges();
            }

            AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);
            
            var channel = CreateRabbitMqConsumer();
            var consumer = new EventingBasicConsumer(channel);
            
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var bytesAsString = Encoding.UTF8.GetString(body);
                
                var user = JsonConvert.DeserializeObject<UserData>(bytesAsString);
            
                Assert.NotNull(user);
                Assert.Equal(user.Name, "Arek");
            };

            channel.BasicConsume(queue: $"Users{ExchangeSuffix}", autoAck: true, consumer: consumer);
        }
    }

    [RequiresRabbitMqFact]
    public void ShouldDeleteDocumentsAfterProcessing()
    {
        using (var store = GetDocumentStore())
        {
            var config = SetupQueueEtlToRabbitMq(store,
                @$"loadToUsers{ExchangeSuffix}(this)", new[] { "Users" },
                new[] { new EtlQueue { Name = $"Users{ExchangeSuffix}", DeleteProcessedDocuments = true } });

            var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

            using (var session = store.OpenSession())
            {
                session.Store(new User { Id = "users/1", Name = "Arek" });
                session.SaveChanges();
            }

            AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);
            
            var channel = CreateRabbitMqConsumer();
            var consumer = new   EventingBasicConsumer(channel);

            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var bytesAsString = Encoding.UTF8.GetString(body);
                
                var user = JsonConvert.DeserializeObject<UserData>(bytesAsString);
            
                Assert.NotNull(user);
                Assert.Equal(user.Name, "Arek");
            };

            channel.BasicConsume(queue: $"Users{ExchangeSuffix}", autoAck: true, consumer: consumer);

            using (var session = store.OpenSession())
            {
                var entity = session.Load<User>("users/1");
                Assert.Null(entity);
            }
        }
    }

    [Fact]
    public async Task ShouldImportTask()
    {
        using (var srcStore = GetDocumentStore())
        using (var dstStore = GetDocumentStore())
        {
            var config = SetupQueueEtlToRabbitMq(srcStore,
                DefaultScript, DefaultCollections, connectionString: "amqp://abc:guest@localhost:1234/");

            var exportFile = GetTempFileName();

            var exportOperation = await srcStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
            await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var operation = await dstStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var destinationRecord = await dstStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dstStore.Database));
            Assert.Equal(1, destinationRecord.QueueConnectionStrings.Count);
            Assert.Equal(1, destinationRecord.QueueEtls.Count);
        }
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
