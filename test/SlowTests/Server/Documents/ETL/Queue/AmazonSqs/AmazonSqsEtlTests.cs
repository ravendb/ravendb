using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.Runtime.Internal.Util;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.ETL.Providers.Queue;
using Raven.Server.Documents.ETL.Providers.Queue.Test;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using QueueItem = Raven.Server.Documents.ETL.Providers.Queue.QueueItem;

namespace SlowTests.Server.Documents.ETL.Queue.AmazonSqs;

public class AmazonSqsEtlTests : AmazonSqsEtlTestBase
{
    public AmazonSqsEtlTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl, AmazonSqsRequired = true)]
    public async Task SimpleScript()
    {
        using (var store = GetDocumentStore())
        {
            var config = SetupQueueEtlToAmazonSqsOnline(store, DefaultScript, DefaultCollections);
            var etlDone = Etl.WaitForEtlToComplete(store);

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

            await AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

            IAmazonSQS queueClient = CreateQueueClient();
            var queueUrl = AsyncHelpers.RunSync(() => queueClient.GetQueueUrlAsync(OrdersQueueName)).QueueUrl;
            var messagesReadResult = AsyncHelpers.RunSync(() => queueClient.ReceiveMessageAsync(queueUrl));
            var order = JsonConvert.DeserializeObject<CloudEventOrderData>(messagesReadResult.Messages.First().Body).Data;

            Assert.NotNull(order);
            Assert.Equal(order.Id, "orders/1-A");
            Assert.Equal(order.OrderLinesCount, 2);
            Assert.Equal(order.TotalCost, 10);
        }
    }

    [RavenFact(RavenTestCategory.Etl, AmazonSqsRequired = true)]
    public async Task SimpleScriptToFifoQueue()
    {
        using (var store = GetDocumentStore())
        {
            IAmazonSQS queueClient = CreateQueueClient();
            await queueClient.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = "users.fifo",
                Attributes = new Dictionary<string, string>
                {
                    // required attribute for FIFO queue
                    { "FifoQueue", "true" },
                }
            });


            var config = SetupQueueEtlToAmazonSqsOnline(store,
                @$"loadTo('users.fifo', this)", new[] { "users" },
                new[] { new EtlQueue { Name = "users.fifo" } }, skipAutomaticQueueDeclaration: true);

            var etlDone = Etl.WaitForEtlToComplete(store);

            using (var session = store.OpenSession())
            {
                session.Store(new User()
                {
                    Id = "users/1-A",
                    Name = "John Doe"
                });
                session.Store(new User()
                {
                    Id = "users/2-A",
                    Name = "Test"
                });
                session.SaveChanges();
            }

            await AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);


            var queueUrl = AsyncHelpers.RunSync(() => queueClient.GetQueueUrlAsync("users.fifo")).QueueUrl;
            var messagesReadResult = AsyncHelpers.RunSync(() => queueClient.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queueUrl,
                MaxNumberOfMessages = 2
            }));
            var userData1 = JsonConvert.DeserializeObject<CloudEventUserData>(messagesReadResult.Messages[0].Body).Data;
            var userData2 = JsonConvert.DeserializeObject<CloudEventUserData>(messagesReadResult.Messages[1].Body).Data;

            Assert.NotNull(userData1);
            Assert.Equal("John Doe", userData1.Name);

            Assert.NotNull(userData2);
            Assert.Equal("Test", userData2.Name);
        }
    }

    [RavenFact(RavenTestCategory.Etl, AmazonSqsRequired = true)]
    public async Task Simple_script_large_message_error_expected()
    {
        using (var store = GetDocumentStore())
        {
            var config = SetupQueueEtlToAmazonSqsOnline(store,
                @$"loadToUsers(this)", new[] { "users" },
                new[] { new EtlQueue { Name = $"users" } });

            using (var session = store.OpenSession())
            {
                session.Store(new User()
                {
                    Id = "users/1-A",
                    Name = GenerateLargeString()
                });
                session.Store(new User()
                {
                    Id = "users/2-A",
                    Name = "Test"
                });
                session.SaveChanges();
            }

            var alert = await AssertWaitForNotNullAsync(() => Etl.TryGetLoadErrorAsync(store.Database, config), timeout: (int)TimeSpan.FromMinutes(1).TotalMilliseconds);

            Assert.Contains(
                "MessageTooLong",
                alert.Error);
        }
    }

    [RavenFact(RavenTestCategory.Etl, AmazonSqsRequired = true)]
    public async Task Simple_script_queue_not_exist_error_expected()
    {
        using (var store = GetDocumentStore())
        {
            var config = SetupQueueEtlToAmazonSqsOnline(store,
                @$"loadToUsers(this)", new[] { "users" },
                new[] { new EtlQueue { Name = $"users" } }, skipAutomaticQueueDeclaration: true);

            using (var session = store.OpenSession())
            {
                session.Store(new User()
                {
                    Id = "users/1-A",
                    Name = GenerateLargeString()
                });
                session.Store(new User()
                {
                    Id = "users/2-A",
                    Name = "Test"
                });
                session.SaveChanges();
            }

            var alert = await AssertWaitForNotNullAsync(() => Etl.TryGetLoadErrorAsync(store.Database, config), timeout: (int)TimeSpan.FromMinutes(1).TotalMilliseconds);

            Assert.Contains(
                "QueueDoesNotExist",
                alert.Error);
        }
    }

    [RavenFact(RavenTestCategory.Etl, AmazonSqsRequired = true)]
    public void Error_if_script_does_not_contain_any_loadTo_method()
    {
        var config = new QueueEtlConfiguration
        {
            Name = "test",
            ConnectionStringName = "test",
            BrokerType = QueueBrokerType.AmazonSqs,
            Transforms =
            {
                new Transformation
                {
                    Name = "test", Collections = { "Orders" }, Script = @"this.TotalCost = 10;"
                }
            }
        };

        config.Initialize(new QueueConnectionString
        {
            Name = "Foo",
            BrokerType = QueueBrokerType.AmazonSqs,
            AmazonSqsConnectionSettings = new AmazonSqsConnectionSettings
            {
                UseEmulator = true
            }
        });

        List<string> errors;
        config.Validate(out errors);

        Assert.Equal(1, errors.Count);

        Assert.Equal("No `loadTo<QueueName>()` method call found in 'test' script", errors[0]);
    }


    [RavenFact(RavenTestCategory.Etl, AmazonSqsRequired = true)]
    public void ShardedAmazonSqsEtlNotSupported()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            var error = Assert.ThrowsAny<NotSupportedInShardingException>(() =>
            {
                SetupQueueEtlToAmazonSqsOnline(store, DefaultScript, DefaultCollections);
            });
            Assert.Contains("Queue ETLs are currently not supported in sharding", error.Message);
        }
    }


    [RavenFact(RavenTestCategory.Etl, AmazonSqsRequired = true)]
    public async Task TestAreHeadersPresent()
    {
        using (var store = GetDocumentStore())
        {
            var config = SetupQueueEtlToAmazonSqsOnline(store, DefaultScript, DefaultCollections);
            var etlDone = Etl.WaitForEtlToComplete(store);

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

            await AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

            IAmazonSQS queueClient = CreateQueueClient();
            var queueUrl = AsyncHelpers.RunSync(() => queueClient.GetQueueUrlAsync(OrdersQueueName)).QueueUrl;
            var messagesReadResult = AsyncHelpers.RunSync(() => queueClient.ReceiveMessageAsync(queueUrl));
            var orderCloudEvent =
                JsonConvert.DeserializeObject<CloudEventOrderData>(messagesReadResult.Messages.First().Body);

            Assert.True(string.IsNullOrWhiteSpace(orderCloudEvent.Id) == false);
            Assert.True(string.IsNullOrWhiteSpace(orderCloudEvent.Specversion) == false);
            Assert.True(string.IsNullOrWhiteSpace(orderCloudEvent.Type) == false);
            Assert.True(string.IsNullOrWhiteSpace(orderCloudEvent.Source) == false);
        }
    }


    [RavenFact(RavenTestCategory.Etl, AmazonSqsRequired = true)]
    public async Task SimpleScriptWithManyDocuments()
    {
        using var store = GetDocumentStore();

        var numberOfOrders = 10;
        var numberOfLinesPerOrder = 2;

        var config = SetupQueueEtlToAmazonSqsOnline(store, DefaultScript, DefaultCollections);
        var etlDone =
            Etl.WaitForEtlToComplete(store, (n, statistics) => statistics.LastProcessedEtag >= numberOfOrders);

        for (int i = 0; i < numberOfOrders; i++)
        {
            using (var session = store.OpenSession())
            {
                Order order = new Order { OrderLines = new List<OrderLine>() };

                for (int j = 0; j < numberOfLinesPerOrder; j++)
                {
                    order.OrderLines.Add(new OrderLine
                    {
                        Cost = j + 1,
                        Product = "foos/" + j,
                        Quantity = (i * j) % 10
                    });
                }

                session.Store(order, "orders/" + i);

                session.SaveChanges();
            }
        }

        await AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

        IAmazonSQS queueClient = CreateQueueClient();
        var queueUrl = AsyncHelpers.RunSync(() => queueClient.GetQueueUrlAsync(OrdersQueueName)).QueueUrl;
        var messagesReadResult = AsyncHelpers.RunSync(() => queueClient.ReceiveMessageAsync(new ReceiveMessageRequest()
        {
            QueueUrl = queueUrl,
            MaxNumberOfMessages = numberOfOrders
        }));
        List<CloudEventOrderData> orders = messagesReadResult.Messages
            .Select(message => JsonConvert.DeserializeObject<CloudEventOrderData>(message.Body)).ToList();

        Assert.Equal(numberOfOrders, orders.Count);

        for (int counter = 0; counter < numberOfOrders; counter++)
        {
            var order = orders.Single(x => x.Data.Id == $"orders/{counter}").Data;
            Assert.Equal(order.OrderLinesCount, numberOfLinesPerOrder);
            Assert.Equal(order.TotalCost, counter * 2);
        }
    }


    [RavenFact(RavenTestCategory.Etl, AmazonSqsRequired = true)]
    public void Error_if_script_is_empty()
    {
        var config = new QueueEtlConfiguration
        {
            Name = "test",
            ConnectionStringName = "test",
            BrokerType = QueueBrokerType.AmazonSqs,
            Transforms = { new Transformation { Name = "test", Collections = { "Orders" }, Script = @"" } }
        };

        config.Initialize(new QueueConnectionString
        {
            Name = "Foo",
            BrokerType = QueueBrokerType.AmazonSqs,
            AmazonSqsConnectionSettings = new AmazonSqsConnectionSettings
            {
                UseEmulator = true
            }
        });

        List<string> errors;
        config.Validate(out errors);

        Assert.Equal(1, errors.Count);

        Assert.Equal("Script 'test' must not be empty", errors[0]);
    }


    [RavenFact(RavenTestCategory.Etl, AmazonSqsRequired = true)]
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

            var result1 = store.Maintenance.Send(new PutConnectionStringOperation<QueueConnectionString>(
                new QueueConnectionString
                {
                    Name = "simulate",
                    BrokerType = QueueBrokerType.AmazonSqs,
                    AmazonSqsConnectionSettings = new AmazonSqsConnectionSettings
                    {
                        UseEmulator = true
                    }
                }));
            Assert.NotNull(result1.RaftCommandIndex);

            var database = await GetDatabase(store.Database);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(
                       out DocumentsOperationContext context))
            {
                var testResult = QueueEtl<QueueItem>.TestScript(
                    new TestQueueEtlScript
                    {
                        DocumentId = "orders/1-A",
                        Configuration = new QueueEtlConfiguration
                        {
                            Name = "simulate",
                            ConnectionStringName = "simulate",
                            Queues = { new EtlQueue() { Name = "Orders" } },
                            BrokerType = QueueBrokerType.AmazonSqs,
                            Transforms =
                            {
                                new Transformation
                                {
                                    Collections = { "Orders" }, Name = "Orders", Script = DefaultScript
                                }
                            }
                        }
                    }, database, database.ServerStore, context);
                {
                    var result = (QueueEtlTestScriptResult)testResult;

                    Assert.Equal(0, result.TransformationErrors.Count);

                    Assert.Equal(1, result.Summary.Count);

                    Assert.Equal("Orders", result.Summary[0].QueueName);
                    Assert.Equal("orders/1-A", result.Summary[0].Messages[0].Attributes.Id);
                    Assert.Equal("com.github.users", result.Summary[0].Messages[0].Attributes.Type);
                    Assert.Equal("/registrations/direct-signup",
                        result.Summary[0].Messages[0].Attributes.Source.ToString());

                    Assert.Equal("test output", result.DebugOutput[0]);
                }
            }
        }
    }


    [RavenFact(RavenTestCategory.Etl, AmazonSqsRequired = true)]
    public async Task ShouldDeleteDocumentsAfterProcessing()
    {
        using (var store = GetDocumentStore())
        {
            var config = SetupQueueEtlToAmazonSqsOnline(store,
                @$"loadToUsers(this)", new[] { "Users" },
                new[] { new EtlQueue { Name = $"Users", DeleteProcessedDocuments = true } });

            var etlDone = Etl.WaitForEtlToComplete(store);

            using (var session = store.OpenSession())
            {
                session.Store(new User { Id = "users/1", Name = "Arek" });
                session.SaveChanges();
            }

            await AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

            IAmazonSQS queueClient = CreateQueueClient();
            var queueUrl = AsyncHelpers.RunSync(() => queueClient.GetQueueUrlAsync("Users")).QueueUrl;
            var messagesReadResult = AsyncHelpers.RunSync(() => queueClient.ReceiveMessageAsync(queueUrl));
            var user = JsonConvert.DeserializeObject<CloudEventUserData>(messagesReadResult.Messages.First().Body).Data;

            Assert.NotNull(user);
            Assert.Equal(user.Name, "Arek");

            using (var session = store.OpenSession())
            {
                var entity = session.Load<User>("users/1");
                Assert.Null(entity);
            }
        }
    }


    [RavenFact(RavenTestCategory.Etl, AmazonSqsRequired = true)]
    public async Task ShouldImportTask()
    {
        using (var srcStore = GetDocumentStore())
        using (var dstStore = GetDocumentStore())
        {
            var config = SetupQueueEtlToAmazonSqsOnline(srcStore,
                DefaultScript, DefaultCollections,
                new List<EtlQueue>() { new() { Name = "Orders", DeleteProcessedDocuments = true } });

            var exportFile = GetTempFileName();

            var exportOperation = await srcStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
            await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var operation = await dstStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var destinationRecord =
                await dstStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dstStore.Database));
            Assert.Equal(1, destinationRecord.QueueConnectionStrings.Count);
            Assert.Equal(1, destinationRecord.QueueEtls.Count);

            Assert.Equal(QueueBrokerType.AmazonSqs, destinationRecord.QueueEtls[0].BrokerType);
            Assert.Equal(DefaultScript, destinationRecord.QueueEtls[0].Transforms[0].Script);
            Assert.Equal(DefaultCollections, destinationRecord.QueueEtls[0].Transforms[0].Collections);

            Assert.Equal(1, destinationRecord.QueueEtls[0].Queues.Count);
            Assert.Equal("Orders", destinationRecord.QueueEtls[0].Queues[0].Name);
            Assert.True(destinationRecord.QueueEtls[0].Queues[0].DeleteProcessedDocuments);
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding | RavenTestCategory.Etl, AmazonSqsRequired = true)]
    public async Task ShouldSkipUnsupportedFeaturesInShardingOnImport_AmazonSqsEtl()
    {
        using (var srcStore = GetDocumentStore())
        using (var dstStore = Sharding.GetDocumentStore())
        {
            var config = SetupQueueEtlToAmazonSqsOnline(srcStore,
                DefaultScript, DefaultCollections,
                new List<EtlQueue>() { new() { Name = "Orders", DeleteProcessedDocuments = true } });

            var record = await srcStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(srcStore.Database));

            Assert.NotNull(record.QueueEtls);
            Assert.Equal(1, record.QueueEtls.Count);

            var exportFile = GetTempFileName();

            var exportOperation = await srcStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
            await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var operation = await dstStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            record = await dstStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dstStore.Database));

            Assert.Empty(record.QueueEtls);
        }
    }


    [RavenFact(RavenTestCategory.Etl, AmazonSqsRequired = true)]
    public void ProperUrlFromHttpConnectionString()
    {
        var config = new QueueEtlConfiguration
        {
            Name = "test",
            ConnectionStringName = "test",
            BrokerType = QueueBrokerType.AmazonSqs,
            Transforms = { new Transformation { Name = "test", Collections = { "Orders" }, Script = @"" } }
        };

        config.Initialize(new QueueConnectionString
        {
            Name = "Foo",
            BrokerType = QueueBrokerType.AmazonSqs,
            AmazonSqsConnectionSettings =
                new AmazonSqsConnectionSettings
                {
                    UseEmulator = true,
                }
        });

        var queueUrl = config.Connection.AmazonSqsConnectionSettings.GetQueueUrl();
        Assert.Equal(queueUrl, Environment.GetEnvironmentVariable(AmazonSqsConnectionSettings.EmulatorUrlEnvironmentVariable));
    }


    [RavenFact(RavenTestCategory.Etl, AmazonSqsRequired = true)]
    public void ProperUrlFromHttpsConnectionString()
    {
        var config = new QueueEtlConfiguration
        {
            Name = "test",
            ConnectionStringName = "test",
            BrokerType = QueueBrokerType.AmazonSqs,
            Transforms = { new Transformation { Name = "test", Collections = { "Orders" }, Script = @"" } }
        };

        config.Initialize(new QueueConnectionString
        {
            Name = "Foo",
            BrokerType = QueueBrokerType.AmazonSqs,
            AmazonSqsConnectionSettings =
                new AmazonSqsConnectionSettings
                {
                    Passwordless = true
                }
        });

        var queueUrl = config.Connection.AmazonSqsConnectionSettings.GetQueueUrl();
        Assert.Equal(queueUrl, "https://queue.amazonaws.com/");
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
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class CloudEventOrderData
    {
        public string Id { get; set; }
        public string Specversion { get; set; }
        public string Type { get; set; }
        public string Source { get; set; }
        public OrderData Data { get; set; }
    }

    private class CloudEventUserData
    {
        public UserData Data { get; set; }
    }
}
