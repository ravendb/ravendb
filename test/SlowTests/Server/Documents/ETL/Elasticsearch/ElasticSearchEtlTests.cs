using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Documents.ETL.Providers.ElasticSearch;
using Raven.Server.Documents.ETL.Providers.ElasticSearch.Test;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.ConnectionString;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.ElasticSearch
{
    public class ElasticSearchEtlTests : ElasticSearchEtlTestBase
    {

        public ElasticSearchEtlTests(ITestOutputHelper output) : base(output)
        {
        }

        protected string UsersIndexName => $"Users{IndexSuffix}".ToLower();

        protected List<ElasticSearchIndex> UsersIndex => new()
        {
            new ElasticSearchIndex {IndexName = UsersIndexName, DocumentIdProperty = "UserId"}
        };

        [RequiresElasticSearchFact]
        public void SimpleScript()
        {
            using (var store = GetDocumentStore())
            using (GetElasticClient(out var client))
            {
                var config = SetupElasticEtl(store, DefaultScript, DefaultIndexes, DefaultCollections);
                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine {Cost = 3, Product = "Cheese", Quantity = 3}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    session.SaveChanges();
                }

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                var ordersCount = client.Count<object>(c => c.Index(OrdersIndexName));
                var orderLinesCount = client.Count<object>(c => c.Index(OrderLinesIndexName));

                Assert.True(ordersCount.IsValid);
                Assert.True(orderLinesCount.IsValid);

                Assert.Equal(1, ordersCount.Count);
                Assert.Equal(2, orderLinesCount.Count);

                etlDone.Reset();

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/1-A");

                    session.SaveChanges();
                }

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                var ordersCountAfterDelete = client.Count<object>(c => c.Index(OrdersIndexName));
                var orderLinesCountAfterDelete = client.Count<object>(c => c.Index(OrderLinesIndexName));

                Assert.True(ordersCount.IsValid);
                Assert.True(orderLinesCount.IsValid);

                Assert.Equal(0, ordersCountAfterDelete.Count);
                Assert.Equal(0, orderLinesCountAfterDelete.Count);
            }
        }

        [RequiresElasticSearchFact]
        public void SimpleScriptWithManyDocuments()
        {
            using (var store = GetDocumentStore())
            using (GetElasticClient(out var client))
            {
                var numberOfOrders = 100;
                var numberOfLinesPerOrder = 5;

                var config = SetupElasticEtl(store, DefaultScript, DefaultIndexes, DefaultCollections);
                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LastProcessedEtag >= numberOfOrders);

                for (int i = 0; i < numberOfOrders; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        Order order = new Order
                        {
                            OrderLines = new List<OrderLine>()
                        };

                        for (int j = 0; j < numberOfLinesPerOrder; j++)
                        {
                            order.OrderLines.Add(new OrderLine { Cost = j + 1, Product = "foos/" + j, Quantity = (i * j) % 10 });
                        }

                        session.Store(order, "orders/" + i);

                        session.SaveChanges();
                    }
                }

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                var ordersCount = client.Count<object>(c => c.Index(OrdersIndexName));
                var orderLinesCount = client.Count<object>(c => c.Index(OrderLinesIndexName));
                
                Assert.Equal(numberOfOrders, ordersCount.Count);
                Assert.Equal(numberOfOrders * numberOfLinesPerOrder, orderLinesCount.Count);

                etlDone = WaitForEtl(store, (n, statistics) => statistics.LastProcessedEtag >= 2 * numberOfOrders);

                for (int i = 0; i < numberOfOrders; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Delete("orders/" + i);

                        session.SaveChanges();
                    }
                }

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                var ordersCountAfterDelete = client.Count<object>(c => c.Index(OrdersIndexName));
                var orderLinesCountAfterDelete = client.Count<object>(c => c.Index(OrderLinesIndexName));

                Assert.Equal(0, ordersCountAfterDelete.Count);
                Assert.Equal(0, orderLinesCountAfterDelete.Count);
            }
        }

        [Fact]
        public async Task Simple_script_error_expected()
        {
            using (var store = GetDocumentStore())
            {
                var config = new ElasticSearchEtlConfiguration
                {
                    ConnectionStringName = "test",
                    Name = "myFirstEtl",
                    ElasticIndexes = DefaultIndexes,
                    Transforms =
                    {
                        new Transformation
                        {
                            Collections = DefaultCollections.ToList(),
                            Script = DefaultScript,
                            Name = "a"
                        }
                    },
                };

                AddEtl(store, config, new ElasticSearchConnectionString { Name = "test", Nodes = new[] { "http://localhost:1234" } }); //wrong elastic search url


                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine {Cost = 3, Product = "Cheese", Quantity = 3}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    session.SaveChanges();
                }

                var alert = await AssertWaitForNotNullAsync(() =>
                {
                    TryGetLoadError(store.Database, config, out var error);

                    return Task.FromResult(error);
                }, timeout: (int)TimeSpan.FromMinutes(1).TotalMilliseconds);
                
                Assert.StartsWith("Raven.Server.Exceptions.ETL.ElasticSearch.ElasticSearchLoadException", alert.Error);
            }
        }

        [RequiresElasticSearchFact]
        public void Can_get_document_id()
        {
            using (var store = GetDocumentStore())
            using (GetElasticClient(out var client))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine {Cost = 3, Product = "Cheese", Quantity = 3}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    session.SaveChanges();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var config = SetupElasticEtl(store, DefaultScript, DefaultIndexes, DefaultCollections);

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                client.Indices.Refresh(OrdersIndexName);
                client.Indices.Refresh(OrderLinesIndexName);

                var orderResponse = client.Search<object>(d => d
                    .Index(OrdersIndexName)
                    .Query(q => q
                        .Term(p => p
                            .Field("Id")
                            .Value("orders/1-a"))
                    )
                );

                var orderLineResponse = client.Search<object>(d => d
                    .Index(OrderLinesIndexName)
                    .Query(q => q
                        .Term(p => p
                            .Field("OrderId")
                            .Value("orders/1-a"))
                    )
                );

                Assert.Equal(1, orderResponse.Documents.Count);
                Assert.Equal("orders/1-a", JObject.FromObject(orderResponse.Documents.First()).ToObject<Dictionary<string, object>>()?["Id"]);

                Assert.Equal(2, orderLineResponse.Documents.Count);

                foreach (var document in orderLineResponse.Documents)
                {
                    Assert.Equal("orders/1-a", JObject.FromObject(document).ToObject<Dictionary<string, object>>()?["OrderId"]);
                }

                etlDone.Reset();

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/1-a");

                    session.SaveChanges();
                }

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                client.Indices.Refresh(OrdersIndexName);
                client.Indices.Refresh(OrderLinesIndexName);


                var ordersCountAfterDelete = client.Count<object>(c => c.Index(OrdersIndexName));
                var orderLinesCountAfterDelete = client.Count<object>(c => c.Index(OrderLinesIndexName));

                Assert.Equal(0, ordersCountAfterDelete.Count);
                Assert.Equal(0, orderLinesCountAfterDelete.Count);
            }
        }

        [RequiresElasticSearchFact]
        public void Can_Update_To_Be_No_Items_In_Child_TTable()
        {
            using (var store = GetDocumentStore())
            using (GetElasticClient(out var client))
            {
                var config = SetupElasticEtl(store, DefaultScript, DefaultIndexes, DefaultCollections);
                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine {Cost = 3, Product = "Cheese", Quantity = 3}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    session.SaveChanges();
                }

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                client.Indices.Refresh(OrdersIndexName);
                client.Indices.Refresh(OrderLinesIndexName);

                var ordersCount = client.Count<object>(c => c.Index(OrdersIndexName));
                var orderLinesCount = client.Count<object>(c => c.Index(OrderLinesIndexName));

                Assert.Equal(1, ordersCount.Count);
                Assert.Equal(2, orderLinesCount.Count);

                etlDone.Reset();

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1-A");
                    order.OrderLines.Clear();
                    session.SaveChanges();
                }

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                client.Indices.Refresh(OrdersIndexName);
                client.Indices.Refresh(OrderLinesIndexName);

                var ordersCountAfterDelete = client.Count<object>(c => c.Index(OrdersIndexName));
                var orderLinesCountAfterDelete = client.Count<object>(c => c.Index(OrderLinesIndexName));

                Assert.Equal(1, ordersCountAfterDelete.Count);
                Assert.Equal(0, orderLinesCountAfterDelete.Count);
            }
        }

        [RequiresElasticSearchFact]
        public void Update_of_disassembled_document()
        {
            using (var store = GetDocumentStore())
            using (GetElasticClient(out var client))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine {Cost = 10, Product = "a", Quantity = 1}, new OrderLine {Cost = 10, Product = "b", Quantity = 1},
                        }
                    });
                    session.SaveChanges();
                }

                var config = SetupElasticEtl(store, DefaultScript, DefaultIndexes, DefaultCollections);
                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                client.Indices.Refresh(OrdersIndexName);

                var orderResponse = client.Search<object>(d => d
                    .Index(OrdersIndexName)
                    .Query(q => q
                        .Term(p => p
                            .Field("Id")
                            .Value("orders/1-a"))
                    )
                );

                Assert.Equal(1, orderResponse.Documents.Count);

                var orderObject = JObject.FromObject(orderResponse.Documents.First()).ToObject<Dictionary<string, object>>();

                Assert.NotNull(orderObject);
                Assert.Equal("orders/1-a", orderObject["Id"]);
                Assert.Equal(2, (int)(long)orderObject["OrderLinesCount"]);
                Assert.Equal(20, (int)(long)orderObject["TotalCost"]);

                etlDone.Reset();

                using (var session = store.OpenSession())
                {
                    session.Store(
                        new Order
                        {
                            OrderLines = new List<OrderLine>
                            {
                                new OrderLine {Product = "a", Cost = 10, Quantity = 1}, new OrderLine {Product = "b", Cost = 10, Quantity = 2}
                            }
                        }, "orders/1-A");

                    session.SaveChanges();
                }

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(2), store.Database, config);

                client.Indices.Refresh(OrdersIndexName);

                var orderResponse1 = client.Search<object>(d => d
                    .Index(OrdersIndexName)
                    .Query(q => q
                        .Term(p => p
                            .Field("Id")
                            .Value("orders/1-a"))
                    )
                );

                Assert.Equal(1, orderResponse1.Documents.Count);

                var orderObject1 = JObject.FromObject(orderResponse1.Documents.First()).ToObject<Dictionary<string, object>>();

                Assert.NotNull(orderObject1);
                Assert.Equal("orders/1-a", orderObject1["Id"]);
                Assert.Equal(2, (int)(long)orderObject1["OrderLinesCount"]);
                Assert.Equal(30, (int)(long)orderObject1["TotalCost"]);
            }
        }

        [RequiresElasticSearchFact]
        public void Docs_from_two_collections_loaded_to_single_one()
        {
            using (var store = GetDocumentStore())
            using (GetElasticClient(out var client))
            {
                var config = SetupElasticEtl(store, @"var userData = { UserId: id(this), Name: this.Name }; loadToUsers" + IndexSuffix + @"(userData)", UsersIndex, new[] { "Users", "People" });
                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Joe Doe" }, "users/1");

                    session.Store(new Person { Name = "James Smith" }, "people/1");

                    session.SaveChanges();
                }

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                client.Indices.Refresh(UsersIndexName);

                var userResponse1 = client.Search<object>(d => d
                    .Index(UsersIndexName)
                    .Query(q => q
                        .Term(p => p
                            .Field("UserId")
                            .Value("users/1"))
                    )
                );

                Assert.Equal(1, userResponse1.Documents.Count);
                var userObject1 = JObject.FromObject(userResponse1.Documents.First()).ToObject<Dictionary<string, object>>();
                Assert.NotNull(userObject1);
                Assert.Equal("Joe Doe", userObject1["Name"]);

                client.Indices.Refresh(UsersIndexName);

                var userResponse2 = client.Search<object>(d => d
                    .Index(UsersIndexName)
                    .Query(q => q
                        .Term(p => p
                            .Field("UserId")
                            .Value("people/1"))
                    )
                );


                Assert.Equal(1, userResponse2.Documents.Count);
                var userObject2 = JObject.FromObject(userResponse2.Documents.First()).ToObject<Dictionary<string, object>>();
                Assert.NotNull(userObject2);
                Assert.Equal("James Smith", userObject2["Name"]);

                // update
                etlDone.Reset();

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Doe Joe" }, "users/1");

                    session.Store(new Person { Name = "Smith James" }, "people/1");

                    session.SaveChanges();
                }

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                client.Indices.Refresh(UsersIndexName);

                var userResponse3 = client.Search<object>(d => d
                    .Index(UsersIndexName)
                    .Query(q => q
                        .Term(p => p
                            .Field("UserId")
                            .Value("users/1"))
                    )
                );

                Assert.Equal(1, userResponse3.Documents.Count);
                var userObject3 = JObject.FromObject(userResponse3.Documents.First()).ToObject<Dictionary<string, object>>();
                Assert.NotNull(userObject3);
                Assert.Equal("Doe Joe", userObject3["Name"]);

                client.Indices.Refresh(UsersIndexName);

                var userResponse4 = client.Search<object>(d => d
                    .Index(UsersIndexName)
                    .Query(q => q
                        .Term(p => p
                            .Field("UserId")
                            .Value("people/1"))
                    )
                );


                Assert.Equal(1, userResponse4.Documents.Count);
                var userObject4 = JObject.FromObject(userResponse4.Documents.First()).ToObject<Dictionary<string, object>>();
                Assert.NotNull(userObject4);
                Assert.Equal("Smith James", userObject4["Name"]);
            }
        }

        [RequiresElasticSearchFact]
        public void Can_load_to_specific_collection_when_applying_to_all_docs()
        {
            using (var src = GetDocumentStore())
            using (GetElasticClient(out var client))
            {
                var etlDone = WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses != 0);

                var config = SetupElasticEtl(src, @"var userData = { UserId: id(this), FirstName: this.Name, LastName: this.LastName }; loadTo" + UsersIndexName + @"(userData)",
                    UsersIndex,
                    new List<string>(),
                    applyToAllDocuments: true);

                using (var session = src.OpenSession())
                {
                    session.Store(new User { Name = "James", LastName = "Smith" }, "users/1");

                    session.SaveChanges();
                }

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), src.Database, config);

                var userResponse = client.Search<object>(d => d
                    .Index(UsersIndexName)
                    .Query(q => q
                        .Term(p => p
                            .Field("UserId")
                            .Value("users/1"))
                    )
                );

                Assert.Equal("users/1", JObject.FromObject(userResponse.Documents.First()).ToObject<Dictionary<string, object>>()?["UserId"]);
            }
        }

        [RequiresElasticSearchFact]
        public void Should_delete_existing_document_when_filtered_by_script()
        {
            using (var src = GetDocumentStore())
            using (GetElasticClient(out var client))
            {
                var etlDone = WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses != 0);

                var config = SetupElasticEtl(src,
                    @"var userData = { UserId: id(this), FirstName: this.Name, LastName: this.LastName }; if (this.Name == 'Joe Doe') loadToUsers" + IndexSuffix + @"(userData)",
                    UsersIndex,
                    new List<string> { "Users" });

                using (var session = src.OpenSession())
                {
                    session.Store(new User { Name = "Joe Doe" }, "users/1");

                    session.SaveChanges();
                }

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), src.Database, config);

                var userResponse = client.Search<object>(d => d
                    .Index(UsersIndexName)
                    .Query(q => q
                        .Term(p => p
                            .Field("UserId")
                            .Value("users/1"))
                    )
                );

                Assert.Equal(1, userResponse.Documents.Count);

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    user.Name = "John Doe";
                    session.SaveChanges();
                }

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), src.Database, config);

                userResponse = client.Search<object>(d => d
                    .Index("users")
                    .Query(q => q
                        .Term(p => p
                            .Field("UserId")
                            .Value("users/1"))
                    )
                );

                Assert.Equal(0, userResponse.Documents.Count);
            }
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
                            new OrderLine {Cost = 3, Product = "Cheese", Quantity = 3}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    session.SaveChanges();
                }

                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var config = new ElasticSearchEtlConfiguration
                {
                    Name = "test",
                    ConnectionStringName = "test",
                    Transforms = { new Transformation { Name = "test", Collections = { "Orders" }, Script = @"this.TotalCost = 10;" } }
                };

                config.Initialize(new ElasticSearchConnectionString { Name = "Foo", Nodes = new[] { "http://localhost:9200" } });

                List<string> errors;
                config.Validate(out errors);

                Assert.Equal(1, errors.Count);

                Assert.Equal("No `loadTo<IndexName>()` method call found in 'test' script", errors[0]);
            }
        }

        [RequiresElasticSearchFact]
        public void Etl_from_encrypted_to_non_encrypted_db_will_work()
        {
            var certificates = Certificates.SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = Certificates.RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }

            var base64Key = Convert.ToBase64String(buffer);

            // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
            try
            {
#pragma warning disable CA1416 // Validate platform compatibility
                ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
#pragma warning restore CA1416 // Validate platform compatibility
            }
            catch (PlatformNotSupportedException)
            {
                // so we fall back to a file
                Server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();
            }

            Server.ServerStore.PutSecretKey(base64Key, dbName, true);

            using (var src = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                ModifyDatabaseName = s => dbName,
            }))
            using (GetElasticClient(out var client))
            {
                var config = new ElasticSearchEtlConfiguration
                {
                    ConnectionStringName = "test",
                    Name = "myFirstEtl",
                    ElasticIndexes = UsersIndex,
                    Transforms =
                    {
                        new Transformation
                        {
                            Collections = {"Users"}, Script = @"var userData = { UserId: id(this), Name: this.Name }; loadTo" + UsersIndexName + @"(userData)", Name = "a"
                        }
                    },
                    AllowEtlOnNonEncryptedChannel = true
                };
                AddEtl(src, config, new ElasticSearchConnectionString { Name = "test", Nodes = ElasticSearchTestNodes.Instance.VerifiedNodes.Value });

                var db = GetDatabase(src.Database).Result;

                Assert.Equal(1, db.EtlLoader.Processes.Length);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User { Name = "Joe Doe" });

                    session.SaveChanges();
                }

                AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), src.Database, config);

                var userResponse1 = client.Search<object>(d => d
                    .Index(UsersIndexName)
                    .Query(q => q
                        .Term(p => p
                            .Field("UserId")
                            .Value("users/1-a"))
                    )
                );

                Assert.Equal(1, userResponse1.Documents.Count);
                var userObject1 = JObject.FromObject(userResponse1.Documents.First()).ToObject<Dictionary<string, object>>();
                Assert.NotNull(userObject1);
                Assert.Equal("Joe Doe", userObject1["Name"]);
            }
        }

        [Fact]
        public void Can_check_elastic_connection_string_against_secured_channel()
        {
            var c = new ElasticSearchEtlConfiguration();

            c.Connection = new ElasticSearchConnectionString
            {
                Name = "Test",
                Nodes = new[] { "https://localhost:9200" }
            };

            Assert.True(c.UsingEncryptedCommunicationChannel());
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
                            new OrderLine {Cost = 3, Product = "Milk", Quantity = 3}, new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    await session.SaveChangesAsync();
                }

                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<ElasticSearchConnectionString>(new ElasticSearchConnectionString
                {
                    Name = "simulate",
                    Nodes = new[] { "http://localhost:9200" }
                }));
                Assert.NotNull(result1.RaftCommandIndex);

                var database = GetDatabase(store.Database).Result;

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    using (ElasticSearchEtl.TestScript(
                        new TestElasticSearchEtlScript
                        {
                            DocumentId = "orders/1-A",
                            Configuration = new ElasticSearchEtlConfiguration
                            {
                                Name = "simulate",
                                ConnectionStringName = "simulate",
                                ElasticIndexes =
                                {
                                    new ElasticSearchIndex { IndexName = "Orders", DocumentIdProperty = "Id" },
                                    new ElasticSearchIndex { IndexName = "OrderLines", DocumentIdProperty = "OrderId" },
                                    new ElasticSearchIndex { IndexName = "NotUsedInScript", DocumentIdProperty = "OrderId" },
                                },
                                Transforms =
                                {
                                    new Transformation
                                    {
                                        Collections = { "Orders" }, Name = "OrdersAndLines", Script = @"
var orderData = {
    Id: id(this),
    OrderLinesCount: this.OrderLines.length,
    TotalCost: 0
};

for (var i = 0; i < this.OrderLines.length; i++) {
    var line = this.OrderLines[i];
    var cost = (line.Quantity * line.PricePerUnit) *  ( 1 - line.Discount);
    orderData.TotalCost += line.Cost * line.Quantity;
    loadToOrderLines({
        OrderId: id(this),
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.Cost
    });
}

loadToOrders(orderData);

output('test output')"
                                    }
                                }
                            }
                        }, database, database.ServerStore, context, out var testResult))
                    {
                        var result = (ElasticSearchEtlTestScriptResult)testResult;

                        Assert.Equal(0, result.TransformationErrors.Count);

                        Assert.Equal(2, result.Summary.Count);

                        var orderLines = result.Summary.First(x => x.IndexName == "orderlines");

                        Assert.Equal(2, orderLines.Commands.Length); // delete by query and bulk

                        Assert.StartsWith("POST orderlines/_delete_by_query?refresh=true", orderLines.Commands[0]);
                        Assert.StartsWith("POST orderlines/_bulk?refresh=wait_for", orderLines.Commands[1]);

                        var orders = result.Summary.First(x => x.IndexName == "orders");

                        Assert.Equal(2, orders.Commands.Length); // refresh, delete by query and bulk

                        Assert.StartsWith("POST orders/_delete_by_query?refresh=true", orders.Commands[0]);
                        Assert.StartsWith("POST orders/_bulk?refresh=wait_for", orders.Commands[1]);

                        Assert.Equal("test output", result.DebugOutput[0]);
                    }
                }
            }
        }

        [Fact]
        public async Task CanTestDeletion()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        OrderLines = new List<OrderLine>
                        {
                            new OrderLine {Cost = 3, Product = "Milk", Quantity = 3},
                            new OrderLine {Cost = 4, Product = "Bear", Quantity = 2},
                        }
                    });
                    await session.SaveChangesAsync();
                }

                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<ElasticSearchConnectionString>(new ElasticSearchConnectionString
                {
                    Name = "simulate",
                    Nodes = new[] { "http://localhost:9200" }
                }));
                Assert.NotNull(result1.RaftCommandIndex);

                var database = GetDatabase(store.Database).Result;

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    using (ElasticSearchEtl.TestScript(
                        new TestElasticSearchEtlScript
                        {
                            DocumentId = "orders/1-A",
                            IsDelete = true,
                            Configuration = new ElasticSearchEtlConfiguration
                            {
                                Name = "simulate",
                                ConnectionStringName = "simulate",
                                ElasticIndexes =
                                {
                                    new ElasticSearchIndex { IndexName = OrdersIndexName, DocumentIdProperty = "Id" },
                                    new ElasticSearchIndex { IndexName = OrderLinesIndexName, DocumentIdProperty = "OrderId" },
                                    new ElasticSearchIndex { IndexName = "NotUsedInScript", DocumentIdProperty = "OrderId" },
                                },
                                Transforms =
                                {
                                    new Transformation
                                    {
                                        Collections = { "Orders" }, Name = "OrdersAndLines", Script = DefaultScript + "output('test output')"
                                    }
                                }
                            }
                        }, database, database.ServerStore, context, out var testResult))
                    {
                        var result = (ElasticSearchEtlTestScriptResult)testResult;

                        Assert.Equal(0, result.TransformationErrors.Count);

                        Assert.Equal(2, result.Summary.Count);

                        var orderLines = result.Summary.First(x => x.IndexName == OrderLinesIndexName);

                        Assert.Equal(1, orderLines.Commands.Length); // delete

                        var orders = result.Summary.First(x => x.IndexName == OrdersIndexName);

                        Assert.Equal(1, orders.Commands.Length); // delete by query
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.NotNull(session.Query<Order>("orders/1-A"));
                }
            }
        }

        [Fact]
        public async Task ShouldImportTask()
        {
            using (var srcStore = GetDocumentStore())
            using (var dstStore = GetDocumentStore())
            {
                SetupElasticEtl(srcStore, DefaultScript, DefaultIndexes, DefaultCollections, nodes: new [] {"http://localhost:1234"});

                var exportFile = GetTempFileName();

                var exportOperation = await srcStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
                await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var operation = await dstStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var destinationRecord = await dstStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dstStore.Database));
                Assert.Equal(1, destinationRecord.ElasticSearchEtls.Count);
                Assert.Equal(1, destinationRecord.ElasticSearchConnectionStrings.Count);
            }
        }

        private class Order
        {
            public Address Address { get; set; }
            public string Id { get; set; }
            public List<OrderLine> OrderLines { get; set; }
        }

        private class Address
        {
            public string City { get; set; }
        }

        private class OrderLine
        {
            public string Product { get; set; }
            public int Quantity { get; set; }
            public int Cost { get; set; }
        }
    }
}
