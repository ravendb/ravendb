using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Nest;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Elasticsearch;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Documents.ETL.Providers.Elasticsearch;
using Raven.Server.Documents.ETL.Providers.Elasticsearch.Test;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Elasticsearch
{
    public class ElasticsearchEtlTests : EtlTestBase
    {
        public ElasticsearchEtlTests(ITestOutputHelper output) : base(output)
        {
        }

        protected const string defaultScript = @"
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
";

        [Fact]
        public void Simple_script()
        {
            using (var store = GetDocumentStore())
            {
                SetupElasticEtl(store, defaultScript, new List<string>() {"Orders", "OrderLines"});
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

                var node = new Uri("http://localhost:9200");
                var settings = new ConnectionSettings(node);
                var client = new ElasticClient(settings);

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var ordersCount = client.Count<object>(c => c.Index("orders"));
                var orderLinesCount = client.Count<object>(c => c.Index("orderlines"));

                Assert.Equal(1, ordersCount.Count);
                Assert.Equal(2, orderLinesCount.Count);

                etlDone.Reset();

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/1-A");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var ordersCountAfterDelete = client.Count<object>(c => c.Index("orders"));
                var orderLinesCountAfterDelete = client.Count<object>(c => c.Index("orderlines"));

                Assert.Equal(0, ordersCountAfterDelete.Count);
                Assert.Equal(0, orderLinesCountAfterDelete.Count);
            }
        }
        
        [Fact]
        public void Simple_script_error_expected()
        {
            using (var store = GetDocumentStore())
            {
                AddEtl(store,
                    new ElasticsearchEtlConfiguration()
                    {
                        ConnectionStringName = "test",
                        Name = "myFirstEtl",
                        ElasticIndexes =
                        {
                            new ElasticsearchIndex {IndexName = "Orders", IndexIdProperty = "Id"},
                            new ElasticsearchIndex {IndexName = "OrderLines", IndexIdProperty = "OrderId"},
                        },
                        Transforms =
                        {
                            new Transformation()
                            {
                                Collections = {"Orders", "OrderLines"},
                                Script = defaultScript,
                                Name = "a"
                            }
                        },
                    }, new ElasticsearchConnectionString {Name = "test", Nodes = new[] {"http://localhost:9300"}}); //wrong elastic search url
                
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

                etlDone.Wait(TimeSpan.FromSeconds(30));
                
                var key = "AlertRaised/Etl_LoadError/ELASTICSEARCH ETL/myFirstEtl/a";
                var alert = GetDatabase(store.Database).Result.NotificationCenter.GetStoredMessage(key);
                Assert.Equal("Loading transformed data to the destination has failed (last 500 errors are shown)", alert);
            }
        }

        [Fact]
        public void Can_get_document_id()
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

                SetupElasticEtl(store, defaultScript, new List<string>() {"Orders", "OrderLines"});

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var node = new Uri("http://localhost:9200");
                var settings = new ConnectionSettings(node);
                var client = new ElasticClient(settings);

                var orderResponse = client.Search<object>(d => d
                    .Index("orders")
                    .Query(q => q
                        .Match(p => p
                            .Field("Id")
                            .Query("orders/1-A"))
                    )
                );

                var orderLineResponse = client.Search<object>(d => d
                    .Index("orderlines")
                    .Query(q => q
                        .Match(p => p
                            .Field("OrderId")
                            .Query("orders/1-A"))
                    )
                );

                Assert.Equal(1, orderResponse.Documents.Count);
                Assert.Equal("orders/1-A", JObject.FromObject(orderResponse.Documents.First()).ToObject<Dictionary<string, object>>()?["Id"]);

                Assert.Equal(2, orderLineResponse.Documents.Count);

                foreach (var document in orderLineResponse.Documents)
                {
                    Assert.Equal("orders/1-A", JObject.FromObject(document).ToObject<Dictionary<string, object>>()?["OrderId"]);
                }

                etlDone.Reset();

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/1-A");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var ordersCountAfterDelete = client.Count<object>(c => c.Index("orders"));
                var orderLinesCountAfterDelete = client.Count<object>(c => c.Index("orderlines"));

                Assert.Equal(0, ordersCountAfterDelete.Count);
                Assert.Equal(0, orderLinesCountAfterDelete.Count);
            }
        }

        [Fact]
        public void Can_Update_To_Be_No_Items_In_Child_TTable()
        {
            using (var store = GetDocumentStore())
            {
                SetupElasticEtl(store, defaultScript, new List<string>() {"Orders", "OrderLines"});
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

                var node = new Uri("http://localhost:9200");
                var settings = new ConnectionSettings(node);
                var client = new ElasticClient(settings);

                etlDone.Wait(TimeSpan.FromSeconds(30));

                var ordersCount = client.Count<object>(c => c.Index("orders"));
                var orderLinesCount = client.Count<object>(c => c.Index("orderlines"));

                Assert.Equal(1, ordersCount.Count);
                Assert.Equal(2, orderLinesCount.Count);

                etlDone.Reset();

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1-A");
                    order.OrderLines.Clear();
                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromSeconds(90));

                var ordersCountAfterDelete = client.Count<object>(c => c.Index("orders"));
                var orderLinesCountAfterDelete = client.Count<object>(c => c.Index("orderlines"));

                Assert.Equal(1, ordersCountAfterDelete.Count);
                Assert.Equal(0, orderLinesCountAfterDelete.Count);
            }
        }

        [Fact]
        public void Update_of_disassembled_document()
        {
            using (var store = GetDocumentStore())
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

                SetupElasticEtl(store, defaultScript, new List<string>() {"Orders", "OrderLines"});
                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);


                etlDone.Wait(TimeSpan.FromMinutes(1));

                var node = new Uri("http://localhost:9200");
                var settings = new ConnectionSettings(node);
                var client = new ElasticClient(settings);

                var orderResponse = client.Search<object>(d => d
                    .Index("orders")
                    .Query(q => q
                        .Match(p => p
                            .Field("Id")
                            .Query("orders/1-A"))
                    )
                );

                Assert.Equal(1, orderResponse.Documents.Count);

                var orderObject = JObject.FromObject(orderResponse.Documents.First()).ToObject<Dictionary<string, object>>();

                Assert.NotNull(orderObject);
                Assert.Equal("orders/1-A", orderObject["Id"]);
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

                etlDone.Wait(TimeSpan.FromMinutes(2));

                var orderResponse1 = client.Search<object>(d => d
                    .Index("orders")
                    .Query(q => q
                        .Match(p => p
                            .Field("Id")
                            .Query("orders/1-A"))
                    )
                );

                Assert.Equal(1, orderResponse1.Documents.Count);

                var orderObject1 = JObject.FromObject(orderResponse1.Documents.First()).ToObject<Dictionary<string, object>>();

                Assert.NotNull(orderObject1);
                Assert.Equal("orders/1-A", orderObject1["Id"]);
                Assert.Equal(2, (int)(long)orderObject1["OrderLinesCount"]);
                Assert.Equal(30, (int)(long)orderObject1["TotalCost"]);
            }
        }

        [Fact]
        public void Docs_from_two_collections_loaded_to_single_one()
        {
            using (var store = GetDocumentStore())
            {
                SetupElasticEtl(store, @"var userData = { UserId: id(this), Name: this.Name }; loadToUsers(userData)", new[] {"Users", "People"});
                var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Joe Doe"}, "users/1");

                    session.Store(new Person {Name = "James Smith"}, "people/1");

                    session.SaveChanges();
                }
                
                etlDone.Wait(TimeSpan.FromSeconds(20));

                var node = new Uri("http://localhost:9200");
                var settings = new ConnectionSettings(node);
                var client = new ElasticClient(settings);

                var userResponse1 = client.Search<object>(d => d
                    .Index("users")
                    .Query(q => q
                        .MatchPhrase(p => p
                            .Field("UserId")
                            .Query("users/1"))
                    )
                );

                Assert.Equal(1, userResponse1.Documents.Count);
                var userObject1 = JObject.FromObject(userResponse1.Documents.First()).ToObject<Dictionary<string, object>>();
                Assert.NotNull(userObject1);
                Assert.Equal("Joe Doe", userObject1["Name"]);

                var userResponse2 = client.Search<object>(d => d
                    .Index("users")
                    .Query(q => q
                        .MatchPhrase(p => p
                            .Field("UserId")
                            .Query("people/1"))
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
                    session.Store(new User {Name = "Doe Joe"}, "users/1");

                    session.Store(new Person {Name = "Smith James"}, "people/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromSeconds(20));

                var userResponse3 = client.Search<object>(d => d
                    .Index("users")
                    .Query(q => q
                        .MatchPhrase(p => p
                            .Field("UserId")
                            .Query("users/1"))
                    )
                );

                Assert.Equal(1, userResponse3.Documents.Count);
                var userObject3 = JObject.FromObject(userResponse3.Documents.First()).ToObject<Dictionary<string, object>>();
                Assert.NotNull(userObject3);
                Assert.Equal("Doe Joe", userObject3["Name"]);

                var userResponse4 = client.Search<object>(d => d
                    .Index("users")
                    .Query(q => q
                        .MatchPhrase(p => p
                            .Field("UserId")
                            .Query("people/1"))
                    )
                );


                Assert.Equal(1, userResponse4.Documents.Count);
                var userObject4 = JObject.FromObject(userResponse4.Documents.First()).ToObject<Dictionary<string, object>>();
                Assert.NotNull(userObject4);
                Assert.Equal("Smith James", userObject4["Name"]);
            }
        }

        [Fact]
        public void Can_load_to_specific_collection_when_applying_to_all_docs()
        {
            using (var src = GetDocumentStore())
            {
                var etlDone = WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupElasticEtl(src, @"var userData = { UserId: id(this), FirstName: this.Name, LastName: this.LastName }; loadToUsers(userData)", new List<string>(),
                    applyToAllDocuments: true);

                using (var session = src.OpenSession())
                {
                    session.Store(new User {Name = "James", LastName = "Smith"}, "users/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromSeconds(30));

                var node = new Uri("http://localhost:9200");
                var settings = new ConnectionSettings(node);
                var client = new ElasticClient(settings);

                var userResponse = client.Search<object>(d => d
                    .Index("users")
                    .Query(q => q
                        .Match(p => p
                            .Field("UserId")
                            .Query("users/1"))
                    )
                );

                Assert.Equal("users/1", JObject.FromObject(userResponse.Documents.First()).ToObject<Dictionary<string, object>>()?["UserId"]);
            }
        }

        [Fact]
        public void Should_delete_existing_document_when_filtered_by_script()
        {
            using (var src = GetDocumentStore())
            {
                var etlDone = WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupElasticEtl(src,
                    @"var userData = { UserId: id(this), FirstName: this.Name, LastName: this.LastName }; if (this.Name == 'Joe Doe') loadToUsers(userData)",
                    new List<string>() {"Users"});

                using (var session = src.OpenSession())
                {
                    session.Store(new User {Name = "Joe Doe"}, "users/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var node = new Uri("http://localhost:9200");
                var settings = new ConnectionSettings(node);
                var client = new ElasticClient(settings);

                var userResponse = client.Search<object>(d => d
                    .Index("users")
                    .Query(q => q
                        .Match(p => p
                            .Field("UserId")
                            .Query("users/1"))
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

                etlDone.Wait(TimeSpan.FromMinutes(1));

                userResponse = client.Search<object>(d => d
                    .Index("users")
                    .Query(q => q
                        .Match(p => p
                            .Field("UserId")
                            .Query("users/1"))
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

                var config = new ElasticsearchEtlConfiguration
                {
                    Name = "test",
                    ConnectionStringName = "test",
                    Transforms = {new Transformation {Name = "test", Collections = {"Orders"}, Script = @"this.TotalCost = 10;"}}
                };

                config.Initialize(new ElasticsearchConnectionString() {Name = "Foo", Nodes = new[] {"http://localhost:9200"}});

                List<string> errors;
                config.Validate(out errors);

                Assert.Equal(1, errors.Count);

                Assert.Equal("No `loadTo<IndexName>()` method call found in 'test' script", errors[0]);
            }
        }

        [Fact]
        public void Etl_from_encrypted_to_non_encrypted_db_will_work()
        {
            var certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            var adminCert = RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

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
                AdminCertificate = adminCert, ClientCertificate = adminCert, ModifyDatabaseRecord = record => record.Encrypted = true, ModifyDatabaseName = s => dbName,
            }))
            {
                AddEtl(src,
                    new ElasticsearchEtlConfiguration()
                    {
                        ConnectionStringName = "test",
                        Name = "myFirstEtl",
                        ElasticIndexes = {new ElasticsearchIndex {IndexName = "Users", IndexIdProperty = "UserId"},},
                        Transforms =
                        {
                            new Transformation()
                            {
                                Collections = {"Users"}, Script = @"var userData = { UserId: id(this), Name: this.Name }; loadToUsers(userData)", Name = "a"
                            }
                        },
                        AllowEtlOnNonEncryptedChannel = true
                    }, new ElasticsearchConnectionString {Name = "test", Nodes = new[] {"http://localhost:9200"}});

                var db = GetDatabase(src.Database).Result;

                Assert.Equal(1, db.EtlLoader.Processes.Length);

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User() {Name = "Joe Doe"});

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                var node = new Uri("http://localhost:9200");
                var settings = new ConnectionSettings(node);
                var client = new ElasticClient(settings);

                var userResponse1 = client.Search<object>(d => d
                    .Index("users")
                    .Query(q => q
                        .MatchPhrase(p => p
                            .Field("UserId")
                            .Query("users/1"))
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
            var c = new ElasticsearchEtlConfiguration();

            c.Connection = new ElasticsearchConnectionString()
            {
                Name = "Test",
                Nodes = new[] {"https://localhost:9200"}
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

                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<ElasticsearchConnectionString>(new ElasticsearchConnectionString()
                {
                    Name = "simulate", Nodes = new[] {"http://localhost:9200"}
                }));
                Assert.NotNull(result1.RaftCommandIndex);

                var database = GetDatabase(store.Database).Result;

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var result = (ElasticsearchEtlTestScriptResult)ElasticsearchEtl.TestScript(
                        new TestElasticsearchEtlScript
                        {
                            DocumentId = "orders/1-A",
                            Configuration = new ElasticsearchEtlConfiguration()
                            {
                                Name = "simulate",
                                ConnectionStringName = "simulate",
                                ElasticIndexes =
                                {
                                    new ElasticsearchIndex {IndexName = "Orders", IndexIdProperty = "Id"},
                                    new ElasticsearchIndex {IndexName = "OrderLines", IndexIdProperty = "OrderId"},
                                    new ElasticsearchIndex {IndexName = "NotUsedInScript", IndexIdProperty = "OrderId"},
                                },
                                Transforms =
                                {
                                    new Transformation() {Collections = {"Orders"}, Name = "OrdersAndLines", Script = defaultScript + "output('test output')"}
                                }
                            }
                        }, database, database.ServerStore, context);

                    Assert.Equal(0, result.TransformationErrors.Count);

                    Assert.Equal(2, result.Summary.Count);

                    var orderLines = result.Summary.First(x => x.IndexName == "orderlines");

                    Assert.Equal(3, orderLines.Commands.Length); // delete and two inserts

                    var orders = result.Summary.First(x => x.IndexName == "orders");

                    Assert.Equal(2, orders.Commands.Length); // delete and insert

                    Assert.Equal("test output", result.DebugOutput[0]);
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

                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<ElasticsearchConnectionString>(new ElasticsearchConnectionString()
                {
                    Name = "simulate", Nodes = new[] {"http://localhost:9200"}
                }));
                Assert.NotNull(result1.RaftCommandIndex);

                var database = GetDatabase(store.Database).Result;

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var result = (ElasticsearchEtlTestScriptResult)ElasticsearchEtl.TestScript(
                        new TestElasticsearchEtlScript
                        {
                            DocumentId = "orders/1-A",
                            IsDelete = true,
                            Configuration = new ElasticsearchEtlConfiguration()
                            {
                                Name = "simulate",
                                ConnectionStringName = "simulate",
                                ElasticIndexes =
                                {
                                    new ElasticsearchIndex {IndexName = "Orders", IndexIdProperty = "Id"},
                                    new ElasticsearchIndex {IndexName = "OrderLines", IndexIdProperty = "OrderId"},
                                    new ElasticsearchIndex {IndexName = "NotUsedInScript", IndexIdProperty = "OrderId"},
                                },
                                Transforms =
                                {
                                    new Transformation() {Collections = {"Orders"}, Name = "OrdersAndLines", Script = defaultScript + "output('test output')"}
                                }
                            }
                        }, database, database.ServerStore, context);

                    Assert.Equal(0, result.TransformationErrors.Count);

                    Assert.Equal(2, result.Summary.Count);

                    var orderLines = result.Summary.First(x => x.IndexName == "orderlines");

                    Assert.Equal(1, orderLines.Commands.Length); // delete

                    var orders = result.Summary.First(x => x.IndexName == "orders");

                    Assert.Equal(1, orders.Commands.Length); // delete
                }
                
                using (var session = store.OpenAsyncSession())
                {
                    Assert.NotNull(session.Query<Order>("orders/1-A"));
                }
            }
        }

        protected void SetupElasticEtl(DocumentStore store, string script, IEnumerable<string> collections = null, bool applyToAllDocuments = false)
        {
            var connectionStringName = $"{store.Database}@{store.Urls.First()} to ELASTIC";

            AddEtl(store,
                new ElasticsearchEtlConfiguration()
                {
                    Name = connectionStringName,
                    ConnectionStringName = connectionStringName,
                    ElasticIndexes =
                    {
                        new ElasticsearchIndex {IndexName = "Orders", IndexIdProperty = "Id"},
                        new ElasticsearchIndex {IndexName = "OrderLines", IndexIdProperty = "OrderId"},
                        new ElasticsearchIndex {IndexName = "Users", IndexIdProperty = "UserId"},
                    },
                    Transforms =
                    {
                        new Transformation()
                        {
                            Name = $"ETL : {connectionStringName}",
                            Collections = new List<string>(collections),
                            Script = script,
                            ApplyToAllDocuments = applyToAllDocuments
                        }
                    }
                },
                new ElasticsearchConnectionString {Name = connectionStringName, Nodes = new[] {"http://localhost:9200"}});
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
