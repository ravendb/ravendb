using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Orders;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Server.Documents.ETL.Providers.ElasticSearch;
using Raven.Server.Documents.ETL.Providers.ElasticSearch.Test;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.ElasticSearch
{

    public class RavenDB_17476 : ElasticSearchEtlTestBase
    {
        public RavenDB_17476(ITestOutputHelper output) : base(output)
        {
        }

        private string ScriptWithNoIdMethodUsage => @"
var orderData = {
    OrderLinesCount: this.OrderLines.length,
    TotalCost: 0
};

for (var i = 0; i < this.Lines.length; i++) {
    var line = this.Lines[i];
    var cost = (line.Quantity * line.PricePerUnit) *  ( 1 - line.Discount);
    orderData.TotalCost += cost;
    loadTo" + OrderLinesIndexName + @"({
        Qty: line.Quantity,
        Product: line.Product,
        Cost: cost
    });
}

loadTo" + OrdersIndexName + @"(orderData);
";

        [RequiresElasticSearchRetryFact]
        public async Task CanOmitDocumentIdPropertyInJsonPassedToLoadTo()
        {
            using (var store = GetDocumentStore())
            using (GetElasticClient(out var client))
            {
                var config = SetupElasticEtl(store, ScriptWithNoIdMethodUsage, DefaultIndexes, new List<string> { "Orders" });

                var etlDone = Etl.WaitForEtlToComplete(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine { PricePerUnit = 3, Product = "Cheese", Quantity = 3 },
                            new OrderLine { PricePerUnit = 4, Product = "Bear", Quantity = 2 },
                        }
                    });
                    await session.SaveChangesAsync();
                }

                await AssertEtlDoneAsync(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                var ordersCount = await client.CountAsync<object>(c => c.Indices(Indices.Index(OrdersIndexName)));
                var orderLinesCount = await client.CountAsync<object>(c => c.Indices(Indices.Index(OrderLinesIndexName)));

                Assert.True(ordersCount.IsValidResponse);
                Assert.True(orderLinesCount.IsValidResponse);

                Assert.Equal(1, ordersCount.Count);
                Assert.Equal(2, orderLinesCount.Count);

                etlDone.Reset();

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete("orders/1-A");

                    await session.SaveChangesAsync();
                }

                await AssertEtlDoneAsync(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                var ordersCountAfterDelete = await client.CountAsync<object>(c => c.Indices(Indices.Index(OrdersIndexName)));
                var orderLinesCountAfterDelete = await client.CountAsync<object>(c => c.Indices(Indices.Index(OrderLinesIndexName)));

                Assert.True(ordersCount.IsValidResponse);
                Assert.True(orderLinesCount.IsValidResponse);

                Assert.Equal(0, ordersCountAfterDelete.Count);
                Assert.Equal(0, orderLinesCountAfterDelete.Count);
            }
        }

        [Fact]
        public async Task TestScriptWillHaveDocumentIdPropertiesNotAddedExplicitlyInTheScript()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine { PricePerUnit = 3, Product = "Milk", Quantity = 3 },
                            new OrderLine { PricePerUnit = 4, Product = "Bear", Quantity = 2 },
                        }
                    });
                    await session.SaveChangesAsync();
                }

                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<ElasticSearchConnectionString>(new ElasticSearchConnectionString
                {
                    Name = "simulate", Nodes = new[] { "http://localhost:9200" }
                }));
                Assert.NotNull(result1.RaftCommandIndex);

                var database = await GetDatabase(store.Database);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var testResult = ElasticSearchEtl.TestScript(
                        new TestElasticSearchEtlScript
                        {
                            DocumentId = "orders/1-A",
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
                                Transforms = { new Transformation { Collections = { "Orders" }, Name = "OrdersAndLines", Script = ScriptWithNoIdMethodUsage } }
                            }
                        }, database, database.ServerStore, context);
                    
                    var result = (ElasticSearchEtlTestScriptResult)testResult;

                    Assert.Equal(0, result.TransformationErrors.Count);

                    Assert.Equal(2, result.Summary.Count);

                    var orderLines = result.Summary.First(x => x.IndexName == OrderLinesIndexName);

                    Assert.Equal(2, orderLines.Commands.Length); // delete by query and bulk

                    Assert.Contains(@"""OrderId"":""orders/1-a""", orderLines.Commands[1]);

                    var orders = result.Summary.First(x => x.IndexName == OrdersIndexName);

                    Assert.Equal(2, orders.Commands.Length); // delete by query and bulk

                    Assert.Contains(@"""Id"":""orders/1-a""", orders.Commands[1]);
                }
            }
        }
    }
}
