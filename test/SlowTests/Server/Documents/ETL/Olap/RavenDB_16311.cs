using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Client;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Server.Documents.ETL.Providers.OLAP;
using Raven.Server.Documents.ETL.Providers.OLAP.Test;
using Raven.Server.ServerWide.Context;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Olap
{
    public class RavenDB_16311 : EtlTestBase
    {
        public RavenDB_16311(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanTestOlapEtlScript()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 31; i++)
                    {
                        await session.StoreAsync(new Query.Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = baseline.AddDays(i),
                            ShipVia = $"shippers/{i}",
                            Company = $"companies/{i}"
                        });
                    }

                    for (int i = 0; i < 28; i++)
                    {
                        await session.StoreAsync(new Query.Order
                        {
                            Id = $"orders/{i + 31}",
                            OrderedAt = baseline.AddMonths(1).AddDays(i),
                            ShipVia = $"shippers/{i + 31}",
                            Company = $"companies/{i + 31}"
                        });
                    }

                    await session.SaveChangesAsync();
                }

                var database = await GetDatabase(store.Database);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var result = (OlapEtlTestScriptResult)OlapEtl.TestScript(new TestOlapEtlScript
                    {
                        DocumentId = "orders/1",
                        Configuration = new OlapEtlConfiguration()
                        {
                            Name = "simulate",
                            Transforms =
                                {
                                    new Transformation()
                                    {
                                        Collections =
                                        {
                                            "Orders"
                                        },
                                        Name = "MonthlyOrders",
                                        Script =
                                            @"
                                                var orderDate = new Date(this.OrderedAt);
                                                var year = orderDate.getFullYear();
                                                var month = orderDate.getMonth();
                                                var key = new Date(year, month);

                                                output('test output')

                                                loadToOrders(partitionBy(['order_date', key]),
                                                    {
                                                        Company : this.Company,
                                                        ShipVia : this.ShipVia
                                                    });
                                                "
                                    }
                                }
                        }
                    }, database, database.ServerStore, context);

                    Assert.Equal(0, result.TransformationErrors.Count);

                    Assert.Equal(1, result.ItemsByPartition.Count);

                    Assert.Equal(4, result.ItemsByPartition[0].Columns.Count);

                    var companyColumn = result.ItemsByPartition[0].Columns.First(x => x.Name == "Company");
                    Assert.Equal("companies/1", companyColumn.Values[0]);

                    var shipViaColumn = result.ItemsByPartition[0].Columns.First(x => x.Name == "ShipVia");
                    Assert.Equal("shippers/1", shipViaColumn.Values[0]);

                    var idColumn = result.ItemsByPartition[0].Columns.First(x => x.Name == "_id");
                    Assert.Equal("orders/1", idColumn.Values[0]);

                    var lastModifiedColumn = result.ItemsByPartition[0].Columns.First(x => x.Name == "_lastModifiedTicks");
                    Assert.NotNull(lastModifiedColumn.Values[0]);

                    Assert.Equal("Orders/order_date=2020-01-01-00-00", result.ItemsByPartition[0].Key);

                    Assert.Equal("test output", result.DebugOutput[0]);
                }
            }
        }
    }
}
