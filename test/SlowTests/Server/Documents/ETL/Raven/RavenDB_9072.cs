using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.ServerWide.Context;
using Xunit;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_9072 : RavenTestBase
    {
        [Fact]
        public async Task CanTestScript()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine{PricePerUnit = 3, Product = "Milk", Quantity = 3},
                            new OrderLine{PricePerUnit = 4, Product = "Bear", Quantity = 2},
                        }
                    });

                    await session.SaveChangesAsync();

                    var database = GetDatabase(store.Database).Result;

                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var result = (RavenEtlTestScriptResult)RavenEtl.TestScript(new TestRavenEtlScript
                        {
                            DocumentId = "orders/1-A",
                            Configuration = new RavenEtlConfiguration()
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
                                        Name = "OrdersAndLines",
                                        Script =
                                            @"
var orderData = {
    Id: id(this),
    LinesCount: this.Lines.length,
    TotalCost: 0
};

for (var i = 0; i < this.Lines.length; i++) {
    var line = this.Lines[i];
    orderData.TotalCost += line.PricePerUnit * line.Quantity;
    loadToOrderLines({
        OrderId: id(this),
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.PricePerUnit
    });
}

output('test output');

loadToOrders(orderData);"
                                    }
                                }
                            }
                        }, database, database.ServerStore, context);

                        Assert.Equal(0, result.TransformationErrors.Count);

                        Assert.Equal(4, result.Commands.Count);

                        Assert.Equal(1, result.Commands.OfType<DeletePrefixedCommandData>().Count());
                        Assert.Equal(3, result.Commands.OfType<PutCommandDataWithBlittableJson>().Count());

                        Assert.Equal("test output", result.DebugOutput[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanTestScriptSpecifiedOnMultipleCollections()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Order());

                    session.SaveChanges();
                }

                var database = GetDatabase(store.Database).Result;

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    RavenEtl.TestScript(new TestRavenEtlScript
                    {
                        DocumentId = "orders/1-A",
                        Configuration = new RavenEtlConfiguration()
                        {
                            Name = "simulate",
                            Transforms =
                                {
                                    new Transformation()
                                    {
                                        Collections =
                                        {
                                            "Orders",
                                            "SpecialOrders"
                                        },
                                        Name = "test",
                                        Script =
                                            @"
loadToOrders(this);"
                                    }
                                }
                        }
                    }, database, database.ServerStore, context);
                }
            }
        }

        [Fact]
        public void ShouldThrowIfTestingOnDocumentBelongingToDifferentCollection()
        {
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenSession())
                {
                    session.Store(new Order());

                    session.SaveChanges();
                }

                var database = GetDatabase(store.Database).Result;

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var ex = Assert.Throws<InvalidOperationException>(() => RavenEtl.TestScript(new TestRavenEtlScript
                    {
                        DocumentId = "orders/1-A",
                        Configuration = new RavenEtlConfiguration()
                        {
                            Name = "simulate",
                            Transforms =
                            {
                                new Transformation()
                                {
                                    Collections =
                                    {
                                        "DifferentCollection"
                                    },
                                    Name = "test",
                                    Script =
                                        @"
loadToDifferentCollection(this);"
                                }
                            }
                        }
                    }, database, database.ServerStore, context));

                    Assert.Contains(
                        "Document 'orders/1-A' belongs to Orders collection while tested ETL script works on the following collections: DifferentCollection",
                        ex.Message);
                }
            }
        }

        [Fact]
        public async Task CanTestEmptyScript()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order());

                    await session.SaveChangesAsync();

                    var database = GetDatabase(store.Database).Result;

                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var result = (RavenEtlTestScriptResult)RavenEtl.TestScript(new TestRavenEtlScript
                        {
                            DocumentId = "orders/1-A",
                            Configuration = new RavenEtlConfiguration()
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
                                        Name = "OrdersAndLines",
                                        Script = null
                                    }
                                }
                            }
                        }, database, database.ServerStore, context);

                        Assert.Equal(0, result.TransformationErrors.Count);

                        Assert.Equal(1, result.Commands.Count);

                        Assert.IsType(typeof(PutCommandDataWithBlittableJson), result.Commands[0]);

                        Assert.Empty(result.DebugOutput);
                    }
                }
            }
        }
    }
}
