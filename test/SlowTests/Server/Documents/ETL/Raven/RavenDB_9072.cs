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
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_9072 : RavenTestBase
    {
        public RavenDB_9072(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanTestScript(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                const string docId = "orders/1-A";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine{PricePerUnit = 3, Product = "Milk", Quantity = 3},
                            new OrderLine{PricePerUnit = 4, Product = "Bear", Quantity = 2},
                        }
                    }, docId);

                    await session.SaveChangesAsync();

                    var database = await Etl.GetDatabaseFor(store, docId);

                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        using (RavenEtl.TestScript(new TestRavenEtlScript
                               {
                                   DocumentId = docId,
                                   Configuration = new RavenEtlConfiguration()
                                   {
                                       Name = "simulate",
                                       Transforms =
                                       {
                                           new Transformation()
                                           {
                                               Collections = {"Orders"},
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
                               }, database, database.ServerStore, context, out var testResult))
                        {
                            var result = (RavenEtlTestScriptResult)testResult;

                            Assert.Equal(0, result.TransformationErrors.Count);

                            Assert.Equal(4, result.Commands.Count);

                            Assert.Equal(1, result.Commands.OfType<DeletePrefixedCommandData>().Count());
                            Assert.Equal(3, result.Commands.OfType<PutCommandDataWithBlittableJson>().Count());

                            Assert.Equal("test output", result.DebugOutput[0]);
                        }
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanTestScriptSpecifiedOnMultipleCollections(Options options)
        {
            const string documentId = "orders/1-A";

            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order(), documentId);
                    session.SaveChanges();
                }

                var database = await Etl.GetDatabaseFor(store, documentId);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    RavenEtl.TestScript(new TestRavenEtlScript
                    {
                        DocumentId = documentId,
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
                    }, database, database.ServerStore, context, out _);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldThrowIfTestingOnDocumentBelongingToDifferentCollection(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                const string documentId = "orders/1-A";

                using (var session = store.OpenSession())
                {
                    session.Store(new Order(), documentId);
                    session.SaveChanges();
                }

                var database = await Etl.GetDatabaseFor(store, documentId);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var ex = Assert.Throws<InvalidOperationException>(() =>
                    {
                        return RavenEtl.TestScript(new TestRavenEtlScript
                        {
                            DocumentId = documentId,
                            Configuration = new RavenEtlConfiguration()
                            {
                                Name = "simulate",
                                Transforms =
                                {
                                    new Transformation()
                                    {
                                        Collections = { "DifferentCollection" },
                                        Name = "test",
                                        Script =
                                            @"
loadToDifferentCollection(this);"
                                    }
                                }
                            }
                        }, database, database.ServerStore, context, out _);
                    });

                    Assert.Contains(
                        "Document 'orders/1-A' belongs to Orders collection while tested ETL script works on the following collections: DifferentCollection",
                        ex.Message);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanTestEmptyScript(Options options)
        {
            const string documentId = "orders/1-A";
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order(), documentId);
                    await session.SaveChangesAsync();
                }

                var database = await Etl.GetDatabaseFor(store, documentId);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (RavenEtl.TestScript(
                   new TestRavenEtlScript
                   {
                       DocumentId = documentId,
                       Configuration = new RavenEtlConfiguration()
                       {
                           Name = "simulate", 
                           Transforms =
                           {
                               new Transformation()
                               {
                                   Collections = { "Orders" }, 
                                   Name = "OrdersAndLines", 
                                   Script = null
                               }
                           }
                       }
                   }, database, database.ServerStore, context, out var testResult))
                {

                    var result = (RavenEtlTestScriptResult)testResult;

                    Assert.Equal(0, result.TransformationErrors.Count);

                    Assert.Equal(1, result.Commands.Count);

                    Assert.IsType(typeof(PutCommandDataWithBlittableJson), result.Commands[0]);

                    Assert.Empty(result.DebugOutput);
                }
            }
        }
    }
}
