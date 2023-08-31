using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Providers.Raven.Test;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_12011 : RavenTestBase
    {
        public RavenDB_12011(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanTestDeletion(Options options)
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
                            new OrderLine{PricePerUnit = 4, Product = "Beer", Quantity = 2}
                        }
                    }, docId);

                    await session.SaveChangesAsync();

                    var database = await Etl.GetDatabaseFor(store, docId);

                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        using (RavenEtl.TestScript(new TestRavenEtlScript
                        {
                            DocumentId = docId,
                            IsDelete = true,
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

loadToOrders(orderData);
"
                                    }
                                }
                            }
                        }, database, database.ServerStore, context, out var testResult))
                        {
                            var result = (RavenEtlTestScriptResult)testResult;
                            
                            Assert.Equal(0, result.TransformationErrors.Count);

                            Assert.Equal(2, result.Commands.Count);

                            Assert.IsType(typeof(DeletePrefixedCommandData), result.Commands[0]);
                            Assert.IsType(typeof(DeleteCommandData), result.Commands[1]);
                        }
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.NotNull(session.Query<Order>(docId));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanOutputInDeleteBehaviorFunction(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                const string docId = "users/1";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User()
                    {
                        Name = "Joe"
                    }, docId);

                    await session.SaveChangesAsync();
                    var database = await Etl.GetDatabaseFor(store, docId);

                    using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        using (RavenEtl.TestScript(new TestRavenEtlScript
                        {
                            DocumentId = docId,
                            IsDelete = true,
                            Configuration = new RavenEtlConfiguration()
                            {
                                Name = "simulate",
                                Transforms =
                                {
                                    new Transformation()
                                    {
                                        Collections = {"Users"},
                                        Name = "Users",
                                        Script =
                                            @"
loadToUsers(this);

function deleteDocumentsOfUsersBehavior(docId) {
    output('document: ' + docId);
    return false;
}
"
                                    }
                                }
                            }
                        }, database, database.ServerStore, context, out var testResult))
                        {
                            var result = (RavenEtlTestScriptResult)testResult;
                            
                            Assert.Equal(0, result.TransformationErrors.Count);

                            Assert.Equal(0, result.Commands.Count);

                            Assert.Equal("document: users/1", result.DebugOutput[0]);
                        }
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.NotNull(session.Query<Order>(docId));
                }
            }
        }
    }
}
