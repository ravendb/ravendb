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
    public class RavenDB_12011 : RavenTestBase
    {
        [Fact]
        public async Task CanTestDeletion()
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
                            IsDelete = true,
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

loadToOrders(orderData);
"
                                    }
                                }
                            }
                        }, database, database.ServerStore, context);

                        Assert.Equal(0, result.TransformationErrors.Count);

                        Assert.Equal(2, result.Commands.Count);

                        Assert.IsType(typeof(DeletePrefixedCommandData), result.Commands[0]);
                        Assert.IsType(typeof(DeleteCommandData), result.Commands[1]);
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.NotNull(session.Query<Order>("orders/1-A"));
                }
            }
        }
    }
}
