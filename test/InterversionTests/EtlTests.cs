using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orders;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class EtlTests : InterversionTestBase
    {
        public EtlTests(ITestOutputHelper output) : base(output)
        {
        }

        private class OrderWithLinesCount
        {
            public int OrderLinesCount { get; set; }
            public decimal TotalCost { get; set; }
        }

        private class LineItemWithTotalCost
        {
            public string ProductName { get; set; }
            public decimal Cost { get; set; }
            public int Quantity { get; set; }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public async Task RavenEtlFromShardedCurrentTo54X()
        {
            using (var srcCurrent = Sharding.GetDocumentStore())
            using (var dest54 = await GetDocumentStoreAsync(Server54Version))
            {
                Etl.AddEtl(srcCurrent, dest54, "Users", script: @"this.Name = 'James Doe';
                                       loadToUsers(this);");

                var etlDone = Etl.WaitForEtlToComplete(srcCurrent);

                using (var session = srcCurrent.OpenSession())
                {
                    session.Store(new User { Name = "Joe Doe" });
                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest54.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");

                    Assert.NotNull(user);
                    Assert.Equal("James Doe", user.Name);
                }

                etlDone.Reset();

                using (var session = srcCurrent.OpenSession())
                {
                    session.Delete("users/1-A");
                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest54.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");

                    Assert.Null(user);
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public async Task RavenEtlFrom54XToShardedCurrent()
        {
            using (var src54 = await GetDocumentStoreAsync(Server54Version))
            using (var destCurrent = Sharding.GetDocumentStore())
            {
                Etl.AddEtl(src54, destCurrent, "Users", script: @"this.Name = 'James Doe';
                                       loadToUsers(this);");

                using (var session = src54.OpenSession())
                {
                    session.Store(new User { Name = "Joe Doe" });
                    session.SaveChanges();
                }

                await Etl.AssertEtlReachedDestination(() =>
                {
                    using (var session = destCurrent.OpenSession())
                    {
                        var user = session.Load<User>("users/1-A");

                        Assert.NotNull(user);
                        Assert.Equal("James Doe", user.Name);
                    }
                }, 60_000, 333);

                using (var session = src54.OpenSession())
                {
                    session.Delete("users/1-A");

                    session.SaveChanges();
                }

                await Etl.AssertEtlReachedDestination(() =>
                {
                    using (var session = destCurrent.OpenSession())
                    {
                        var user = session.Load<User>("users/1-A");

                        Assert.Null(user);
                    }
                }, 60_000, 333);
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public async Task RavenEtlBetween54XAndCurrent()
        {
            using (var store54 = await GetDocumentStoreAsync(Server54Version))
            using (var storeCurrent = Sharding.GetDocumentStore())
            {
                Etl.AddEtl(store54, storeCurrent, "Users", script: @"this.Name = 'James Doe';
                                       loadToUsers(this);"
                );

                Etl.AddEtl(storeCurrent, store54, "Orders", @"
var orderData = {
    OrderLinesCount: this.Lines.length,
    TotalCost: 0
};

for (var i = 0; i < this.Lines.length; i++) {
    var line = this.Lines[i];
    var cost = (line.Quantity * line.PricePerUnit) *  ( 1 - line.Discount);

    orderData.TotalCost += cost;

    loadToOrderLines({
        Quantity: line.Quantity,
        ProductName: line.ProductName,
        Cost: cost
    });
}

loadToOrders(orderData);
");

                using (var session = store54.OpenSession())
                {
                    session.Store(new User { Name = "Joe Doe" });
                    session.SaveChanges();
                }

                using (var session = storeCurrent.OpenSession())
                {
                    session.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                ProductName = "a",
                                PricePerUnit = 10,
                                Quantity = 1
                            },
                            new OrderLine
                            {
                                ProductName = "b",
                                PricePerUnit = 10,
                                Quantity = 2
                            }
                        }
                    });

                    session.SaveChanges();
                }


                await Etl.AssertEtlReachedDestination(() =>
                {
                    using (var session = storeCurrent.OpenSession())
                    {
                        var user = session.Load<User>("users/1-A");

                        Assert.NotNull(user);
                        Assert.Equal("James Doe", user.Name);
                    }
                }, 60_000, 333);

                await Etl.AssertEtlReachedDestination(() =>
                {
                    using (var session = store54.OpenSession())
                    {
                        var order = session.Load<OrderWithLinesCount>("orders/1-A");

                        Assert.Equal(2, order.OrderLinesCount);
                        Assert.Equal(30, order.TotalCost);

                        var lines = session.Advanced.LoadStartingWith<LineItemWithTotalCost>("orders/1-A/OrderLines/").OrderBy(x => x.ProductName).ToList();

                        Assert.Equal(2, lines.Count);

                        Assert.Equal(10, lines[0].Cost);
                        Assert.Equal("a", lines[0].ProductName);
                        Assert.Equal(1, lines[0].Quantity);

                        Assert.Equal(20, lines[1].Cost);
                        Assert.Equal("b", lines[1].ProductName);
                        Assert.Equal(2, lines[1].Quantity);
                    }
                }, 60_000, 333);
            }
        }
    }
}
