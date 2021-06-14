using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Raven.Server.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16827 : ReplicationTestBase

    {
        public RavenDB_16827(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ElectionLoop()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var clusterSize = 2;
            var databaseName = GetDatabaseName();
            var settings = new Dictionary<string, string>() {[RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = "50",};
            var cluster = await CreateRaftCluster(clusterSize, false, watcherCluster: false, customSettings: settings);
            using (var store = GetDocumentStore(new Options {Server = cluster.Leader, ReplicationFactor = 2}).Initialize())
            {
                var order = new Order()
                {
                    Company = "Toli",
                    Employee = "Mitzi",
                    Lines = new List<Lines>()
                    {
                        new Lines()
                        {
                            Discount = 1,
                            PricePerUnit = 1,
                            Product = "1",
                            ProductName = "1",
                            Quantity = 1
                        },
                        new Lines()
                        {
                            Discount = 2,
                            PricePerUnit = 2,
                            Product = "2",
                            ProductName = "2",
                            Quantity = 2
                        },
                        new Lines()
                        {
                            Discount = 3,
                            PricePerUnit = 3,
                            Product = "3",
                            ProductName = "3",
                            Quantity = 3
                        },
                        new Lines()
                        {
                            Discount = 4,
                            PricePerUnit = 4,
                            Product = "4",
                            ProductName = "4",
                            Quantity = 4
                        }
                    }
                };
                for (int i = 0; i < 12; i++)
                {
                    order.Lines.AddRange(order.Lines);
                }

                using (var session = store.OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
                {
                    await session.StoreAsync(order, "orders/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var res = WaitForValue(() =>
                    {
                        var order2 = session.Load<Order>("orders/1");
                        return order2 == null;
                    }, false, 500);

                    Assert.False(res);
                }


            }
        }

        public class Lines
        {
            public float Discount { get; set; }
            public float PricePerUnit { get; set; }
            public string Product { get; set; }
            public string ProductName { get; set; }
            public int Quantity { get; set; }
        }

        public class Order
        {
            public string Company { get; set; }
            public string Employee { get; set; }
            public float Freight { get; set; }
            public List<Lines> Lines { get; set; }
            public DateTimeOffset OrderedAt { get; set; }
            public DateTimeOffset RequireAt { get; set; }
            public string ShipVia { get; set; }
            public object ShippedAt { get; set; }
        }
    }
}
