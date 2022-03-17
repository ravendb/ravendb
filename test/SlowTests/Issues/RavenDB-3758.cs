using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using SlowTests.Core.Utils.Entities.Faceted;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3758 : RavenTestBase
    {
        public RavenDB_3758(ITestOutputHelper output) : base(output)
        {
        }

        private class Orders_All : AbstractIndexCreationTask<Order>
        {
            public override string IndexName
            {
                get { return "Orders/All"; }
            }

            public Orders_All()
            {
                Map = orders =>
                      from order in orders
                      select new { order.Currency, order.Product, order.Total, order.Quantity, order.Region, Val = "One-123" };
            }
        }

        private class Orders_All_Changed : AbstractIndexCreationTask<Order>
        {
            public override string IndexName
            {
                get { return "Orders/All"; }
            }

            public Orders_All_Changed()
            {
                Map = orders =>
                      from order in orders
                      select new { order.Currency, order.Product, order.Total, order.Quantity, order.Region, order.At, Val = "Two-123" };
            }
        }

        private class Orders_All_Changed2 : AbstractIndexCreationTask<Order>
        {
            public override string IndexName
            {
                get { return "Orders/All"; }
            }

            public Orders_All_Changed2()
            {
                Map = orders =>
                      from order in orders
                      select new { order.Currency, order.Product, order.Total, order.Quantity, order.Region, order.At, order.Tax, Val = "Three-123" };
            }
        }

        [Fact]
        public void Can_Overwrite_Side_By_Side_Index()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_All().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Currency = Currency.EUR, Product = "Milk", Total = 1000, Quantity = 1 });
                    session.Store(new Order { Currency = Currency.NIS, Product = "Milk", Total = 2000, Quantity = 2 });
                    session.Store(new Order
                    {
                        Currency = Currency.EUR,
                        Product = "iPhone",
                        Total = 3000,
                        Quantity = 3
                    });
                    session.SaveChanges();
                }
                Indexes.WaitForIndexing(store);

                Assert.Equal(1, store.Maintenance.Send(new GetStatisticsOperation()).CountOfIndexes);

                store.Maintenance.Send(new StopIndexingOperation());

                new Orders_All_Changed().Execute(store);
                new Orders_All_Changed2().Execute(store);

                Assert.Equal(2, store.Maintenance.Send(new GetStatisticsOperation()).CountOfIndexes);

                var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10));
                var index = indexes.Single(x => x.Name == $"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{new Orders_All().IndexName}");

                Assert.Contains("Three-123", index.Maps.First());
            }
        }

        [Fact]
        public async Task Can_Overwrite_Side_By_Side_Index_Async()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_All().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Currency = Currency.EUR, Product = "Milk", Total = 1000, Quantity = 1 });
                    session.Store(new Order { Currency = Currency.NIS, Product = "Milk", Total = 2000, Quantity = 2 });
                    session.Store(new Order
                    {
                        Currency = Currency.EUR,
                        Product = "iPhone",
                        Total = 3000,
                        Quantity = 3
                    });
                    session.SaveChanges();
                }
                Indexes.WaitForIndexing(store);

                Assert.Equal(1, store.Maintenance.Send(new GetStatisticsOperation()).CountOfIndexes);

                store.Maintenance.Send(new StopIndexingOperation());

                await new Orders_All_Changed().ExecuteAsync(store);
                await new Orders_All_Changed2().ExecuteAsync(store);

                Assert.Equal(2, store.Maintenance.Send(new GetStatisticsOperation()).CountOfIndexes);

                var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10));
                var index = indexes.Single(x => x.Name == $"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}{new Orders_All().IndexName}");

                Assert.Contains("Three-123", index.Maps.First());
            }
        }
    }
}
