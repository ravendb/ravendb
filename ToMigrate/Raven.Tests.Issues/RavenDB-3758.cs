using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto.Faceted;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3758 : RavenTest
    {
        public class Orders_All : AbstractIndexCreationTask<Order>
        {
            public override string IndexName
            {
                get { return "Orders/All"; }
            }

            public Orders_All()
            {
                Map = orders =>
                      from order in orders
                      select new { order.Currency, order.Product, order.Total, order.Quantity, order.Region };

                Sort(x => x.Total, SortOptions.Double);
                Sort(x => x.Quantity, SortOptions.Int);
                Sort(x => x.Region, SortOptions.Long);
            }
        }

        public class Orders_All_Changed : AbstractIndexCreationTask<Order>
        {
            public override string IndexName
            {
                get { return "Orders/All"; }
            }

            public Orders_All_Changed()
            {
                Map = orders =>
                      from order in orders
                      select new { order.Currency, order.Product, order.Total, order.Quantity, order.Region, order.At };

                Sort(x => x.Total, SortOptions.Double);
                Sort(x => x.Quantity, SortOptions.Int);
                Sort(x => x.Region, SortOptions.Long);
            }
        }

        public class Orders_All_Changed2 : AbstractIndexCreationTask<Order>
        {
            public override string IndexName
            {
                get { return "Orders/All"; }
            }

            public Orders_All_Changed2()
            {
                Map = orders =>
                      from order in orders
                      select new { order.Currency, order.Product, order.Total, order.Quantity, order.Region, order.At, order.Tax };

                Sort(x => x.Total, SortOptions.Double);
                Sort(x => x.Quantity, SortOptions.Int);
                Sort(x => x.Region, SortOptions.Long);
                Sort(x => x.Tax, SortOptions.Float);
            }
        }

        [Fact]
        public void Can_Overwrite_Side_By_Side_Index()
        {
            using (var store = NewDocumentStore())
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
                WaitForIndexing(store);

                new Orders_All_Changed().SideBySideExecute(store, replaceTimeUtc: DateTime.Now.AddDays(3));
                new Orders_All_Changed2().SideBySideExecute(store);
            }
        }

        [Fact]
        public async Task Can_Overwrite_Side_By_Side_Index_Async()
        {
            using (var store = NewDocumentStore())
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
                WaitForIndexing(store);

                await new Orders_All_Changed().SideBySideExecuteAsync(store, replaceTimeUtc: DateTime.Now.AddDays(3)).ConfigureAwait(false);
                await new Orders_All_Changed2().SideBySideExecuteAsync(store).ConfigureAwait(false);
            }
        }
    }
}
