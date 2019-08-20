using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using SlowTests.Core.Utils.Entities.Faceted;
using Xunit;

namespace SlowTests.Tests.Faceted
{
    public class RavenDB_12748 : RavenTestBase
    {
        [Fact]
        public void CanCorrectlyAggregate()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_All().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Currency = Currency.EUR, Product = "Milk", Total = 3, Quantity = 3 });
                    session.Store(new Order { Currency = Currency.NIS, Product = "Milk", Total = 9, Quantity = 5 });
                    session.Store(new Order { Currency = Currency.EUR, Product = "iPhone", Total = 3333, Quantity = 7777 });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order, Orders_All>()
                        .AggregateBy(f => f.ByField(x => x.Region))
                        .AndAggregateBy(f => f.ByField(x => x.Product).SumOn(x => x.Total).AverageOn(x => x.Total).SumOn(x => x.Quantity))
                        .Execute();

                    var facetResult = r["Region"];
                    Assert.Equal(1, facetResult.Values.Count);
                    Assert.Null(facetResult.Values.First().Name);
                    Assert.Equal(3, facetResult.Values.First().Count);

                    facetResult = r["Product"];
                    var totalValues = facetResult.Values.Where(x => x.Name == "Total").ToList();

                    Assert.Equal(2, totalValues.Count);

                    var milkValue = totalValues.First(x => x.Range == "milk");
                    var iphoneValue = totalValues.First(x => x.Range == "iphone");

                    Assert.Equal(2, milkValue.Count);
                    Assert.Equal(1, iphoneValue.Count);
                    Assert.Equal(12, milkValue.Sum);
                    Assert.Equal(3333, iphoneValue.Sum);
                    Assert.Equal(6, milkValue.Average);
                    Assert.Equal(3333, iphoneValue.Average);
                    Assert.Null(milkValue.Max);
                    Assert.Null(iphoneValue.Max);
                    Assert.Null(milkValue.Min);
                    Assert.Null(iphoneValue.Min);

                    var quantityValues = facetResult.Values.Where(x => x.Name == "Quantity").ToList();

                    milkValue = quantityValues.First(x => x.Range == "milk");
                    iphoneValue = quantityValues.First(x => x.Range == "iphone");

                    Assert.Equal(2, milkValue.Count);
                    Assert.Equal(1, iphoneValue.Count);
                    Assert.Equal(8, milkValue.Sum);
                    Assert.Equal(7777, iphoneValue.Sum);
                    Assert.Null(milkValue.Average);
                    Assert.Null(iphoneValue.Average);
                    Assert.Null(milkValue.Max);
                    Assert.Null(iphoneValue.Max);
                    Assert.Null(milkValue.Min);
                    Assert.Null(iphoneValue.Min);
                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_Ranges()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_All().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Currency = Currency.EUR, Product = "Milk", Total = 3, Quantity = 3 });
                    session.Store(new Order { Currency = Currency.NIS, Product = "Milk", Total = 9, Quantity = 5 });
                    session.Store(new Order { Currency = Currency.EUR, Product = "iPhone", Total = 3333, Quantity = 7777 });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                        .AggregateBy(f => f
                            .ByRanges(
                                x => x.Total < 100,
                                x => x.Total >= 100 && x.Total < 500,
                                x => x.Total >= 500 && x.Total < 1500,
                                x => x.Total >= 1500)
                            .SumOn(x => x.Total).AverageOn(x => x.Total).SumOn(x => x.Quantity))
                        .Execute();

                    var facetResult = r["Total"];
                    Assert.Equal(8, facetResult.Values.Count);

                    var range1 = facetResult.Values.Single(x => x.Range == "Total < 100.0" && x.Name == "Total");
                    var range2 = facetResult.Values.Single(x => x.Range == "Total >= 1500.0" && x.Name == "Total");

                    Assert.Equal(2, range1.Count);
                    Assert.Equal(1, range2.Count);
                    Assert.Equal(12, range1.Sum);
                    Assert.Equal(3333, range2.Sum);
                    Assert.Equal(6, range1.Average);
                    Assert.Equal(3333, range2.Average);
                    Assert.Null(range1.Max);
                    Assert.Null(range2.Max);
                    Assert.Null(range1.Min);
                    Assert.Null(range2.Min);

                    range1 = facetResult.Values.Single(x => x.Range == "Total < 100.0" && x.Name == "Quantity");
                    range2 = facetResult.Values.Single(x => x.Range == "Total >= 1500.0" && x.Name == "Quantity");

                    Assert.Equal(2, range1.Count);
                    Assert.Equal(1, range2.Count);
                    Assert.Equal(8, range1.Sum);
                    Assert.Equal(7777, range2.Sum);
                    Assert.Null(range1.Average);
                    Assert.Null(range2.Average);
                    Assert.Null(range1.Max);
                    Assert.Null(range2.Max);
                    Assert.Null(range1.Min);
                    Assert.Null(range2.Min);
                }
            }
        }

        private class Orders_All : AbstractIndexCreationTask<Order>
        {
            public Orders_All()
            {
                Map = orders =>
                    from order in orders
                    select new
                    {
                        order.Currency,
                        order.Product,
                        order.Total,
                        order.Quantity,
                        order.Region,
                        order.At,
                        order.Tax
                    };
            }
        }
    }
}
