// -----------------------------------------------------------------------
//  <copyright file="Aggregation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;
using Raven.Client;

namespace Raven.Tests.Faceted
{
    public class Aggregation : RavenTest
    {
        public class Order
        {
            public string Product { get; set; }
            public decimal Total { get; set; }
            public Currency Currency { get; set; }
        }

        public enum Currency
        {
            USD,
            EUR,
            NIS
        }

        public class Orders_All : AbstractIndexCreationTask<Order>
        {
            public Orders_All()
            {
                Map = orders =>
                      from order in orders
                      select new { order.Currency, order.Product, order.Total };

                Sort(x => x.Total, SortOptions.Double);
            }
        }

        [Fact]
        public void CanCorrectlyAggregate()
        {
            using (var store = NewDocumentStore())
            {
                new Orders_All().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Currency = Currency.EUR, Product = "Milk", Total = 3 });
                    session.Store(new Order { Currency = Currency.NIS, Product = "Milk", Total = 9 });
                    session.Store(new Order { Currency = Currency.EUR, Product = "iPhone", Total = 3333 });
                    session.SaveChanges();
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order, Orders_All>()
                           .AggregateBy(x => x.Product)
                           .SumOn(x => x.Total)
                           .ToList();

                    var facetResult = r.Results["Product"];
                    Assert.Equal(2, facetResult.Values.Count);

                    Assert.Equal(12, facetResult.Values.First(x => x.Range == "milk").Value);
                    Assert.Equal(3333, facetResult.Values.First(x => x.Range == "iphone").Value);

                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_MultipleItems()
        {
            using (var store = NewDocumentStore())
            {
                new Orders_All().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Currency = Currency.EUR, Product = "Milk", Total = 3 });
                    session.Store(new Order { Currency = Currency.NIS, Product = "Milk", Total = 9 });
                    session.Store(new Order { Currency = Currency.EUR, Product = "iPhone", Total = 3333 });
                    session.SaveChanges();
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                       .AggregateBy(order => order.Product)
                          .SumOn(order => order.Total)
                       .AndAggregateOn(order => order.Currency)
                           .SumOn(order => order.Total)
                       .ToList();

                    var facetResult = r.Results["Product"];
                    Assert.Equal(2, facetResult.Values.Count);

                    Assert.Equal(12, facetResult.Values.First(x => x.Range == "milk").Value);
                    Assert.Equal(3333, facetResult.Values.First(x => x.Range == "iphone").Value);

                    facetResult = r.Results["Currency"];
                    Assert.Equal(2, facetResult.Values.Count);

                    Assert.Equal(3336, facetResult.Values.First(x => x.Range == "eur").Value);
                    Assert.Equal(9, facetResult.Values.First(x => x.Range == "nis").Value);


                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_WithAverage()
        {
            using (var store = NewDocumentStore())
            {
                new Orders_All().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Currency = Currency.EUR, Product = "Milk", Total = 3 });
                    session.Store(new Order { Currency = Currency.NIS, Product = "Milk", Total = 9 });
                    session.Store(new Order { Currency = Currency.EUR, Product = "iPhone", Total = 3333 });
                    session.SaveChanges();
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                       .AggregateBy(x => x.Product)
                         .MaxOn(x => x.Total)
                       .AndAggregateOn(x => x.Product)
                         .MinOn(x => x.Total)
                       .AndAggregateOn(x => x.Product)
                         .AverageOn(x => x.Total)
                       .ToList();

                    var facetResult = r.Results["Product"];
                    Assert.Equal(2, facetResult.Values.Count);

                    Assert.Equal(12, facetResult.Values.First(x => x.Range == "milk").Value);
                    Assert.Equal(3333, facetResult.Values.First(x => x.Range == "iphone").Value);

                    facetResult = r.Results["Currency"];
                    Assert.Equal(2, facetResult.Values.Count);

                    Assert.Equal(3336, facetResult.Values.First(x => x.Range == "eur").Value);
                    Assert.Equal(9, facetResult.Values.First(x => x.Range == "nis").Value);


                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_Ranges()
        {
            using (var store = NewDocumentStore())
            {
                new Orders_All().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Currency = Currency.EUR, Product = "Milk", Total = 3 });
                    session.Store(new Order { Currency = Currency.NIS, Product = "Milk", Total = 9 });
                    session.Store(new Order { Currency = Currency.EUR, Product = "iPhone", Total = 3333 });
                    session.SaveChanges();
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                       .AggregateBy(x => x.Product)
                         .SumOn(x => x.Total)
                       .AndAggregateOn(x => x.Total)
                           .AddRanges(x => x.Total < 100,
                                      x => x.Total >= 100 && x.Total < 500,
                                      x => x.Total >= 500 && x.Total < 1500,
                                      x => x.Total >= 1500)
                       .SumOn(x => x.Total)
                       .ToList();

                    var facetResult = r.Results["Product"];
                    Assert.Equal(2, facetResult.Values.Count);

                    Assert.Equal(12, facetResult.Values.First(x => x.Range == "milk").Value);
                    Assert.Equal(3333, facetResult.Values.First(x => x.Range == "iphone").Value);

                    facetResult = r.Results["Total"];
                    Assert.Equal(4, facetResult.Values.Count);

                    Assert.Equal(12, facetResult.Values.First(x => x.Range == "[NULL TO Dx100]").Value);
                    Assert.Equal(3333, facetResult.Values.First(x => x.Range == "{Dx1500 TO NULL]").Value);


                }
            }
        }
    }
}