// -----------------------------------------------------------------------
//  <copyright file="Aggregation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Linq;
using SlowTests.Core.Utils.Entities.Faceted;
using Xunit;

namespace SlowTests.Tests.Faceted
{
    public class Aggregation : RavenNewTestBase
    {
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

                Sort(x => x.Total, SortOptions.NumericDouble);
                Sort(x => x.Quantity, SortOptions.NumericLong);
                Sort(x => x.Region, SortOptions.NumericLong);
                Sort(x => x.Tax, SortOptions.NumericDouble);
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_AnonymousTypes_Double()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_All().Execute(store);

                using (var session = store.OpenSession())
                {

                    var obj = new { Currency = Currency.EUR, Product = "Milk", Total = 1.1, Region = 1 };
                    var obj2 = new { Currency = Currency.EUR, Product = "Milk", Total = 1, Region = 1 };

                    session.Store(obj);
                    session.Store(obj2);
                    session.Advanced.GetMetadataFor(obj)["@collection"] = "Orders";
                    session.Advanced.GetMetadataFor(obj2)["@collection"] = "Orders";

                    session.SaveChanges();
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                       .AggregateBy(x => x.Region)
                         .MaxOn(x => x.Total)
                         .MinOn(x => x.Total)
                       .ToList();

                    var facetResult = r.Results["Region"];
                    Assert.Equal(2, facetResult.Values[0].Hits);
                    Assert.Equal(1, facetResult.Values[0].Min);
                    Assert.Equal(1.1, facetResult.Values[0].Max);
                    Assert.Equal(1, facetResult.Values.Count(x => x.Range == "1"));
                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_AnonymousTypes_Float()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_All().Execute(store);

                using (var session = store.OpenSession())
                {

                    var obj = new { Currency = Currency.EUR, Product = "Milk", Total = 1.1, Region = 1, Tax = 1 };
                    var obj2 = new { Currency = Currency.EUR, Product = "Milk", Total = 1, Region = 1, Tax = 1.5 };

                    session.Store(obj);
                    session.Store(obj2);
                    session.Advanced.GetMetadataFor(obj)["@collection"] = "Orders";
                    session.Advanced.GetMetadataFor(obj2)["@collection"] = "Orders";

                    session.SaveChanges();
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                       .AggregateBy(x => x.Region)
                         .MaxOn(x => x.Tax)
                         .MinOn(x => x.Tax)
                       .ToList();

                    var facetResult = r.Results["Region"];
                    Assert.Equal(2, facetResult.Values[0].Hits);
                    Assert.Equal(1, facetResult.Values[0].Min);
                    Assert.Equal(1.5, facetResult.Values[0].Max);

                    Assert.Equal(1, facetResult.Values.Count(x => x.Range == "1"));
                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_AnonymousTypes_Int()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_All().Execute(store);

                using (var session = store.OpenSession())
                {

                    var obj = new { Currency = Currency.EUR, Product = "Milk", Quantity = 1.0, Total = 1.1, Region = 1, Tax = 1 };
                    var obj2 = new { Currency = Currency.EUR, Product = "Milk", Quantity = 2, Total = 1, Region = 1, Tax = 1.5 };

                    session.Store(obj);
                    session.Store(obj2);
                    session.Advanced.GetMetadataFor(obj)["@collection"] = "Orders";
                    session.Advanced.GetMetadataFor(obj2)["@collection"] = "Orders";

                    session.SaveChanges();
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                       .AggregateBy(x => x.Region)
                         .MaxOn(x => x.Quantity)
                         .MinOn(x => x.Quantity)
                       .ToList();

                    var facetResult = r.Results["Region"];
                    Assert.Equal(2, facetResult.Values[0].Hits);
                    Assert.Equal(1, facetResult.Values[0].Min);
                    Assert.Equal(2, facetResult.Values[0].Max);

                    Assert.Equal(1, facetResult.Values.Count(x => x.Range == "1"));
                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_AnonymousTypes_Long()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_All().Execute(store);

                using (var session = store.OpenSession())
                {

                    var obj = new { Currency = Currency.EUR, Product = "Milk", Total = 1.1, Region = 1.0, Tax = 1 };
                    var obj2 = new { Currency = Currency.EUR, Product = "Milk", Total = 1, Region = 2, Tax = 1.5 };

                    session.Store(obj);
                    session.Store(obj2);
                    session.Advanced.GetMetadataFor(obj)["@collection"] = "Orders";
                    session.Advanced.GetMetadataFor(obj2)["@collection"] = "Orders";

                    session.SaveChanges();
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                       .AggregateBy(x => x.Product)
                         .MaxOn(x => x.Region)
                         .MinOn(x => x.Region)
                       .ToList();

                    var facetResult = r.Results["Product"];
                    Assert.Equal(2, facetResult.Values[0].Hits);
                    Assert.Equal(1, facetResult.Values[0].Min);
                    Assert.Equal(2, facetResult.Values[0].Max);

                    Assert.Equal(1, facetResult.Values.Count(x => x.Range == "milk"));
                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate()
        {
            using (var store = GetDocumentStore())
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

                    Assert.Equal(12, facetResult.Values.First(x => x.Range == "milk").Sum);
                    Assert.Equal(3333, facetResult.Values.First(x => x.Range == "iphone").Sum);

                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_MultipleItems()
        {
            using (var store = GetDocumentStore())
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

                    Assert.Equal(12, facetResult.Values.First(x => x.Range == "milk").Sum);
                    Assert.Equal(3333, facetResult.Values.First(x => x.Range == "iphone").Sum);

                    facetResult = r.Results["Currency"];
                    Assert.Equal(2, facetResult.Values.Count);

                    Assert.Equal(3336, facetResult.Values.First(x => x.Range == "eur").Sum);
                    Assert.Equal(9, facetResult.Values.First(x => x.Range == "nis").Sum);


                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_MultipleAggregations()
        {
            using (var store = GetDocumentStore())
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
                         .MinOn(x => x.Total)
                       .ToList();

                    var facetResult = r.Results["Product"];
                    Assert.Equal(2, facetResult.Values.Count);

                    Assert.Equal(9, facetResult.Values.First(x => x.Range == "milk").Max);
                    Assert.Equal(3, facetResult.Values.First(x => x.Range == "milk").Min);

                    Assert.Equal(3333, facetResult.Values.First(x => x.Range == "iphone").Max);
                    Assert.Equal(3333, facetResult.Values.First(x => x.Range == "iphone").Min);

                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_LongDataType()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_All().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Currency = Currency.EUR, Product = "Milk", Total = 3, Region = 1 });
                    session.Store(new Order { Currency = Currency.NIS, Product = "Milk", Total = 9, Region = 1 });
                    session.Store(new Order { Currency = Currency.EUR, Product = "iPhone", Total = 3333, Region = 2 });
                    session.SaveChanges();
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                       .AggregateBy(x => x.Region)
                         .MaxOn(x => x.Total)
                         .MinOn(x => x.Total)
                       .ToList();

                    var facetResult = r.Results["Region"];
                    Assert.Equal(2, facetResult.Values.Count);

                    Assert.Equal(1, facetResult.Values.Count(x => x.Range == "1"));
                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_DateTimeDataType()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_All().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Currency = Currency.EUR, Product = "Milk", Total = 3, Region = 1, At = DateTime.Today });
                    session.Store(new Order { Currency = Currency.NIS, Product = "Milk", Total = 9, Region = 1, At = DateTime.Today.AddDays(-1) });
                    session.Store(new Order { Currency = Currency.EUR, Product = "iPhone", Total = 3333, Region = 2, At = DateTime.Today });
                    session.SaveChanges();
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                       .AggregateBy(x => x.At)
                         .MaxOn(x => x.Total)
                         .MinOn(x => x.Total)
                       .ToList();

                    var facetResult = r.Results["At"];
                    Assert.Equal(2, facetResult.Values.Count);

                    Assert.Equal(1, facetResult.Values.Count(x => x.Range == DateTime.Today.ToString(Raven.Abstractions.Default.DateTimeFormatsToWrite)));
                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_DisplayName()
        {
            using (var store = GetDocumentStore())
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
                       .AggregateBy(x => x.Product, "ProductMax")
                         .MaxOn(x => x.Total)
                       .AndAggregateOn(x => x.Product, "ProductMin")
                         .CountOn(x => x.Currency)
                       .ToList();

                    Assert.Equal(2, r.Results.Count);

                    Assert.NotNull(r.Results["ProductMax"]);
                    Assert.NotNull(r.Results["ProductMin"]);

                    Assert.Equal(3333, r.Results["ProductMax"].Values.First().Max);
                    Assert.Equal(2, r.Results["ProductMin"].Values[1].Count);

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

                    Assert.Equal(12, facetResult.Values.First(x => x.Range == "milk").Sum);
                    Assert.Equal(3333, facetResult.Values.First(x => x.Range == "iphone").Sum);

                    facetResult = r.Results["Total"];
                    Assert.Equal(4, facetResult.Values.Count);

                    Assert.Equal(12, facetResult.Values.First(x => x.Range == "[NULL TO Dx100]").Sum);
                    Assert.Equal(3333, facetResult.Values.First(x => x.Range == "{Dx1500 TO NULL]").Sum);


                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_DateTimeDataType_WithRangeCounts()
        {
            using (var store = GetDocumentStore())
            {
                new ItemsOrders_All().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new ItemsOrder { Items = new List<string> { "first", "second" }, At = DateTime.Today });
                    session.Store(new ItemsOrder { Items = new List<string> { "first", "second" }, At = DateTime.Today.AddDays(-1) });
                    session.Store(new ItemsOrder { Items = new List<string> { "first", "second" }, At = DateTime.Today });
                    session.Store(new ItemsOrder { Items = new List<string> { "first" }, At = DateTime.Today });
                    session.SaveChanges();
                }

                var minValue = DateTime.MinValue;
                var end0 = DateTime.Today.AddDays(-2);
                var end1 = DateTime.Today.AddDays(-1);
                var end2 = DateTime.Today;

                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<ItemsOrder>("ItemsOrders/All")
                        .Where(x => x.At >= end0)
                       .AggregateBy(x => x.At)
                            .AddRanges(
                                x => x.At >= minValue, // all - 4
                                x => x.At >= end0 && x.At < end1, // 1
                                x => x.At >= end1 && x.At < end2 // 3
                            )
                         .CountOn(x => x.At)
                       .ToList();

                    var facetResults = r.Results["At"].Values;
                    Assert.Equal(4, facetResults[0].Count);
                    Assert.Equal(1, facetResults[1].Count);
                    Assert.Equal(3, facetResults[2].Count);
                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_DateTimeDataType_WithRangeCounts_AndInOperator_AfterOtherWhere()
        {
            using (var store = GetDocumentStore())
            {
                new ItemsOrders_All().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new ItemsOrder { Items = new List<string> { "first", "second" }, At = DateTime.Today });
                    session.Store(new ItemsOrder { Items = new List<string> { "first", "second" }, At = DateTime.Today.AddDays(-1) });
                    session.Store(new ItemsOrder { Items = new List<string> { "first", "second" }, At = DateTime.Today });
                    session.Store(new ItemsOrder { Items = new List<string> { "first" }, At = DateTime.Today });
                    session.SaveChanges();
                }

                var items = new List<string> { "second" };
                var minValue = DateTime.MinValue;
                var end0 = DateTime.Today.AddDays(-2);
                var end1 = DateTime.Today.AddDays(-1);
                var end2 = DateTime.Today;

                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<ItemsOrder>("ItemsOrders/All")
                        .Where(x => x.At >= end0)
                        .Where(x => x.Items.In(items))
                        .AggregateBy(x => x.At)
                            .AddRanges(
                                x => x.At >= minValue, // all - 3
                                x => x.At >= end0 && x.At < end1, // 1
                                x => x.At >= end1 && x.At < end2 // 2
                            )
                         .CountOn(x => x.At)
                       .ToList();

                    var facetResults = r.Results["At"].Values;
                    Assert.Equal(3, facetResults[0].Count);
                    Assert.Equal(1, facetResults[1].Count);
                    Assert.Equal(2, facetResults[2].Count);
                }
            }
        }

        [Fact]
        public void CanCorrectlyAggregate_DateTimeDataType_WithRangeCounts_AndInOperator_BeforeOtherWhere()
        {
            using (var store = GetDocumentStore())
            {
                new ItemsOrders_All().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new ItemsOrder { Items = new List<string> { "first", "second" }, At = DateTime.Today });
                    session.Store(new ItemsOrder { Items = new List<string> { "first", "second" }, At = DateTime.Today.AddDays(-1) });
                    session.Store(new ItemsOrder { Items = new List<string> { "first", "second" }, At = DateTime.Today });
                    session.Store(new ItemsOrder { Items = new List<string> { "first" }, At = DateTime.Today });
                    session.SaveChanges();
                }

                var items = new List<string> { "second" };
                var minValue = DateTime.MinValue;
                var end0 = DateTime.Today.AddDays(-2);
                var end1 = DateTime.Today.AddDays(-1);
                var end2 = DateTime.Today;

                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<ItemsOrder>("ItemsOrders/All")
                        .Where(x => x.Items.In(items))
                        .Where(x => x.At >= end0)
                        .AggregateBy(x => x.At)
                            .AddRanges(
                                x => x.At >= minValue, // all - 3
                                x => x.At >= end0 && x.At < end1, // 1
                                x => x.At >= end1 && x.At < end2 // 2
                            )
                         .CountOn(x => x.At)
                       .ToList();

                    var facetResults = r.Results["At"].Values;
                    Assert.Equal(3, facetResults[0].Count);
                    Assert.Equal(1, facetResults[1].Count);
                    Assert.Equal(2, facetResults[2].Count);
                }
            }
        }

        private class ItemsOrder
        {
            public List<string> Items { get; set; }
            public DateTime At { get; set; }
        }

        private class ItemsOrders_All : AbstractIndexCreationTask<ItemsOrder>
        {
            public ItemsOrders_All()
            {
                Map = orders =>
                      from order in orders
                      select new { order.At, order.Items };
            }
        }
    }
}
