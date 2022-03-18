// -----------------------------------------------------------------------
//  <copyright file="Aggregation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using SlowTests.Core.Utils.Entities.Faceted;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Faceted
{
    public class Aggregation : RavenTestBase
    {
        public Aggregation(ITestOutputHelper output) : base(output)
        {
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
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                       .AggregateBy(factory => factory.ByField(x => x.Region).MaxOn(x => x.Total).MinOn(x => x.Total))
                       .Execute();

                    var facetResult = r["Region"];
                    Assert.Equal(2, facetResult.Values[0].Count);
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
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                        .AggregateBy(factory => factory.ByField(x => x.Region).MaxOn(x => x.Tax).MinOn(x => x.Tax))
                        .Execute();

                    var facetResult = r["Region"];
                    Assert.Equal(2, facetResult.Values[0].Count);
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
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                        .AggregateBy(factory => factory.ByField(x => x.Region).MaxOn(x => x.Quantity).MinOn(x => x.Quantity))
                        .Execute();

                    var facetResult = r["Region"];
                    Assert.Equal(2, facetResult.Values[0].Count);
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
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                       .AggregateBy(factory => factory.ByField(x => x.Product).MaxOn(x => x.Region).MinOn(x => x.Region))
                       .Execute();

                    var facetResult = r["Product"];
                    Assert.Equal(2, facetResult.Values[0].Count);
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
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order, Orders_All>()
                           .AggregateBy(f => f.ByField(x => x.Product).SumOn(x => x.Total))
                           .Execute();

                    var facetResult = r["Product"];
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
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                       .AggregateBy(x => x.ByField(y => y.Product).SumOn(y => y.Total))
                       .AndAggregateBy(x => x.ByField(order => order.Currency).SumOn(y => y.Total))
                       .Execute();

                    var facetResult = r["Product"];
                    Assert.Equal(2, facetResult.Values.Count);

                    Assert.Equal(12, facetResult.Values.First(x => x.Range == "milk").Sum);
                    Assert.Equal(3333, facetResult.Values.First(x => x.Range == "iphone").Sum);

                    facetResult = r["Currency"];
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
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                       .AggregateBy(factory => factory.ByField(x => x.Product).MaxOn(x => x.Total).MinOn(x => x.Total))
                       .Execute();

                    var facetResult = r["Product"];
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
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                        .AggregateBy(factory => factory.ByField(x => x.Region).MaxOn(x => x.Total).MinOn(x => x.Total))
                       .Execute();

                    var facetResult = r["Region"];
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
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                        .AggregateBy(factory => factory.ByField(x => x.At).MaxOn(x => x.Total).MinOn(x => x.Total))
                       .Execute();

                    var facetResult = r["At"];
                    Assert.Equal(2, facetResult.Values.Count);

                    Assert.Equal(1, facetResult.Values.Count(x => x.Range == DateTime.Today.ToString(DefaultFormat.DateTimeFormatsToWrite)));
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
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                       .AggregateBy(f => f.ByField(x => x.Product).WithDisplayName("ProductMax").MaxOn(x => x.Total))
                       .AndAggregateBy(f => f.ByField(x => x.Product).WithDisplayName("ProductMin"))
                       .Execute();

                    Assert.Equal(2, r.Count);

                    Assert.NotNull(r["ProductMax"]);
                    Assert.NotNull(r["ProductMin"]);

                    Assert.Equal(3333, r["ProductMax"].Values.First().Max);
                    Assert.Equal(2, r["ProductMin"].Values[1].Count);

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
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<Order>("Orders/All")
                        .AggregateBy(f => f.ByField(x => x.Product).SumOn(x => x.Total))
                        .AndAggregateBy(f => f
                            .ByRanges(
                                x => x.Total < 100,
                                x => x.Total >= 100 && x.Total < 500,
                                x => x.Total >= 500 && x.Total < 1500,
                                x => x.Total >= 1500)
                            .SumOn(x => x.Total))
                        .Execute();

                    var facetResult = r["Product"];
                    Assert.Equal(2, facetResult.Values.Count);

                    Assert.Equal(12, facetResult.Values.First(x => x.Range == "milk").Sum);
                    Assert.Equal(3333, facetResult.Values.First(x => x.Range == "iphone").Sum);

                    facetResult = r["Total"];
                    Assert.Equal(4, facetResult.Values.Count);

                    Assert.Equal(12, facetResult.Values.First(x => x.Range == "Total < 100.0").Sum);
                    Assert.Equal(3333, facetResult.Values.First(x => x.Range == "Total >= 1500.0").Sum);
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

                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<ItemsOrder>("ItemsOrders/All")
                        .Where(x => x.At >= end0)
                       .AggregateBy(f => f.ByRanges(
                            x => x.At >= minValue, // all - 4
                            x => x.At >= end0 && x.At < end1, // 0
                            x => x.At >= end1 && x.At < end2 // 1
                            ))
                       .Execute();

                    var facetResults = r["At"].Values;
                    Assert.Equal(4, facetResults[0].Count);
                    Assert.Equal(0, facetResults[1].Count);
                    Assert.Equal(1, facetResults[2].Count);
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

                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<ItemsOrder>("ItemsOrders/All")
                        .Where(x => x.At >= end0)
                        .Where(x => x.Items.In(items))
                        .AggregateBy(f => f.ByRanges(
                            x => x.At >= minValue, // all - 3
                            x => x.At >= end0 && x.At < end1, // 0
                            x => x.At >= end1 && x.At < end2 // 1
                            ))
                       .Execute();

                    var facetResults = r["At"].Values;
                    Assert.Equal(3, facetResults[0].Count);
                    Assert.Equal(0, facetResults[1].Count);
                    Assert.Equal(1, facetResults[2].Count);
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

                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var r = session.Query<ItemsOrder>("ItemsOrders/All")
                        .Where(x => x.Items.In(items))
                        .Where(x => x.At >= end0)
                        .AggregateBy(
                            f => f
                                .ByRanges(
                                    x => x.At >= minValue, // all - 3
                                    x => x.At >= end0 && x.At < end1, // 0
                                    x => x.At >= end1 && x.At < end2 // 1
                                ))
                        .Execute();

                    var facetResults = r["At"].Values;
                    Assert.Equal(3, facetResults[0].Count);
                    Assert.Equal(0, facetResults[1].Count);
                    Assert.Equal(1, facetResults[2].Count);
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
