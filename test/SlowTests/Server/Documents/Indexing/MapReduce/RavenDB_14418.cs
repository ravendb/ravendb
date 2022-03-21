using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.MapReduce;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing.MapReduce
{
    public class RavenDB_14418 : RavenTestBase
    {
        public RavenDB_14418(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanPassCollectionNameToLoadDocument()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_ProfitByProductAndOrderedAt().Execute(store);

                new Profits_Monthly().Execute(store);
                new Profits_Monthly2().Execute(store);

                using (var session = store.OpenSession())
                {
                    PutOrders(session);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Profits_Monthly.Result, Profits_Monthly>().OrderBy(x => x.ProfitValue).ToList();

                    Assert.Equal(2, results.Count);

                    Assert.Equal(1, results[0].ProfitValue);
                    Assert.Equal(2, results[0].Month);
                    Assert.Equal(2020, results[0].Year);

                    Assert.Equal(4, results[1].ProfitValue);
                    Assert.Equal(1, results[1].Month);
                    Assert.Equal(2020, results[1].Year);

                    results = session.Query<Profits_Monthly.Result, Profits_Monthly2>().OrderBy(x => x.ProfitValue).ToList();

                    Assert.Equal(2, results.Count);

                    Assert.Equal(1, results[0].ProfitValue);
                    Assert.Equal(2, results[0].Month);
                    Assert.Equal(2020, results[0].Year);

                    Assert.Equal(4, results[1].ProfitValue);
                    Assert.Equal(1, results[1].Month);
                    Assert.Equal(2020, results[1].Year);
                }
            }
        }

        [Fact]
        public void CanDefineReferencesCollectionName()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_ProfitByProductAndOrderedAt("MyProfitsReferences").Execute(store);

                new Profits_Monthly_Loading_MyProfitsReferences().Execute(store);

                using (var session = store.OpenSession())
                {
                    PutOrders(session);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Profits_Monthly_Loading_MyProfitsReferences.Result, Profits_Monthly_Loading_MyProfitsReferences>().OrderBy(x => x.ProfitValue).ToList();

                    Assert.Equal(2, results.Count);

                    Assert.Equal(1, results[0].ProfitValue);
                    Assert.Equal(2, results[0].Month);
                    Assert.Equal(2020, results[0].Year);

                    Assert.Equal(4, results[1].ProfitValue);
                    Assert.Equal(1, results[1].Month);
                    Assert.Equal(2020, results[1].Year);
                }
            }
        }

        [Fact]
        public void ShouldNotAllowToProvideSameCollectionNameForOutputReduceAndReferences()
        {
            using (var store = GetDocumentStore())
            {
                var ex = Assert.Throws<IndexInvalidException>(() => new Orders_ProfitByProductAndOrderedAt("Profits").Execute(store));

                Assert.Contains("Collection defined in PatternReferencesCollectionName must not be the same as in OutputReduceToCollection. Collection name: 'Profits'", ex.Message);
            }
        }

        [Fact]
        public void CanUpdateReferencesCollectionName()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_ProfitByProductAndOrderedAt("MyProfitsReferences").Execute(store);

                using (var session = store.OpenSession())
                {
                    PutOrders(session);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                new Orders_ProfitByProductAndOrderedAt("MyNewProfitsReferences").Execute(store);

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<dynamic>(collectionName: "MyProfitsReferences").Count();

                    Assert.Equal(0, count);

                    count = session.Query<dynamic>(collectionName: "MyNewProfitsReferences").Count();

                    Assert.Equal(3, count);
                }
            }
        }

        [Fact]
        public void CanChangeOutputCollectionNameIfPatternIsDefined()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_ProfitByProductAndOrderedAt(outputReduceToCollection: "Profits").Execute(store);

                using (var session = store.OpenSession())
                {
                    PutOrders(session);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                new Orders_ProfitByProductAndOrderedAt(outputReduceToCollection: "Profits2").Execute(store);

                Indexes.WaitForIndexing(store);

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<dynamic>(collectionName: "Profits").Count();

                    Assert.Equal(0, count);

                    count = session.Query<dynamic>(collectionName: "Profits2").Count();

                    Assert.Equal(4, count);
                }
            }
        }

        private class Orders_ProfitByProductAndOrderedAt : AbstractIndexCreationTask<Order, Orders_ProfitByProductAndOrderedAt.Result>
        {
            public class Result
            {
                public DateTime OrderedAt { get; set; }
                public string Product { get; set; }
                public decimal ProfitValue { get; set; }
            }

            public Orders_ProfitByProductAndOrderedAt(string referencesCollectionName = null, string outputReduceToCollection = null)
            {
                Map = orders => from order in orders
                    from line in order.Lines
                    select new { line.Product, order.OrderedAt, ProfitValue = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                Reduce = results => from r in results
                    group r by new { r.OrderedAt, r.Product }
                    into g
                    select new { g.Key.Product, g.Key.OrderedAt, ProfitValue = g.Sum(r => r.ProfitValue) };

                OutputReduceToCollection = outputReduceToCollection ?? "Profits";

                PatternForOutputReduceToCollectionReferences = x => $"reports/daily/{x.OrderedAt:yyyy-MM-dd}";

                if (referencesCollectionName != null)
                    PatternReferencesCollectionName = referencesCollectionName;
            }
        }

        private class Profit
        {
            public DateTime OrderedAt { get; set; }
            public string Product { get; set; }
            public decimal ProfitValue { get; set; }
        }

        public class MyProfitsReferences : OutputReduceToCollectionReference
        {

        }

        private class Profits_Monthly : AbstractIndexCreationTask<Order, Profits_Monthly.Result>
        {
            public class Result
            {
                public int Month { get; set; }
                public int Year { get; set; }
                public IEnumerable<string> Products { get; set; }
                public decimal ProfitValue { get; set; }
            }

            public Profits_Monthly()
            {
                Map = orders => from order in orders
                    let reference = LoadDocument<OutputReduceToCollectionReference>($"reports/daily/{order.OrderedAt:yyyy-MM-dd}", "Profits/References")
                    from id in reference.ReduceOutputs
                    let reduceResult = LoadDocument<Profit>(id)
                    select new Result
                    {
                        Products = new List<string>() { reduceResult.Product },
                        Month = reduceResult.OrderedAt.Month,
                        Year = reduceResult.OrderedAt.Year,
                        ProfitValue = reduceResult.ProfitValue
                    };

                Reduce = results => from r in results
                    group r by new { r.Month, r.Year }
                    into g
                    select new Result { Month = g.Key.Month, Year = g.Key.Year, ProfitValue = g.Sum(r => r.ProfitValue), Products = g.SelectMany(x => x.Products).Distinct()};
            }
        }

        private class Profits_Monthly2 : AbstractIndexCreationTask<Order, Profits_Monthly2.Result>
        {
            public class Result
            {
                public int Month { get; set; }
                public int Year { get; set; }
                public IEnumerable<string> Products { get; set; }
                public decimal ProfitValue { get; set; }
            }

            public Profits_Monthly2()
            {
                Map = orders => from order in orders
                    let reference = LoadDocument<OutputReduceToCollectionReference>($"reports/daily/{order.OrderedAt:yyyy-MM-dd}", "Profits/References")
                    from reduceResult in LoadDocument<Profit>(reference.ReduceOutputs)
                    select new Result
                    {
                        Products = new List<string>() { reduceResult.Product },
                        Month = reduceResult.OrderedAt.Month,
                        Year = reduceResult.OrderedAt.Year,
                        ProfitValue = reduceResult.ProfitValue
                    };

                Reduce = results => from r in results
                    group r by new { r.Month, r.Year }
                    into g
                    select new Result { Month = g.Key.Month, Year = g.Key.Year, ProfitValue = g.Sum(r => r.ProfitValue), Products = g.SelectMany(x => x.Products).Distinct() };
            }
        }

        private class Profits_Monthly_Loading_MyProfitsReferences : AbstractIndexCreationTask<Order, Profits_Monthly.Result>
        {
            public class Result
            {
                public int Month { get; set; }
                public int Year { get; set; }
                public IEnumerable<string> Products { get; set; }
                public decimal ProfitValue { get; set; }
            }

            public Profits_Monthly_Loading_MyProfitsReferences()
            {
                Map = orders => from order in orders
                    let reference = LoadDocument<MyProfitsReferences>($"reports/daily/{order.OrderedAt:yyyy-MM-dd}")
                    from id in reference.ReduceOutputs
                    let reduceResult = LoadDocument<Profit>(id)
                    select new Result
                    {
                        Products = new List<string>() { reduceResult.Product },
                        Month = reduceResult.OrderedAt.Month,
                        Year = reduceResult.OrderedAt.Year,
                        ProfitValue = reduceResult.ProfitValue
                    };

                Reduce = results => from r in results
                    group r by new { r.Month, r.Year }
                    into g
                    select new Result { Month = g.Key.Month, Year = g.Key.Year, ProfitValue = g.Sum(r => r.ProfitValue), Products = g.SelectMany(x => x.Products).Distinct() };
            }
        }

        private static void PutOrders(IDocumentSession session)
        {
            session.Store(
                new Order()
                {
                    OrderedAt = new DateTime(2020, 1, 26),
                    Lines = new List<OrderLine>() { new OrderLine() { Product = "products/1", PricePerUnit =  2, Quantity = 1}, new OrderLine() { Product = "products/2", PricePerUnit = 1, Quantity = 1, } }
                }, "orders/1");

            session.Store(new Order() { OrderedAt = new DateTime(2020, 1, 25), Lines = new List<OrderLine>() { new OrderLine() { Product = "products/2", PricePerUnit = 1, Quantity = 1 } } }, "orders/2");

            session.Store(new Order() { OrderedAt = new DateTime(2020, 2, 22), Lines = new List<OrderLine>() { new OrderLine() { Product = "products/1", PricePerUnit = 1, Quantity = 1 } } }, "orders/3");
        }
    }
}
