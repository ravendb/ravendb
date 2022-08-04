using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.MapReduce;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing.MapReduce
{
    public class RavenDB_14451 : RavenTestBase
    {
        public RavenDB_14451(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldUpdateReduceOutputReferenceDocumentInsteadOfOverwritingIt()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(
                        new Order()
                        {
                            OrderedAt = new DateTime(2019, 10, 26),
                            Lines = new List<OrderLine>() { new OrderLine() { Product = "products/1", }, new OrderLine() { Product = "products/2", } }
                        }, "orders/1");

                    session.SaveChanges();
                }

                new Orders_ProfitByProductAndOrderedAt().Execute(store);

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    // 2019-10-26
                    var doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(2, doc.ReduceOutputs.Count);

                    Orders_ProfitByProductAndOrderedAt.Result output;

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.Store(
                        new Order()
                        {
                            OrderedAt = new DateTime(2019, 10, 26),
                            Lines = new List<OrderLine>() { new OrderLine() { Product = "products/3", }, new OrderLine() { Product = "products/4", } }
                        }, "orders/2");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    // 2019-10-26
                    var doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(4, doc.ReduceOutputs.Count);

                    Orders_ProfitByProductAndOrderedAt.Result output;

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");

                    order.Lines.RemoveAt(0);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    // 2019-10-26
                    var doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(3, doc.ReduceOutputs.Count);

                    Orders_ProfitByProductAndOrderedAt.Result output;

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/2");

                    order.Lines.RemoveAt(0);

                    session.Store(
                        new Order()
                        {
                            OrderedAt = new DateTime(2019, 10, 26),
                            Lines = new List<OrderLine>() { new OrderLine() { Product = "products/5", }, new OrderLine() { Product = "products/6", } }
                        }, "orders/3");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    // 2019-10-26
                    var doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(4, doc.ReduceOutputs.Count);

                    Orders_ProfitByProductAndOrderedAt.Result output;

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/3");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    // 2019-10-26
                    var doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(2, doc.ReduceOutputs.Count);

                    Orders_ProfitByProductAndOrderedAt.Result output;

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }
                }

                // delete everything

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/1");
                    session.Delete("orders/2");

                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    Assert.Null(session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-26"));
                }

                // add same items again

                using (var session = store.OpenSession())
                {
                    session.Store(
                        new Order()
                        {
                            OrderedAt = new DateTime(2019, 10, 26),
                            Lines = new List<OrderLine>() { new OrderLine() { Product = "products/1", }, new OrderLine() { Product = "products/2", } }
                        }, "orders/1");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    // 2019-10-26
                    var doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(2, doc.ReduceOutputs.Count);

                    Orders_ProfitByProductAndOrderedAt.Result output;

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }
                }
            }
        }

        [Fact]
        public void ShouldEnsureThatIdsAreUniqueAfterIndexReset()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(
                        new Order()
                        {
                            OrderedAt = new DateTime(2019, 10, 26),
                            Lines = new List<OrderLine>() { new OrderLine() { Product = "products/1", }, new OrderLine() { Product = "products/2", } }
                        }, "orders/1");

                    session.SaveChanges();
                }

                var index = new Orders_ProfitByProductAndOrderedAt();
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                store.Maintenance.Send(new ResetIndexOperation(index.IndexName));

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    // 2019-10-26
                    var doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(2, doc.ReduceOutputs.Count);

                    Orders_ProfitByProductAndOrderedAt.Result output;

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }

                    Assert.Equal(new HashSet<string>(doc.ReduceOutputs).Count, doc.ReduceOutputs.Count);
                }
            }
        }

        private class Orders_ProfitByProductAndOrderedAt : AbstractIndexCreationTask<Order, Orders_ProfitByProductAndOrderedAt.Result>
        {
            public class Result
            {
                public DateTime OrderedAt { get; set; }
                public string Product { get; set; }
                public decimal Profit { get; set; }
            }

            public Orders_ProfitByProductAndOrderedAt()
            {
                Map = orders => from order in orders
                    from line in order.Lines
                    select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                Reduce = results => from r in results
                    group r by new { r.OrderedAt, r.Product }
                    into g
                    select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                OutputReduceToCollection = "Profits";

                PatternForOutputReduceToCollectionReferences = x => $"reports/daily/{x.OrderedAt:yyyy-MM-dd}";
            }
        }
    }
}
