using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.MapReduce;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.OutputToCollection;
using Raven.Server.Documents.Indexes.Static;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing.MapReduce
{
    public class RavenDB_12932 : RavenTestBase
    {
        public RavenDB_12932(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCreateIndexesWithPattern()
        {
            using (var store = GetDocumentStore())
            {
                AbstractIndexCreationTask<Order, DifferentApproachesToDefinePatternForOutputReduceToCollectionReferences.Result> index;

                index = new DifferentApproachesToDefinePatternForOutputReduceToCollectionReferences.One_A("One_A");
                Assert.Equal("reports/daily/{OrderedAt:yyyy-MM-dd}", index.CreateIndexDefinition().PatternForOutputReduceToCollectionReferences);
                index.Execute(store);

                index = new DifferentApproachesToDefinePatternForOutputReduceToCollectionReferences.One_B("One_B");
                Assert.Equal("{OrderedAt:yyyy-MM-dd}reports/daily/{Profit:C}", index.CreateIndexDefinition().PatternForOutputReduceToCollectionReferences);
                index.Execute(store);
                
                index = new DifferentApproachesToDefinePatternForOutputReduceToCollectionReferences.Two_A("Two_A");
                Assert.Equal("reports/daily/{OrderedAt}", index.CreateIndexDefinition().PatternForOutputReduceToCollectionReferences);
                index.Execute(store);

                index = new DifferentApproachesToDefinePatternForOutputReduceToCollectionReferences.Two_B("Two_B");
                Assert.Equal("reports/daily/{OrderedAt}/product/{Product}", index.CreateIndexDefinition().PatternForOutputReduceToCollectionReferences);
                index.Execute(store);

                index = new DifferentApproachesToDefinePatternForOutputReduceToCollectionReferences.Three_A("Three_A");
                Assert.Equal("reports/daily/{OrderedAt:MM/dd/yyyy}", index.CreateIndexDefinition().PatternForOutputReduceToCollectionReferences);
                index.Execute(store);

                index = new DifferentApproachesToDefinePatternForOutputReduceToCollectionReferences.Three_B("Three_B");
                Assert.Equal("reports/daily/{OrderedAt:MM/dd/yyyy}/{Product}", index.CreateIndexDefinition().PatternForOutputReduceToCollectionReferences);
                index.Execute(store);
            }
        }

        [Fact]
        public void CanDefinePatternForReferenceDocumentsOfReduceOutputs()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    PutOrders(session);

                    session.SaveChanges();
                }

                new Orders_ProfitByProductAndOrderedAt().Execute(store);

                WaitForIndexing(store);

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

                    // 2019-10-24

                    doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-24", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(1, doc.ReduceOutputs.Count);

                    output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(doc.ReduceOutputs[0]);

                    Assert.NotNull(output);

                    // 2019-10-25

                    doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-25", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(1, doc.ReduceOutputs.Count);

                    output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(doc.ReduceOutputs[0]);

                    Assert.NotNull(output);
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/1");
                    session.Delete("orders/2");
                    session.Delete("orders/3");

                    session.SaveChanges();

                    WaitForIndexing(store);

                    Assert.Null(session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-24"));
                    Assert.Null(session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-25"));
                    Assert.Null(session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-26"));
                }
            }
        }

        private static void PutOrders(IDocumentSession session)
        {
            session.Store(
                new Order()
                {
                    OrderedAt = new DateTime(2019, 10, 26),
                    Lines = new List<OrderLine>() {new OrderLine() {Product = "products/1",}, new OrderLine() {Product = "products/2",}}
                }, "orders/1");

            session.Store(new Order() {OrderedAt = new DateTime(2019, 10, 25), Lines = new List<OrderLine>() {new OrderLine() {Product = "products/2",}}}, "orders/2");

            session.Store(new Order() {OrderedAt = new DateTime(2019, 10, 24), Lines = new List<OrderLine>() {new OrderLine() {Product = "products/1",}}}, "orders/3");
        }


        [Fact]
        public void MultipleReduceOutputsIntoSingleReferenceDocument()
        {
            using (var store = GetDocumentStore())
            {
                var numberOfOutputs = 100;

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < numberOfOutputs; i++)
                    {
                        session.Store(new Order
                        {
                            OrderedAt = new DateTime(2019, 10, 26),
                            Lines = new List<OrderLine>()
                            {
                                new OrderLine()
                                {
                                    Product = "products/" + i,
                                }
                            }
                        }, "orders/" + i);
                    }

                    session.SaveChanges();
                }

                new Orders_ProfitByProductAndOrderedAt().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(numberOfOutputs, doc.ReduceOutputs.Count);

                    Orders_ProfitByProductAndOrderedAt.Result output;

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/15");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(numberOfOutputs - 1, doc.ReduceOutputs.Count);

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        Orders_ProfitByProductAndOrderedAt.Result output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/37");
                    session.Delete("orders/83");
                    session.Delete("orders/12");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(numberOfOutputs - 4, doc.ReduceOutputs.Count);

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        Orders_ProfitByProductAndOrderedAt.Result output = session.Load<Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }
                }
            }
        }

        [Fact]
        public async Task CanUpdateIndexWithPatternForOutputReduceToCollectionReferences()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    PutOrders(session);

                    session.SaveChanges();
                }

                new Orders_ProfitByProductAndOrderedAt().Execute(store);

                WaitForIndexing(store);

                await store.ExecuteIndexAsync(new Replacement.Orders_ProfitByProductAndOrderedAt());

                WaitForUserToContinueTheTest(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    // 2019-10-26
                    var doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-26", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(2, doc.ReduceOutputs.Count);

                    Replacement.Orders_ProfitByProductAndOrderedAt.Result output;

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        output = session.Load<Replacement.Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }

                    // 2019-10-24

                    doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-24", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(1, doc.ReduceOutputs.Count);

                    output = session.Load<Replacement.Orders_ProfitByProductAndOrderedAt.Result>(doc.ReduceOutputs[0]);

                    Assert.NotNull(output);

                    // 2019-10-25

                    doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2019-10-25", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(1, doc.ReduceOutputs.Count);

                    output = session.Load<Replacement.Orders_ProfitByProductAndOrderedAt.Result>(doc.ReduceOutputs[0]);

                    Assert.NotNull(output);
                }
            }
        }

        [Fact]
        public async Task CanPersistPatternForOutputReduceToCollectionReferences()
        {
            using (var store = GetDocumentStore())
            {
                var indexToCreate = new Orders_ProfitByProductAndOrderedAt(referencesCollectionName: "CustomCollection");
                indexToCreate.Execute(store);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndexes().First();

                var definition = MapIndexDefinition.Load(index._environment, out var version);
                Assert.NotNull(definition.PatternForOutputReduceToCollectionReferences);
                Assert.Equal(IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion, version);

                Assert.Equal("CustomCollection", definition.PatternReferencesCollectionName);

            }
        }

        [Fact]
        public void ShouldCreateIndexErrorIfPatternFormattingIsNotValid()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order()
                    {
                        OrderedAt = new DateTime(2019, 10, 26),
                        Lines = new List<OrderLine>()
                        {
                            new OrderLine()
                            {
                                Product = "products/1",
                            },
                            new OrderLine()
                            {
                                Product = "products/2",
                            }
                        }
                    }, "orders/1");

                    session.SaveChanges();
                }

                var index = new Index_WithInvalidPropertyName();
                index.Execute(store);

                WaitForIndexing(store);

                var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] {index.IndexName}));

                Assert.Equal(1, errors.Length);

                Assert.Equal(2, errors[0].Errors.Length);
            }
        }

        [Fact]
        public async Task CanUpdatePatternForOutputReduceToCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    PutOrders(session);

                    session.SaveChanges();
                }

                new Orders_ProfitByProductAndOrderedAt().Execute(store);

                WaitForIndexing(store);

                await store.ExecuteIndexAsync(new Replacement_DifferentPattern.Orders_ProfitByProductAndOrderedAt());

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<OutputReduceToCollectionReference>("reports/monthly/2019-10", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(4, doc.ReduceOutputs.Count);

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        Replacement.Orders_ProfitByProductAndOrderedAt.Result output = session.Load<Replacement.Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }

                    int count = session.Advanced.RawQuery<object>("from 'Profits/References'").Count();

                    Assert.Equal(1, count);
                }
            }
        }

        [Fact]
        public async Task CanUpdatePatternFieldInIndexDefinitionSoItWillAffectReferenceDocuments()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    PutOrders(session);

                    session.SaveChanges();
                }

                new Orders_ProfitByProductAndOrderedAt().Execute(store);

                WaitForIndexing(store);

                WaitForUserToContinueTheTest(store);

                await store.ExecuteIndexAsync(new Replacement_PatternFieldUpdate.Orders_ProfitByProductAndOrderedAt());

                WaitForIndexing(store);

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<OutputReduceToCollectionReference>("reports/daily/2020-01-01", x => x.IncludeDocuments(y => y.ReduceOutputs));

                    Assert.Equal(2, doc.ReduceOutputs.Count);

                    foreach (var docReduceOutput in doc.ReduceOutputs)
                    {
                        Replacement.Orders_ProfitByProductAndOrderedAt.Result output = session.Load<Replacement.Orders_ProfitByProductAndOrderedAt.Result>(docReduceOutput);

                        Assert.NotNull(output);
                    }

                    int count = session.Advanced.RawQuery<object>("from 'Profits/References'").Count();

                    Assert.Equal(1, count);
                }
            }
        }

        [Fact]
        public void OutputReferencesPatternTests()
        {
            var sut = new OutputReferencesPattern(null, "reports/daily/{OrderedAt:yyyy-MM-dd}");

            using (sut.BuildReferenceDocumentId(out var builder))
            {
                builder.Add("OrderedAt", new DateTime(2019, 10, 26));

                Assert.Equal("reports/daily/2019-10-26", builder.GetId());
            }

            sut = new OutputReferencesPattern(null, "output/{UserId}/{Date:yyyy-MM-dd}");

            using (sut.BuildReferenceDocumentId(out var builder))
            {
                builder.Add("Date", new DateTime(2019, 10, 26));
                builder.Add("UserId", "arek");

                Assert.Equal("output/arek/2019-10-26", builder.GetId());
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

            public Orders_ProfitByProductAndOrderedAt(string referencesCollectionName = null)
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
                
                if (referencesCollectionName != null)
                    PatternReferencesCollectionName = referencesCollectionName;
            }
        }

        private class Index_WithInvalidPropertyName : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"from order in docs.Orders
                        from line in order.Lines
                        select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };"
                    },
                    Reduce = @"from r in results
                    group r by new { r.OrderedAt, r.Product }
                    into g
                    select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };",
                    OutputReduceToCollection = "Profits",
                    PatternForOutputReduceToCollectionReferences = "reports/daily/{InvalidPropertyName:yyyy-MM-dd}",
                    Name = "Index/WithInvalidPropertyName"
                };
            }
        }

        private static class Replacement
        {
            public class Orders_ProfitByProductAndOrderedAt : AbstractIndexCreationTask<Order, Orders_ProfitByProductAndOrderedAt.Result>
            {
                public class Result
                {
                    public DateTime OrderedAt { get; set; }
                    public string MyProduct { get; set; }
                    public decimal MyProfit { get; set; }
                }

                public Orders_ProfitByProductAndOrderedAt()
                {
                    Map = orders => from order in orders
                        from line in order.Lines
                        select new Result { MyProduct = line.Product, OrderedAt = order.OrderedAt, MyProfit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                    Reduce = results => from r in results
                        group r by new { OrderedAt = r.OrderedAt, Product = r.MyProduct }
                        into g
                        select new Result
                        {
                            MyProduct = g.Key.Product, OrderedAt = g.Key.OrderedAt, MyProfit = g.Sum(r => r.MyProfit)
                        };

                    OutputReduceToCollection = "Profits";

                    PatternForOutputReduceToCollectionReferences = x => string.Format("reports/daily/{0:yyyy-MM-dd}", x.OrderedAt);

                }
            }
        }

        private static class Replacement_DifferentPattern
        {
            public class Orders_ProfitByProductAndOrderedAt : AbstractIndexCreationTask
            {
                public override IndexDefinition CreateIndexDefinition()
                {
                    return new IndexDefinition
                    {
                        Maps =
                        {
                            @"from order in docs.Orders
                        from line in order.Lines
                        select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };"
                        },
                        Reduce = @"from r in results
                    group r by new { r.OrderedAt, r.Product }
                    into g
                    select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };",
                        OutputReduceToCollection = "Profits",
                        PatternForOutputReduceToCollectionReferences = "reports/monthly/{OrderedAt:yyyy-MM}"
                    };
                }
            }
        }

        private static class Replacement_PatternFieldUpdate
        {
            public class Orders_ProfitByProductAndOrderedAt : AbstractIndexCreationTask
            {
                public override IndexDefinition CreateIndexDefinition()
                {
                    return new IndexDefinition
                    {
                        Maps =
                        {
                            @"from order in docs.Orders
                        from line in order.Lines
                        select new {
                            line.Product, 
                            OrderedAt = new DateTime(2020, 1, 1), // fixed date field so it will affect reference documents 
                            Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };
                        "
                        },
                        Reduce = @"from r in results
                    group r by new { r.OrderedAt, r.Product }
                    into g
                    select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };",
                        OutputReduceToCollection = "Profits",
                        PatternForOutputReduceToCollectionReferences = "reports/daily/{OrderedAt:yyyy-MM-dd}"
                    };
                }
            }
        }

        private static class DifferentApproachesToDefinePatternForOutputReduceToCollectionReferences
        {
            public class Result
            {
                public DateTime OrderedAt { get; set; }
                public string Product { get; set; }
                public decimal Profit { get; set; }
            }

            public class One_A : AbstractIndexCreationTask<Order, Result>
            {
                public One_A(string outputReduceToCollection = null)
                {
                    Map = orders => from order in orders
                        from line in order.Lines
                        select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                    Reduce = results => from r in results
                        group r by new { r.OrderedAt, r.Product }
                        into g
                        select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                    OutputReduceToCollection = outputReduceToCollection ?? "Profits";

                    PatternForOutputReduceToCollectionReferences = x => $"reports/daily/{x.OrderedAt:yyyy-MM-dd}";
                }
            }

            public class One_B : AbstractIndexCreationTask<Order, Result>
            {
                public One_B(string outputReduceToCollection = null)
                {
                    Map = orders => from order in orders
                        from line in order.Lines
                        select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                    Reduce = results => from r in results
                        group r by new { r.OrderedAt, r.Product }
                        into g
                        select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                    OutputReduceToCollection = outputReduceToCollection ?? "Profits";

                    PatternForOutputReduceToCollectionReferences = x => $"{x.OrderedAt:yyyy-MM-dd}reports/daily/{x.Profit:C}";
                }
            }

            public class Two_A : AbstractIndexCreationTask<Order, Result>
            {
                public Two_A(string outputReduceToCollection = null)
                {
                    Map = orders => from order in orders
                        from line in order.Lines
                        select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                    Reduce = results => from r in results
                        group r by new { r.OrderedAt, r.Product }
                        into g
                        select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                    OutputReduceToCollection = outputReduceToCollection ?? "Profits";

                    PatternForOutputReduceToCollectionReferences = x => "reports/daily/" + x.OrderedAt;
                }
            }

            public class Two_B : AbstractIndexCreationTask<Order, Result>
            {
                public Two_B(string outputReduceToCollection = null)
                {
                    Map = orders => from order in orders
                        from line in order.Lines
                        select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                    Reduce = results => from r in results
                        group r by new { r.OrderedAt, r.Product }
                        into g
                        select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                    OutputReduceToCollection = outputReduceToCollection ?? "Profits";

                    PatternForOutputReduceToCollectionReferences = x => "reports/daily/" + x.OrderedAt + "/product/" + x.Product;
                }
            }

            public class Three_A : AbstractIndexCreationTask<Order, Result>
            {
                public Three_A(string outputReduceToCollection = null)
                {
                    Map = orders => from order in orders
                        from line in order.Lines
                        select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                    Reduce = results => from r in results
                        group r by new { r.OrderedAt, r.Product }
                        into g
                        select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                    OutputReduceToCollection = outputReduceToCollection ?? "Profits";

                    PatternForOutputReduceToCollectionReferences = x => string.Format("reports/daily/{0:MM/dd/yyyy}", x.OrderedAt);
                }
            }

            public class Three_B : AbstractIndexCreationTask<Order, Result>
            {
                public Three_B(string outputReduceToCollection = null)
                {
                    Map = orders => from order in orders
                        from line in order.Lines
                        select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                    Reduce = results => from r in results
                        group r by new { r.OrderedAt, r.Product }
                        into g
                        select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                    OutputReduceToCollection = outputReduceToCollection ?? "Profits";

                    PatternForOutputReduceToCollectionReferences = x => string.Format("reports/daily/{0:MM/dd/yyyy}/{1}", x.OrderedAt, x.Product);
                }
            }
        }
    }
}
