using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing.MapReduce
{
    public class RavenDB_14521 : RavenTestBase
    {
        public RavenDB_14521(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanProvideMultipleStringInterpolations()
        {
            using (var store = GetDocumentStore())
            {
                var index = new IndexWithMultipleStringInterpolationsInPattern();

                Assert.Equal("reports/daily/{OrderedAt:MM/dd/yyyy}/{Product}/{Profit:C}/{Count}", index.CreateIndexDefinition().PatternForOutputReduceToCollectionReferences);

                index.Execute(store);
            }
        }

        [Fact]
        public void ShouldThrowOnUsingNonUniqueFields()
        {
            using (var store = GetDocumentStore())
            {
                var index = new IndexWithMultipleStringInterpolationsInPatternNonUniqueFields();

                var ex = Assert.Throws<IndexInvalidException>(() => index.Execute(store));

                Assert.Contains("Pattern should contain unique fields only. Duplicated field: 'OrderedAt'", ex.Message);
            }
        }

        public class IndexWithMultipleStringInterpolationsInPattern : AbstractIndexCreationTask<Order, IndexWithMultipleStringInterpolationsInPattern.Result>
        {
            public class Result
            {
                public DateTime OrderedAt { get; set; }
                public string Product { get; set; }
                public decimal Profit { get; set; }
                public int Count { get; set; }
            }

            public IndexWithMultipleStringInterpolationsInPattern(string outputReduceToCollection = null)
            {
                Map = orders => from order in orders
                    from line in order.Lines
                    select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                Reduce = results => from r in results
                    group r by new { r.OrderedAt, r.Product, r.Profit }
                    into g
                    select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                OutputReduceToCollection = outputReduceToCollection ?? "Profits";

                PatternForOutputReduceToCollectionReferences = x => $"reports/daily/{x.OrderedAt:MM/dd/yyyy}/{x.Product}/{x.Profit:C}/{x.Count}";
            }
        }

        public class IndexWithMultipleStringInterpolationsInPatternNonUniqueFields : AbstractIndexCreationTask<Order, IndexWithMultipleStringInterpolationsInPattern.Result>
        {
            public class Result
            {
                public DateTime OrderedAt { get; set; }
                public string Product { get; set; }
                public decimal Profit { get; set; }
            }

            public IndexWithMultipleStringInterpolationsInPatternNonUniqueFields(string outputReduceToCollection = null)
            {
                Map = orders => from order in orders
                    from line in order.Lines
                    select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                Reduce = results => from r in results
                    group r by new { r.OrderedAt, r.Product }
                    into g
                    select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                OutputReduceToCollection = outputReduceToCollection ?? "Profits";

                PatternForOutputReduceToCollectionReferences = x => $"reports/daily/{x.OrderedAt}/{x.Product}/{x.OrderedAt}/{x.Product}";
            }
        }
    }
}
