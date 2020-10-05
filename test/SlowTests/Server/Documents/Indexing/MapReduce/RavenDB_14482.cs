using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing.MapReduce
{
    public class RavenDB_14482 : RavenTestBase
    {
        public RavenDB_14482(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldErrorOnInvalidReferenceDocumentId()
        {
            using (var store = GetDocumentStore())
            {
                Order entity = new Order()
                {
                    OrderedAt = null,
                    Lines = new List<OrderLine>() { new OrderLine() { Product = "products/1", } }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(entity, "orders/1");

                    session.SaveChanges();
                }

                var index1 = new Orders_ProfitByProductAndOrderedAt();
                index1.Execute(store);

                WaitForIndexing(store, allowErrors: true);
                var errors = WaitForIndexingErrors(store, indexNames: new[] { index1.IndexName });

                Assert.Contains("Invalid pattern reference document ID. Field 'OrderedAt' was null", errors[0].Errors[0].Error);

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Query<object>(collectionName: "Profits/References").Count());
                }

                var now = DateTime.Now;

                using (var session = store.OpenSession())
                {
                    entity.OrderedAt = now;

                    session.Store(entity, "orders/1");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.Equal(1, session.Query<object>(collectionName: "Profits/References").Count());
                }

                var index2 = new Orders_ProfitByProductAndOrderedAtEndsWithPipe();
                index2.Execute(store);

                WaitForIndexing(store, allowErrors: true);
                errors = WaitForIndexingErrors(store, indexNames: new[] { index2.IndexName });

                Assert.Contains($"Invalid pattern reference document ID: 'reports/daily/{now:yyyy-MM-dd}|'. Error: reference ID must not end with '|' character", errors[0].Errors[0].Error);

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Query<object>(collectionName: "Profits2/References").Count());
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

        private class Orders_ProfitByProductAndOrderedAtEndsWithPipe : AbstractIndexCreationTask<Order, Orders_ProfitByProductAndOrderedAt.Result>
        {
            public class Result
            {
                public DateTime OrderedAt { get; set; }
                public string Product { get; set; }
                public decimal Profit { get; set; }
            }

            public Orders_ProfitByProductAndOrderedAtEndsWithPipe()
            {
                Map = orders => from order in orders
                                from line in order.Lines
                                select new { line.Product, order.OrderedAt, Profit = line.Quantity * line.PricePerUnit * (1 - line.Discount) };

                Reduce = results => from r in results
                                    group r by new { r.OrderedAt, r.Product }
                    into g
                                    select new { g.Key.Product, g.Key.OrderedAt, Profit = g.Sum(r => r.Profit) };

                OutputReduceToCollection = "Profits2";

                PatternForOutputReduceToCollectionReferences = x => $"reports/daily/{x.OrderedAt:yyyy-MM-dd}|";
            }
        }

        private class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
            public string Employee { get; set; }
            public DateTime? OrderedAt { get; set; }
            public List<OrderLine> Lines { get; set; }
        }

        private class OrderLine
        {
            public string Product { get; set; }
            public string ProductName { get; set; }
            public decimal PricePerUnit { get; set; }
            public int Quantity { get; set; }
            public decimal Discount { get; set; }
        }
    }
}
