using Tests.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Core.Indexing
{
    public class OutputReduceToCollectionClusterTests : ClusterTestBase
    {
        public OutputReduceToCollectionClusterTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanUpdatePatternReferencesCollectionNameWithoutConflicts()
        {
            var cluster = await CreateRaftCluster(numberOfNodes: 3, watcherCluster: true, shouldRunInMemory: false);
            using (var store = GetDocumentStore(new Options {Server = cluster.Leader, ReplicationFactor = 3}))
            {
                using (var session = store.OpenSession())
                {
                    PutOrders(session);

                    session.SaveChanges();
                }

                var indexToCreate = new Orders_ProfitByProductAndOrderedAt(referencesCollectionName: "Foo");
                await indexToCreate.ExecuteAsync(store);

                WaitForIndexingInTheCluster(store);

                indexToCreate = new Orders_ProfitByProductAndOrderedAt(referencesCollectionName: "Bar");
                await indexToCreate.ExecuteAsync(store);

                WaitForIndexingInTheCluster(store);

                indexToCreate = new Orders_ProfitByProductAndOrderedAt(referencesCollectionName: "Baz");
                await indexToCreate.ExecuteAsync(store);

                WaitForIndexingInTheCluster(store);

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(0, stats.CountOfRevisionDocuments);
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
    }
}
