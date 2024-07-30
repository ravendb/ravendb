using System;
using System.Linq;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
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

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task CanUpdateDatabaseChangeVectorAfterCreatingArtificialDocs()
        {
            var clusterSize = 5;
            var cluster = await CreateRaftCluster(numberOfNodes: clusterSize, watcherCluster: true);
            using (var store = GetDocumentStore(new Options 
                   {
                       Server = cluster.Leader, 
                       ReplicationFactor = clusterSize,
                   }))
            {
                for (int i = 1; i < 100; i+=3)
                {
                    using (var session = store.OpenSession())
                    {
                        PutOrders(session, i);
                        session.Advanced.WaitForReplicationAfterSaveChanges(replicas: clusterSize - 1);
                        session.SaveChanges();
                    }    
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

                var r = await WaitForNullAsync(() => AssertChangeVector(store));
                Assert.True(r == null, r);
            }
        }

        private static async Task<string> AssertChangeVector(DocumentStore store)
        {
            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
            string databaseChangeVector = null;
            foreach (var nodeTag in record.Topology.AllNodes)
            {
                var databaseStatistics = await store.Maintenance.SendAsync(new GetStatisticsOperation("get-change-vector", nodeTag));
                if (databaseChangeVector != null)
                {
                    if (databaseChangeVector != databaseStatistics.DatabaseChangeVector)
                        return $"{databaseChangeVector} != {databaseStatistics.DatabaseChangeVector}";
                }
                databaseChangeVector = databaseStatistics.DatabaseChangeVector;
            }

            return null;
        }

        private static void PutOrders(IDocumentSession session, int i = 1)
        {
            session.Store(
                new Order()
                {
                    OrderedAt = new DateTime(2019, 10, 26),
                    Lines =
                    [
                        new OrderLine { Product = $"products/{i}", },
                        new OrderLine { Product = $"products/{i + 1}", }
                    ]
                }, $"orders/{i}");

            session.Store(new Order 
            {
                OrderedAt = new DateTime(2019, 10, 25), 
                Lines = [
                    new OrderLine { Product = $"products/{i+1}", }]
            }, $"orders/{i + 1}");

            session.Store(new Order
            {
                OrderedAt = new DateTime(2019, 10, 24), 
                Lines = [
                    new OrderLine{ Product = $"products/{i}", }
                ]
            }, $"orders/{i+2}");
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
