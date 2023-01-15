using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Indexes
{
    public class RavenDB_19621 : ClusterTestBase
    {
        public RavenDB_19621(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Indexes )]
        public async Task ShouldThrowOnAttemptToDeployStaticRollingIndexInShardedDatabase()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 2, orchestratorReplicationFactor: 3);

            using (var store = Sharding.GetDocumentStore(options))
            {
                var task = store.ExecuteIndexAsync(new MyRollingIndex());
                var e = await Assert.ThrowsAsync<NotSupportedInShardingException>(async () => await task);
                Assert.Contains("Rolling index deployment for a sharded database is currently not supported", e.Message);
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Indexes | RavenTestCategory.JavaScript)]
        public async Task ShouldThrowOnAttemptToDeployStaticJavascriptRollingIndexInShardedDatabase()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 2, orchestratorReplicationFactor: 3);

            using (var store = Sharding.GetDocumentStore(options))
            {
                var task = store.ExecuteIndexAsync(new MyJavaScriptRollingIndex());
                var e = await Assert.ThrowsAsync<NotSupportedInShardingException>(async () => await task);
                Assert.Contains("Rolling index deployment for a sharded database is currently not supported", e.Message);
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Indexes)]
        public async Task ShouldThrowOnAttemptToDeployStaticIndexInShardedDatabase_WhenDeploymentModeConfigurationIsRolling()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 2, orchestratorReplicationFactor: 3);
            options.ModifyDatabaseRecord += databaseRecord =>
            {
                databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexDeploymentMode)] = "Rolling";
            };

            using (var store = Sharding.GetDocumentStore(options))
            {
                var task = store.ExecuteIndexAsync(new MyIndex());
                var e = await Assert.ThrowsAsync<NotSupportedInShardingException>(async () => await task);
                Assert.Contains("Rolling index deployment for a sharded database is currently not supported", e.Message);
            }
        }


        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Indexes)]
        public async Task ShouldThrowOnAttemptToDeployAutoRollingIndexInShardedDatabase()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 2, orchestratorReplicationFactor: 3);

            options.ModifyDatabaseRecord += databaseRecord =>
            {
                databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexDeploymentMode)] = "Rolling";
            };

            using (var store = Sharding.GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Company = "companies/1" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<Order>()
                        .Where(o => o.Company != null)
                        .ToListAsync();

                    var e = await Assert.ThrowsAsync<NotSupportedInShardingException>(async () => await query);
                    Assert.Contains("Rolling index deployment for a sharded database is currently not supported", e.Message);
                }
            }
        }

        private class MyIndex : AbstractIndexCreationTask<Order>
        {
            public MyIndex()
            {
                Map = orders => from order in orders
                    select new
                    {
                        order.Company,
                    };
            }
        }

        private class MyRollingIndex : AbstractIndexCreationTask<Order>
        {
            public MyRollingIndex()
            {
                Map = orders => from order in orders
                    select new
                    {
                        order.Company,
                    };

                DeploymentMode = IndexDeploymentMode.Rolling;
            }
        }

        private class MyJavaScriptRollingIndex : AbstractJavaScriptIndexCreationTask
        {
            public MyJavaScriptRollingIndex()
            {
                Maps = new HashSet<string>
                {
                    @"map('Users', function (u){ return { Name: u.Name, Count: 1};})",
                };
                DeploymentMode = IndexDeploymentMode.Rolling;
            }
        }
    }
}
