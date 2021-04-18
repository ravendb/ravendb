using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Index = Raven.Server.Documents.Indexes.Index;

namespace SlowTests.Rolling
{
    public class RollingIndexesClusterTests : ClusterTestBase
    {
        public RollingIndexesClusterTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task DeployStaticRollingIndex()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = false;
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var leaderStore = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3,
            }))
            {
                var dbName = leaderStore.Database;

                await GenerateTestData(leaderStore);

                var index = await CreateIndex(cluster, dbName);

                WaitForIndexingInTheCluster(leaderStore, dbName);

                using (cluster.Leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var record = cluster.Leader.ServerStore.Cluster.ReadDatabase(ctx, dbName);
                    var history = record.IndexesHistory;
                    var deployment = history[index][0].RollingDeployment;
                    
                    Assert.Equal(3, deployment.Count);
                    Assert.True(deployment.All(x => x.Value.State == RollingIndexState.Done));
                }
            }
        }

        [Fact]
        public async Task AddNewNodeWhileRollingIndexDeployed()
        {
            
        }

        [Fact]
        public async Task RollingIndexDeployedWithError()
        {

        }

        [Fact]
        public async Task RollingIndexDeployedSwapNow()
        {

        }

        [Fact]
        public async Task EditRollingIndexDeployedWhileOldDeploymentInProgress()
        {

        }

        [Fact]
        public async Task RollingIndexDeployedWhileNodeIsDown()
        {
           
        }

        [Fact]
        public async Task ForceIndexDeployed()
        {
            
        }

        private static async Task<string> CreateIndex((List<RavenServer> Nodes, RavenServer Leader) cluster, string dbName)
        {
            var indexDefinition = new IndexDefinition {Name = "order_companies", Maps = {"from order in docs.Orders select new { company = order.Company }"}, Rolling = true};

            var db = await cluster.Leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbName);

            await db.IndexStore.CreateIndexInternal(indexDefinition, Guid.NewGuid().ToString());

            foreach (var node in cluster.Nodes)
            {
                await WaitForIndex(dbName, indexDefinition.Name, node);
            }

            return indexDefinition.Name;
        }

        private static async Task<Index> WaitForIndex(string database, string name, RavenServer server)
        {
            var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);

            while (true)
            {
                await Task.Delay(250);

                try
                {
                    var index = db.IndexStore.GetIndex(name);
                    if(index == null)
                        continue;
                    return index;
                }
                catch (PendingRollingIndexException)
                {
                }
            }
        }

        private static async Task GenerateTestData(IDocumentStore leaderStore)
        {
            await leaderStore.Maintenance.SendAsync(new CreateSampleDataOperation());

            var operation = await leaderStore.Operations.SendAsync(new PatchByQueryOperation(@"FROM Orders UPDATE { put(""Orders/"", this); }"));

            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(2));
        }
    }
}
