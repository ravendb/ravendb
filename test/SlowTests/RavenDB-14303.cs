using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests
{
    public class RavenDB_14303 : ClusterTestBase
    {

        [Fact]
        public async Task IgnoreIdleIndexOnPromotable()
        {
            var clusterSize = 3;
            var (nodes, leader) = await CreateRaftCluster(clusterSize);
            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 2,

            }))
            {
                try
                {
                    Random rnd = new();
                    for (int i = 0; i < 100; i++)
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            await session.StoreAsync(new User { Name = "Toli", Count = i, Age = rnd.Next(1, 100) }, "users/" + i);
                            await session.SaveChangesAsync();
                        }
                    }

                    var count = new AutoIndexField
                    {
                        Name = "Count",
                    };

                    var age = new AutoIndexField
                    {
                        Name = "Age",
                    };

                    var sum = new AutoIndexField
                    {
                        Name = "Sum",
                    };
                    var node = nodes.First(n => n.ServerStore.DatabasesLandlord.IsDatabaseLoaded(store.Database));
                    var db = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var index = await db.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition("Users", new[] { count, sum }, new[] { age }), Guid.NewGuid().ToString());

                    await leader.ServerStore.Engine.SendToLeaderAsync(new SetIndexStateCommand(index.Name, IndexState.Idle, db.Name, Guid.NewGuid().ToString()));
                    db.ServerStore.ForTestingPurposesOnly().StopIndex = true;

                    var addRes = await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database));
                    await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(addRes.RaftCommandIndex, TimeSpan.FromSeconds(5));

                    var val = await WaitForValueAsync(async () => await GetPromotableCount(store, store.Database), 0);
                    Assert.Equal(0, val);
                    val = await WaitForValueAsync(async () => await GetMembersCount(store, store.Database), 3);
                    Assert.Equal(3, val);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
        }
        private static async Task<int> GetMembersCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Members.Count;
        }

        protected static async Task<int> GetPromotableCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Promotables.Count;
        }


        public RavenDB_14303(ITestOutputHelper output) : base(output)
        {
        }
    }
}
