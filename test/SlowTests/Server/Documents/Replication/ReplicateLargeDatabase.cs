using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Replication
{
    public class ReplicateLargeDatabase : ReplicationTestBase
    {
        public ReplicateLargeDatabase(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task AutomaticResolveWithIdenticalContent()
        {
            DocumentStore store1;
            DocumentStore store2;

            CreateSampleDatabase(out store1);
            CreateSampleDatabase(out store2);

            await SetupReplicationAsync(store1, store2);
            Assert.Equal(1, WaitForValue(() => store2.Maintenance.Send(new GetReplicationPerformanceStatisticsOperation()).Incoming.Length, 1));
            var stats = store2.Maintenance.Send(new GetReplicationPerformanceStatisticsOperation());
            var errors = stats.Incoming
                .SelectMany(x => x.Performance.Where(y => y.Errors != null).SelectMany(z => z.Errors)).ToList();
            Assert.Empty(errors);
        }

        [Fact]
        public async Task AutomaticResolveWithIdenticalContentForSharding()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                CallCreateSampleDatabaseEndpoint((DocumentStore)store1);
                CallCreateSampleDatabaseEndpoint((DocumentStore)store2);

                await SetupReplicationAsync(store1, store2);

                int shardNumber = 0;
                int replicationIncoming = 0;
                ReplicationPerformance replicationPerformance = null;
                var dbs = Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store2.Database);
                await AssertWaitForValueAsync(async () =>
                {
                    foreach (var task in dbs)
                    {
                        var db = await task;
                        shardNumber = ShardHelper.GetShardNumber(db.Name);
          
                        replicationPerformance = await store2.Maintenance.ForShard(shardNumber).SendAsync(new GetReplicationPerformanceStatisticsOperation());
                        replicationIncoming = replicationPerformance.Incoming.Length;
                        return replicationPerformance.Incoming.Length;
                    }

                    return 0;
                }, 3, 30_000, 333);

                var errors = replicationPerformance?.Incoming
                    .SelectMany(x => x.Performance.Where(y => y.Errors != null).SelectMany(z => z.Errors))?.ToList();
                Assert.Empty(errors);
            }
        }

        public void CreateSampleDatabase(out DocumentStore store)
        {
            store = GetDocumentStore();
            CallCreateSampleDatabaseEndpoint(store);
        }

        public void CallCreateSampleDatabaseEndpoint(DocumentStore store)
        {
            store.Maintenance.Send(new CreateSampleDataOperation());
        }
    }
}
