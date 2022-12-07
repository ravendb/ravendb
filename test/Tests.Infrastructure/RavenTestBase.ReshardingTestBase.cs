using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Utils;
using Sparrow.Json;
using Xunit;

namespace FastTests;

public partial class RavenTestBase
{
    public class ReshardingTestBase
    {
        private readonly RavenTestBase _parent;

        public ReshardingTestBase(RavenTestBase parent)
        {
            _parent = parent;
        }

        public async Task StartMovingShardForId(IDocumentStore store, string id, List<RavenServer> servers = null)
        {
            servers ??= _parent.GetServers();

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var bucket = ShardHelper.GetBucket(id);
            var location = ShardHelper.GetShardNumber(record.Sharding.BucketRanges, bucket);
            var newLocation = (location + 1) % record.Sharding.Shards.Length;

            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
            {
                var user = await session.Advanced.ExistsAsync(id);
                Assert.NotNull(user);
            }

            foreach (var server in servers)
            {
                try
                {
                    await server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, location, newLocation);
                    break;
                }
                catch
                {
                    //
                }
            }


            var exists = _parent.WaitForDocument<dynamic>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, newLocation), timeout: 30_000);
            Assert.True(exists, $"{id} wasn't found at shard {newLocation}");
        }

        public async Task WaitForMigrationComplete(IDocumentStore store, string id)
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
            {
                var bucket = ShardHelper.GetBucket(id);
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database), cts.Token);
                while (record.Sharding.BucketMigrations.ContainsKey(bucket))
                {
                    await Task.Delay(250, cts.Token);
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database), cts.Token);
                }
            }
        }

        public async Task MoveShardForId(IDocumentStore store, string id, List<RavenServer> servers = null)
        {
            try
            {
                await StartMovingShardForId(store, id, servers);
                await WaitForMigrationComplete(store, id);
            }
            catch (Exception e)
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    var sharding = store.Conventions.Serialization.DefaultConverter.ToBlittable(record.Sharding, ctx).ToString();
                    throw new InvalidOperationException(
                        $"Failed to completed the migration for {id}{Environment.NewLine}{sharding}{Environment.NewLine}{_parent.Cluster.CollectLogsFromNodes(servers ?? new List<RavenServer> { _parent.Server })}",
                        e);
                }
            }
        }
    }
}
