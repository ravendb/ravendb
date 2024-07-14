using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
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

        public async Task<int> StartMovingShardForId(IDocumentStore store, string id, int? toShard = null, List<RavenServer> servers = null)
        {
            servers ??= _parent.GetServers();

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var bucket = _parent.Sharding.GetBucket(record.Sharding, id);
            PrefixedShardingSetting prefixedSetting = null;
            foreach (var setting in record.Sharding.Prefixed)
            {
                if (id.StartsWith(setting.Prefix, StringComparison.OrdinalIgnoreCase))
                {
                    prefixedSetting = setting;
                    break;
                }
            }  

            var shardNumber = ShardHelper.GetShardNumberFor(record.Sharding, bucket);
            var shards = prefixedSetting != null ? prefixedSetting.Shards : record.Sharding.Shards.Keys.ToList();

            int moveToShard;
            if (toShard.HasValue)
            {
                if (shards.Contains(toShard.Value) == false)
                    throw new InvalidOperationException($"Cannot move bucket '{bucket}' from shard {shardNumber} to shard {toShard}. " +
                                                        $"Sharding topology does not contain shard {toShard}");
                moveToShard = toShard.Value;
            }
            else
            {
                moveToShard = ShardingTestBase.GetNextSortedShardNumber(shards, shardNumber);
            }

            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, shardNumber)))
            {
                Assert.True(await session.Advanced.ExistsAsync(id), "The document doesn't exists on the source");
            }

            foreach (var server in servers)
            {
                try
                {
                    await server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, moveToShard, prefix: prefixedSetting?.Prefix, raftId: RaftIdGenerator.NewId());
                    break;
                }
                catch
                {
                    //
                }
            }
                
            var exists = _parent.WaitForDocument<dynamic>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, moveToShard), timeout: 30_000);
            Assert.True(exists, $"{id} wasn't found at shard {moveToShard}");

            return bucket;
        }

        public async Task WaitForMigrationComplete(IDocumentStore store, int bucket, int timeout = 30_000)
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(timeout)))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database), cts.Token);
                while (record.Sharding.BucketMigrations.ContainsKey(bucket))
                {
                    await Task.Delay(250, cts.Token);
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database), cts.Token);
                }
            }
        }

        public async Task MoveShardForId(IDocumentStore store, string id, int? toShard = null, List<RavenServer> servers = null, int timeout = 30_000)
        {
            try
            {
                servers ??= _parent.GetServers();
                var bucket = await StartMovingShardForId(store, id, toShard, servers);
                await WaitForMigrationComplete(store, bucket, timeout);
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
