using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_21084 : RavenTestBase
{
    public RavenDB_21084(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sharding)]
    public async Task DisableShardedDb_ShouldRemoveDbFromShardedDatabasesCache()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            var dbLandlord = Server.ServerStore.DatabasesLandlord;

            Assert.True(dbLandlord.ShardedDatabasesCache.TryGetValue(store.Database, out _));

            var shardingConfig = await Sharding.GetShardingConfigurationAsync(store);
            foreach (var shardNumber in shardingConfig.Shards.Keys)
            {
                var shard = ShardHelper.ToShardName(store.Database, shardNumber);
                Assert.True(dbLandlord.DatabasesCache.TryGetValue(shard, out _));
            }

            var operationResult = await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: true));
            Assert.True(operationResult.Success);
            Assert.True(operationResult.Disabled);

            // should remove document databases from DatabasesCache
            foreach (var shardNumber in shardingConfig.Shards.Keys)
            {
                var shard = ShardHelper.ToShardName(store.Database, shardNumber);
                Assert.False(dbLandlord.DatabasesCache.TryGetValue(shard, out _));
            }

            // should remove sharded database context from ShardedDatabasesCache
            Assert.False(dbLandlord.ShardedDatabasesCache.TryGetValue(store.Database, out _));

            await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: false));
        }
    }

}
