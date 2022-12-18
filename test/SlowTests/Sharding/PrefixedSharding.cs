using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding;

public class PrefixedSharding : RavenTestBase
{
    public PrefixedSharding(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task CanShardByDocumentsPrefix()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new Dictionary<string, PrefixedShardingSetting>
                {
                    ["eu/"] = new PrefixedShardingSetting
                    {
                        // range for 'eu/' is : 
                        // shard 0 : [1M, 2M]
                        Shards = new List<int> { 0 }
                    },
                    ["asia/"] = new PrefixedShardingSetting
                    {
                        // range for 'asia/' is :
                        // shard 1 : [2M, 2.5M]
                        // shard 2 : [2.5M, 3M]
                        Shards = new List<int> { 1, 2 }
                    }
                };
            }
        });

        var shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(6, shardingConfiguration.BucketRanges.Count);

        // 'eu' range
        Assert.Equal(0, shardingConfiguration.BucketRanges[3].ShardNumber);
        Assert.Equal(ShardHelper.NumberOfBuckets, shardingConfiguration.BucketRanges[3].BucketRangeStart);

        // 'asia' ranges
        Assert.Equal(1, shardingConfiguration.BucketRanges[4].ShardNumber);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2, shardingConfiguration.BucketRanges[4].BucketRangeStart);

        Assert.Equal(2, shardingConfiguration.BucketRanges[5].ShardNumber);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2.5, shardingConfiguration.BucketRanges[5].BucketRangeStart);

        using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            using (Slice.From(context.Allocator, "eu/0", out Slice idSlice))
            {
                var shardNumber = ShardHelper.GetShardNumberFor(shardingConfiguration, idSlice);
                Assert.Equal(0, shardNumber);
            }

            using (Slice.From(context.Allocator, "asia/1", out Slice idSlice))
            {
                var shardNumber = ShardHelper.GetShardNumberFor(shardingConfiguration, idSlice);
                Assert.Equal(1, shardNumber);
            }

            using (Slice.From(context.Allocator, "asia/2", out Slice idSlice))
            {
                var shardNumber = ShardHelper.GetShardNumberFor(shardingConfiguration, idSlice);
                Assert.Equal(2, shardNumber);
            }
        }

        var rand = new System.Random(2022_04_19);
        var prefixes = new[] { "us/", "eu/", "asia/", null };

        int d = 0;
        for (int t = 0; t < 16; t++)
        {
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 16; i++)
                {
                    string id = prefixes[rand.Next(prefixes.Length)] + "items/" + (++d);
                    await session.StoreAsync(new Item { }, id);
                }

                await session.SaveChangesAsync();
            }
        }

        using (var s = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, 0)))
        {
            // shard $0 has all the eu/ docs, no asia/ docs and fair share of the others
            Assert.Equal(73, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("eu/")));
            Assert.Equal(0, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("asia/")));
            Assert.Equal(25, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("us/")));
            Assert.Equal(19, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("items/")));
        }

        using (var s = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, 1)))
        {
            // shard $1 has no eu/ docs, half of the asia/ docs and fair share of the others
            Assert.Equal(0, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("eu/")));
            Assert.Equal(35, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("asia/")));
            Assert.Equal(19, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("us/")));
            Assert.Equal(23, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("items/")));
        }

        using (var s = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, 2)))
        {
            // shard $1 has no eu/ docs, half of the asia/ docs and fair share of the others
            Assert.Equal(0, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("eu/")));
            Assert.Equal(19, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("asia/")));
            Assert.Equal(22, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("us/")));
            Assert.Equal(21, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("items/")));
        }
    }

    private class Item
    {
        public string Id;
    }
}
