using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
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
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        // range for 'eu/' is : 
                        // shard 0 : [1M, 2M]
                        Prefix = "eu/",
                        Shards = new List<int> { 0 }
                    },
                    new PrefixedShardingSetting
                    {
                        // range for 'asia/' is :
                        // shard 1 : [2M, 2.5M]
                        // shard 2 : [2.5M, 3M]
                        Prefix = "asia/",
                        Shards = new List<int> { 1, 2 }
                    }
                };
            }
        });

        var shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(6, shardingConfiguration.BucketRanges.Count);

        // 'eu' range
        Assert.Equal(ShardHelper.NumberOfBuckets, shardingConfiguration.Prefixed[0].BucketRangeStart);

        Assert.Equal(0, shardingConfiguration.BucketRanges[3].ShardNumber);
        Assert.Equal(ShardHelper.NumberOfBuckets, shardingConfiguration.BucketRanges[3].BucketRangeStart);

        // 'asia' ranges
        Assert.Equal(ShardHelper.NumberOfBuckets * 2, shardingConfiguration.Prefixed[1].BucketRangeStart);

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


    [Fact]
    public async Task ShouldThrowOnAttemptToAddPrefixThatDoesntEndWithSlashOrComma()
    {
        using var store = Sharding.GetDocumentStore();

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        var shardingConfiguration = record.Sharding;

        shardingConfiguration.Prefixed ??= new List<PrefixedShardingSetting>();
        shardingConfiguration.Prefixed.Add(new PrefixedShardingSetting
        {
            Prefix = "asia",
            Shards = new List<int> { 1, 2 }
        });

        var task = store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, replicationFactor: 1, record.Etag));
        var e = await Assert.ThrowsAsync<RavenException>(async () => await task);
        Assert.Contains(
            "Cannot add prefix 'asia' to ShardingConfiguration.Prefixed. In order to define sharding by prefix, the prefix string must end with '/' or '-' characters",
            e.Message);
    }

    [Fact]
    public async Task ShouldNotAllowToAddPrefixIfWeHaveDocsStartingWith()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/",
                        Shards = new List<int> { 0 }
                    }
                };
            }
        });

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "asia/items/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        var shardingConfiguration = record.Sharding;

        shardingConfiguration.Prefixed.Add(new PrefixedShardingSetting
        {
            Prefix = "asia/",
            Shards = new List<int> { 1, 2 }
        });

        var task =  store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, replicationFactor: 1, record.Etag));
        var e = await Assert.ThrowsAsync<RavenException>(async () => await task);
        Assert.Contains(
            $"Cannot add prefix 'asia/' to ShardingConfiguration.Prefixed. There are existing documents in database '{store.Database}' that start with 'asia/'",
            e.Message);
    }

    [Fact]
    public async Task ShouldNotAllowToDeletePrefixIfWeHaveDocsStartingWith()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        Prefix = "asia/",
                        Shards = new List<int> { 0 }
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/",
                        Shards = new List<int> { 1, 2 }
                    }
                };
            }
        });

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "asia/items/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        var shardingConfiguration = record.Sharding;

        shardingConfiguration.Prefixed.RemoveAt(0);

        var task = store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, replicationFactor: 1, record.Etag));
        var e = await Assert.ThrowsAsync<RavenException>(async () => await task);
        Assert.Contains(
            $"Cannot remove prefix 'asia/' from ShardingConfiguration.Prefixed. There are existing documents in database '{store.Database}' that start with 'asia/'",
            e.Message);
    }

    [Fact]
    public async Task CanAddPrefixIfNoDocsStartingWith()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/",
                        Shards = new List<int> { 0 }
                    }
                };
            }
        });

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "eu/items/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        var shardingConfiguration = record.Sharding;

        shardingConfiguration.Prefixed.Add(new PrefixedShardingSetting
        {
            Prefix = "asia/",
            Shards = new List<int> { 1, 2 }
        });

        Assert.Equal(4, shardingConfiguration.BucketRanges.Count);

        await store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, replicationFactor: 1, record.Etag));

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);

        Assert.Equal(2, shardingConfiguration.Prefixed.Count);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2, shardingConfiguration.Prefixed[1].BucketRangeStart);
        Assert.Equal(6, shardingConfiguration.BucketRanges.Count);
    }

    [Fact]
    public async Task CanDeletePrefixIfNoDocsStartingWith()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/",
                        Shards = new List<int> { 0, 1 }
                    }
                };
            }
        });

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "asia/items/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        var shardingConfiguration = record.Sharding;

        shardingConfiguration.Prefixed.RemoveAt(0);

        Assert.Equal(5, shardingConfiguration.BucketRanges.Count);

        await store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, replicationFactor: 1, record.Etag));

        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);

        Assert.Equal(0, shardingConfiguration.Prefixed.Count);
        Assert.Equal(3, shardingConfiguration.BucketRanges.Count);
    }

    [Fact]
    public async Task CanDeleteOnePrefixThenAddAnotherIfNoDocsStartingWith()
    {
        using var store = Sharding.GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Sharding ??= new ShardingConfiguration();
                record.Sharding.Prefixed = new List<PrefixedShardingSetting>
                {
                    new PrefixedShardingSetting
                    {
                        Prefix = "eu/",
                        Shards = new List<int> { 0, 1 }
                    },
                    new PrefixedShardingSetting
                    {
                        Prefix = "us/",
                        Shards = new List<int> { 1, 2 }
                    }
                };
            }
        });

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                string id = "asia/items/" + i;
                await session.StoreAsync(new Item(), id);
            }

            await session.SaveChangesAsync();
        }

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        var shardingConfiguration = record.Sharding;
        Assert.Equal(7, shardingConfiguration.BucketRanges.Count);

        // remove 'eu/' prefix
        shardingConfiguration.Prefixed.RemoveAt(0);
        await store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, replicationFactor: 1, record.Etag));
        
        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(1, shardingConfiguration.Prefixed.Count);
        Assert.Equal(5, shardingConfiguration.BucketRanges.Count);

        // add a new prefix
        record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        shardingConfiguration = record.Sharding;
        shardingConfiguration.Prefixed.Add(new PrefixedShardingSetting
        {
            Prefix = "africa/",
            Shards = new List<int> { 0, 2 }
        });

        await store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, replicationFactor: 1, record.Etag));
        shardingConfiguration = await Sharding.GetShardingConfigurationAsync(store);
        Assert.Equal(2, shardingConfiguration.Prefixed.Count);
        Assert.Equal(7, shardingConfiguration.BucketRanges.Count);

        Assert.Equal(ShardHelper.NumberOfBuckets * 2, shardingConfiguration.BucketRanges[3].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 2.5, shardingConfiguration.BucketRanges[4].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 3, shardingConfiguration.BucketRanges[5].BucketRangeStart);
        Assert.Equal(ShardHelper.NumberOfBuckets * 3.5, shardingConfiguration.BucketRanges[6].BucketRangeStart);
    }

    private class Item
    {
        public string Id;
    }
}
