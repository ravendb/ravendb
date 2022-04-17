using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using SlowTests.MailingList;
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
                record.Sharding.Prefixed = new Dictionary<string, List<ShardBucketRange>>
                {
                    ["eu/"] = new List<ShardBucketRange>()
                    {
                        new ShardBucketRange
                        {
                            ShardNumber = 0,
                            BucketRangeStart = 0
                        }
                    },
                    ["asia/"] = new List<ShardBucketRange>()
                    {
                        new ShardBucketRange
                        {
                            ShardNumber = 1,
                            BucketRangeStart = 0
                        },
                        new ShardBucketRange
                        {
                            ShardNumber = 2,
                            BucketRangeStart = 512*1024
                        }
                    },
                };
            }
        });
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
        
        using (var s = store.OpenAsyncSession(store.Database + "$0"))
        {
            // shard $0 has all the eu/ docs, no asia/ docs and fair share of the others
            Assert.Equal(73, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("eu/")));
            Assert.Equal(0, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("asia/")));
            Assert.Equal(25, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("us/")));
            Assert.Equal(19, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("items/")));
        }
        
        using (var s = store.OpenAsyncSession(store.Database + "$1"))
        {
            // shard $1 has no eu/ docs, half of the asia/ docs and fair share of the others
            Assert.Equal(0, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("eu/")));
            Assert.Equal(35, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("asia/")));
            Assert.Equal(19, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("us/")));
            Assert.Equal(23, await s.Query<Item>().CountAsync(i => i.Id.StartsWith("items/")));
        }
        
        using (var s = store.OpenAsyncSession(store.Database + "$2"))
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
