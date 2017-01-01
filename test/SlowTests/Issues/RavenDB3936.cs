using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Client.Shard;
using Xunit;

namespace SlowTests.Issues
{
    public class ShardingWithAsyncTransformer : RavenTestBase
    {
        private class Profile
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string Location { get; set; }
        }

        private class Transformer : AbstractTransformerCreationTask<Profile>
        {
            public Transformer()
            {
                TransformResults = profiles =>
                    from profile in profiles
                    select new { profile.Name };
            }
        }

        private class Result
        {
#pragma warning disable 649
            public string Name;
#pragma warning restore 649
        }

        [Fact]
        public async Task CanUseAsyncTransformer()
        {
            var store1 = GetDocumentStore();
            var store2 = GetDocumentStore();
            var shards = new Dictionary<string, IDocumentStore>
            {
                {"Shard1", store1},
                {"Shard2", store2}
            };

            var shardStrategy = new ShardStrategy(shards);
            shardStrategy.ShardingOn<Profile>(x => x.Location);

            using (var shardedDocumentStore = new ShardedDocumentStore(shardStrategy))
            {
                shardedDocumentStore.Initialize();
                new Transformer().Execute(shardedDocumentStore);

                using (var session = shardedDocumentStore.OpenAsyncSession())
                {
                    await session.StoreAsync(new Profile
                    {
                        Name = "Oren",
                        Location = "Shard1"
                    });
                    await session.SaveChangesAsync();
                }

                using (var session = shardedDocumentStore.OpenAsyncSession())
                {
                    var results = await session.Query<Profile>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "Oren")
                        .TransformWith<Transformer, Result>()
                        .ToListAsync();

                    Assert.Equal("Oren", results[0].Name);
                }

            }
        }
    }
}
