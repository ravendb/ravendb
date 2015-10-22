using System.Collections.Generic;
using System.Threading.Tasks;
using Lucene.Net.Search.Function;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3938 : RavenTestBase
    {
        [Fact]
        public async Task DeleteItemOnAsyncShardedDocumentSession()
        {
            var server1 = GetNewServer(8079);
            var server2 = GetNewServer(8078);
            var shards = new Dictionary<string, IDocumentStore>
            {
                {"Shard1", new DocumentStore {Url = server1.Configuration.ServerUrl}},
                {"Shard2", new DocumentStore {Url = server2.Configuration.ServerUrl}},
            };

            var shardStrategy = new ShardStrategy(shards);
            shardStrategy.ShardingOn<Profile>(x => x.Location);

            using (var shardedDocumentStore = new ShardedDocumentStore(shardStrategy))
            {
                shardedDocumentStore.Initialize();

                var profile = new Profile { Name = "Test", Location = "Shard1" };
                var profile2 = new Profile { Name = "Test2", Location = "Shard2" };

                using (var documentSessionAsync = shardedDocumentStore.OpenAsyncSession())
                {
                    await documentSessionAsync.StoreAsync(profile, profile.Id);
                    await documentSessionAsync.StoreAsync(profile2, profile2.Id);
                    await documentSessionAsync.SaveChangesAsync();

                    documentSessionAsync.Delete(profile);
                    await documentSessionAsync.SaveChangesAsync();

                    var doc = documentSessionAsync.LoadAsync<Profile>(profile.Id);
                    Assert.Null(await doc);

                }

                using (var documentSessionAsync = shardedDocumentStore.OpenAsyncSession())
                {
                    Assert.Null(await documentSessionAsync.LoadAsync<Profile>(profile.Id));

                }
            }
        }

        public class Profile
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string Location { get; set; }
        }
    }
}