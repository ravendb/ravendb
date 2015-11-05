using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Server;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3465 : RavenTestBase
    {
        [Fact]
        public void get_metadata_for_sharded()
        {
            var server1 = GetNewServer(8079);
            var server2 = GetNewServer(8078);
            var shards = new List<IDocumentStore>
            {
                new DocumentStore {Identifier = "Shard1", Url = server1.Configuration.ServerUrl},
                new DocumentStore {Identifier = "Shard2", Url = server2.Configuration.ServerUrl}
            }
                .ToDictionary(x => x.Identifier, x => x);

            var shardStrategy = new ShardStrategy(shards);
            shardStrategy.ShardingOn<Profile>(x => x.Location);

            using (var shardedDocumentStore = new ShardedDocumentStore(shardStrategy))
            {
                shardedDocumentStore.Initialize();

                var profile = new Profile {Name = "Test", Location = "Shard1"};
                var profile2 = new Profile {Name = "Test2", Location = "Shard2"};

                using (var documentSession = shardedDocumentStore.OpenSession())
                {
                    documentSession.Store(profile, profile.Id);
                    documentSession.Store(profile2, profile2.Id);
                    documentSession.SaveChanges();
                }
                using (var documentSession = shardedDocumentStore.OpenSession())
                {
                    var correctId = profile.Id;
                    var correctId2 = profile2.Id;

                    documentSession.Store(profile, profile.Id);
                    var metaData = documentSession.Advanced.GetMetadataFor(profile);
                    var metaData2 = documentSession.Advanced.GetMetadataFor(profile2);

                    Assert.NotNull(metaData);
                    Assert.NotNull(metaData2);
                    Assert.Equal(correctId, profile.Id);
                }
            }
        }

        [Fact]
        public async Task get_metadata_for_async_sharded()
        {
            var server1 = GetNewServer(8079);
            var server2 = GetNewServer(8078);
            var shards = new Dictionary<string, IDocumentStore>
            {
                {"Shard1", new DocumentStore{Url = server1.Configuration.ServerUrl}},
                {"Shard2", new DocumentStore{Url = server2.Configuration.ServerUrl}},
            };

            var shardStrategy = new ShardStrategy(shards);
            shardStrategy.ShardingOn<Profile>(x => x.Location);

            using (var shardedDocumentStore = new ShardedDocumentStore(shardStrategy))
            {
                shardedDocumentStore.Initialize();

                var profile = new Profile {Name = "Test", Location = "Shard1"};
                var profile2 = new Profile {Name = "Test2", Location = "Shard2"};

                using (var documentSession = shardedDocumentStore.OpenSession())
                {
                    documentSession.Store(profile, profile.Id);
                    documentSession.Store(profile2, profile2.Id);
                    documentSession.SaveChanges();
                }

                using (var documentSession = shardedDocumentStore.OpenSession())
                {
                    var metaData = documentSession.Advanced.GetMetadataFor(profile);
                }
                using (var documentSession = shardedDocumentStore.OpenAsyncSession())
                {
                    //var data = await documentSession.LoadAsync<Profile>(profile.Id);
                    var metaData = await documentSession.Advanced.GetMetadataForAsync(profile);
                    var metaData2 = await documentSession.Advanced.GetMetadataForAsync(profile2);

                    Assert.NotNull(metaData);
                    Assert.NotNull(metaData2);
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
