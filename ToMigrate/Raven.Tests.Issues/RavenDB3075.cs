using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class ShardedIdGenerationTest : RavenTestBase
    {
        public class Profile
        {
            public string Id { get; set; }
            
            public string Name { get; set; }

            public string Location { get; set; }
        }

        [Fact]
        public void OverwritingExistingDocumentGeneratesWrongIdWithShardedDocumentStore()
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

                using (var documentSession = shardedDocumentStore.OpenSession())
                {
                    documentSession.Store(profile, profile.Id);
                    documentSession.SaveChanges();
                }

                using (var documentSession = shardedDocumentStore.OpenSession())
                {
                    var correctId = profile.Id;

                    documentSession.Store(profile, profile.Id);

                    Assert.Equal(correctId, profile.Id);
                }
            }
        }
    }
}
