using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3465 : RavenTestBase
    {
        [Fact(Skip = "RavenDB-6283")]
        public void get_metadata_for_sharded()
        {
            using (var shard1 = GetDocumentStore())
            using (var shard2 = GetDocumentStore())
            {
                var shards = new Dictionary<string, IDocumentStore>
                {
                    {"Shard1", shard1},
                    {"Shard2", shard2}
                };

                //var shardStrategy = new ShardStrategy(shards);
                //shardStrategy.ShardingOn<Profile>(x => x.Location);

                //using (var shardedDocumentStore = new ShardedDocumentStore(shardStrategy))
                //{
                //    shardedDocumentStore.Initialize();

                //    var profile = new Profile { Name = "Test", Location = "Shard1" };
                //    var profile2 = new Profile { Name = "Test2", Location = "Shard2" };

                //    using (var documentSession = shardedDocumentStore.OpenSession())
                //    {
                //        documentSession.Store(profile, profile.Id);
                //        documentSession.Store(profile2, profile2.Id);
                //        documentSession.SaveChanges();
                //    }
                //    using (var documentSession = shardedDocumentStore.OpenSession())
                //    {
                //        var correctId = profile.Id;
                //        var correctId2 = profile2.Id;

                //        documentSession.Store(profile, profile.Id);
                //        var metaData = documentSession.Advanced.GetMetadataFor(profile);
                //        var metaData2 = documentSession.Advanced.GetMetadataFor(profile2);

                //        Assert.NotNull(metaData);
                //        Assert.NotNull(metaData2);
                //        Assert.Equal(correctId, profile.Id);
                //    }
                //}
            }
        }

        [Fact(Skip = "RavenDB-6283")]
        public void get_metadata_for_async_sharded()
        {
            using (var shard1 = GetDocumentStore())
            using (var shard2 = GetDocumentStore())
            {
                var shards = new Dictionary<string, IDocumentStore>
                {
                    {"Shard1", shard1},
                    {"Shard2", shard2}
                };

                //var shardStrategy = new ShardStrategy(shards);
                //shardStrategy.ShardingOn<Profile>(x => x.Location);

                //using (var shardedDocumentStore = new ShardedDocumentStore(shardStrategy))
                //{
                //    shardedDocumentStore.Initialize();

                //    var profile = new Profile { Name = "Test", Location = "Shard1" };
                //    var profile2 = new Profile { Name = "Test2", Location = "Shard2" };

                //    using (var documentSession = shardedDocumentStore.OpenSession())
                //    {
                //        documentSession.Store(profile, profile.Id);
                //        documentSession.Store(profile2, profile2.Id);
                //        documentSession.SaveChanges();
                //    }

                //    using (var documentSession = shardedDocumentStore.OpenSession())
                //    {
                //        var metaData = documentSession.Advanced.GetMetadataFor(profile);
                //    }
                //    using (var documentSession = shardedDocumentStore.OpenAsyncSession())
                //    {
                //        //var data = await documentSession.LoadAsync<Profile>(profile.Id);
                //        var metaData = await documentSession.Advanced.GetMetadataForAsync(profile);
                //        var metaData2 = await documentSession.Advanced.GetMetadataForAsync(profile2);

                //        Assert.NotNull(metaData);
                //        Assert.NotNull(metaData2);
                //    }
                //}
            }
        }

        private class Profile
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string Location { get; set; }
        }
    }
}
