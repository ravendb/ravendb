using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Server;
using Raven.Tests.Helpers;
using Xunit;

namespace RavenDbShardingTests
{
    public class ShardedIdGenerationTest : RavenTestBase
    {
        public class Profile
        {
            public string Id { get; set; }
            
            public string Name { get; set; }

            public string Location { get; set; }
        }

        public IDocumentStore NewRemoteDocumentStoreWithUrl(int port, bool fiddler = false, RavenDbServer ravenDbServer = null, string databaseName = null,
            bool runInMemory = true,
            string dataDirectory = null,
            string requestedStorage = null,
            bool enableAuthentication = false)
        {
            ravenDbServer = ravenDbServer ?? GetNewServer(runInMemory: runInMemory, dataDirectory: dataDirectory, requestedStorage: requestedStorage, enableAuthentication: enableAuthentication);
            ModifyServer(ravenDbServer);
            var store = new DocumentStore
            {
                Url = GetServerUrl(port),
                DefaultDatabase = databaseName,
            };
            stores.Add(store);
            store.AfterDispose += (sender, args) => ravenDbServer.Dispose();
            ModifyStore(store);
            return store.Initialize();
        }

        private static string GetServerUrl(int port)
        {
            return "http://localhost:" + port;
        }

        [Fact]
        public void OverwritingExistingDocumentGeneratesWrongIdWithShardedDocumentStore()
        {
            using (var store1 = NewRemoteDocumentStoreWithUrl(8079, ravenDbServer: GetNewServer(8079)))
            {
                using (var store2 = NewRemoteDocumentStoreWithUrl(8078, ravenDbServer: GetNewServer(8078)))
                {
                    var shards = new List<IDocumentStore> { 
                        new DocumentStore { Identifier="Shard1", Url = store1.Url}, 
                        new DocumentStore { Identifier="Shard2", Url = store2.Url} }
                            .ToDictionary(x => x.Identifier, x => x);

                    var shardStrategy = new ShardStrategy(shards);
                    shardStrategy.ShardingOn<Profile>(x => x.Location);

                    using (var shardedDocumentStore = new ShardedDocumentStore(shardStrategy))
                    {
                        shardedDocumentStore.Initialize();

                        var profile = new Profile { Name = "Test", Location = "Shard1" };

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
    }
}