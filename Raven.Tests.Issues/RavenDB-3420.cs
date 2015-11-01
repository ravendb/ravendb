using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_3420 : RavenTestBase
    {
        [InlineData(null, null)]
        [InlineData("ShardedDatabase", "ShardedDatabase")]
        [InlineData("ShardedDatabase1", "ShardedDatabase2")]
        [Theory]
        public void bulk_insert_sharded(string databaseName1, string databaseName2)
        {
            var server1 = GetNewServer(8079);
            var server2 = GetNewServer(8078);
            var shards = new Dictionary<string, IDocumentStore>
            {
                {"Shard1", new DocumentStore {Url = server1.Configuration.ServerUrl, DefaultDatabase = databaseName1}},
                {"Shard2", new DocumentStore {Url = server2.Configuration.ServerUrl, DefaultDatabase = databaseName2}}
            };
            var shardStrategy = new ShardStrategy(shards);
            shardStrategy.ShardingOn<Profile>(x => x.Location);

            using (var shardedDocumentStore = new ShardedDocumentStore(shardStrategy))
            {
                shardedDocumentStore.Initialize();

                var entity1 = new Profile {Id = "bulk1", Name = "Hila", Location = "Shard1"};
                var entity2 = new Profile {Name = "Jay", Location = "Shard2"};
                var entity3 = new Profile {Name = "Jay", Location = "Shard1"};
                using (var bulkInsert = shardedDocumentStore.ShardedBulkInsert())
                {
                    bulkInsert.Store(entity1);
                    bulkInsert.Store(entity2);
                    bulkInsert.Store(entity3);
                }
            }

            using (var store1 = new DocumentStore { Url = server1.SystemDatabase.Configuration.ServerUrl, DefaultDatabase = databaseName1 }.Initialize())
            {
                using (var session = store1.OpenSession())
                {
                    var docs = session.Load<Profile>("Shard1/bulk1");
                    Assert.Equal("Shard1", docs.Location);
                    var docs2 = session.Load<Profile>("Shard1/profiles/2");
                    Assert.Equal("Shard1", docs2.Location);

                    var totalDocs = session.Query<Profile>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(2, totalDocs.Count);
                }
            }
            using (var store2 = new DocumentStore { Url = server2.SystemDatabase.Configuration.ServerUrl, DefaultDatabase = databaseName2 }.Initialize())
            {
                using (var session = store2.OpenSession())
                {
                    var docs = session.Load<Profile>("Shard2/profiles/1");
                    Assert.Equal("Shard2", docs.Location);

                    var totalDocs = session.Query<Profile>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(1, totalDocs.Count);
                }
            }
        }

        [InlineData(null)]
        [InlineData("ShardedDatabase")]
        [Theory]
        public void bulk_insert_sharded_when_specifying_default_database(string databaseName)
        {
            var server1 = GetNewServer(8079);
            var server2 = GetNewServer(8078);
            var shards = new Dictionary<string, IDocumentStore>
            {
                {"Shard1", new DocumentStore {Url = server1.Configuration.ServerUrl}},
                {"Shard2", new DocumentStore {Url = server2.Configuration.ServerUrl}}
            };
            var shardStrategy = new ShardStrategy(shards);
            shardStrategy.ShardingOn<Profile>(x => x.Location);

            EnsureDatabaseExists(databaseName, server1.Configuration.ServerUrl);
            EnsureDatabaseExists(databaseName, server2.Configuration.ServerUrl);

            using (var shardedDocumentStore = new ShardedDocumentStore(shardStrategy))
            {
                shardedDocumentStore.Initialize();

                var entity1 = new Profile { Id = "bulk1", Name = "Hila", Location = "Shard1" };
                var entity2 = new Profile { Name = "Jay", Location = "Shard2" };
                var entity3 = new Profile { Name = "Jay", Location = "Shard1" };
                using (var bulkInsert = shardedDocumentStore.ShardedBulkInsert(databaseName))
                {
                    bulkInsert.Store(entity1);
                    bulkInsert.Store(entity2);
                    bulkInsert.Store(entity3);
                }
            }

            using (var store1 = new DocumentStore { Url = server1.SystemDatabase.Configuration.ServerUrl, DefaultDatabase = databaseName }.Initialize())
            {
                using (var session = store1.OpenSession())
                {
                    var docs = session.Load<Profile>("Shard1/bulk1");
                    Assert.Equal("Shard1", docs.Location);
                    var docs2 = session.Load<Profile>("Shard1/profiles/2");
                    Assert.Equal("Shard1", docs2.Location);

                    var totalDocs = session.Query<Profile>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(2, totalDocs.Count);
                }
            }
            using (var store2 = new DocumentStore { Url = server2.SystemDatabase.Configuration.ServerUrl, DefaultDatabase = databaseName }.Initialize())
            {
                using (var session = store2.OpenSession())
                {
                    var docs = session.Load<Profile>("Shard2/profiles/1");
                    Assert.Equal("Shard2", docs.Location);

                    var totalDocs = session.Query<Profile>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();
                    Assert.Equal(1, totalDocs.Count);
                }
            }
        }

        private static void EnsureDatabaseExists(string databaseName, string serverUrl)
        {
            using (var store = new DocumentStore { Url = serverUrl }.Initialize())
            {
                if (databaseName != null)
                    store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists(databaseName);
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
