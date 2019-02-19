using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3420 : RavenTestBase
    {
        [Theory(Skip = "RavenDB-6283")]
        [InlineData("ShardedDatabase", "ShardedDatabase")]
        [InlineData("ShardedDatabase1", "ShardedDatabase2")]
        public void bulk_insert_sharded(string databaseName1, string databaseName2)
        {
            using (var server1 = GetNewServer())
            using (var server2 = GetNewServer())
            {
                var shard1 = new DocumentStore
                {
                    Urls = UseFiddler(Server.WebUrl),
                    Database = databaseName1
                };
                shard1.Initialize();

                var shard2 = new DocumentStore
                {
                    Urls = UseFiddler(Server.WebUrl),
                    Database = databaseName2
                };
                shard2.Initialize();

                var shards = new Dictionary<string, IDocumentStore>
                {
                    {"Shard1", shard1},
                    {"Shard2", shard2}
                };

                //var shardStrategy = new ShardStrategy(shards);
                //shardStrategy.ShardingOn<Profile>(x => x.Location);

                //CreateDatabase(shard1, shard1.Database);
                //CreateDatabase(shard2, shard2.Database);

                //using (var shardedDocumentStore = new ShardedDocumentStore(shardStrategy))
                //{
                //    shardedDocumentStore.Initialize();

                //    var entity1 = new Profile { Id = "bulk1", Name = "Hila", Location = "Shard1" };
                //    var entity2 = new Profile { Name = "Jay", Location = "Shard2" };
                //    var entity3 = new Profile { Name = "Jay", Location = "Shard1" };
                //    using (var bulkInsert = shardedDocumentStore.ShardedBulkInsert())
                //    {
                //        bulkInsert.Store(entity1);
                //        bulkInsert.Store(entity2);
                //        bulkInsert.Store(entity3);
                //    }
                //}

                using (var store1 = new DocumentStore { Urls = new[] {server1.WebUrl}, Database = databaseName1 }.Initialize())
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
                using (var store2 = new DocumentStore { Urls = new[] {server2.WebUrl}, Database = databaseName2 }.Initialize())
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
        }

        [Theory(Skip = "RavenDB-6283")]
        [InlineData("ShardedDatabase")]
        public void bulk_insert_sharded_when_specifying_default_database(string databaseName)
        {
            using (var store = GetDocumentStore())
            using (var server1 = GetNewServer())
            using (var server2 = GetNewServer())
            {
                var shard1 = new DocumentStore
                {
                    Urls = new[] {server1.WebUrl},
                    Database = store.Database
                };
                shard1.Initialize();

                var shard2 = new DocumentStore
                {
                    Urls = new[] {server2.WebUrl},
                    Database = store.Database
                };
                shard2.Initialize();

                var shards = new Dictionary<string, IDocumentStore>
                {
                    {"Shard1", shard1},
                    {"Shard2", shard2}
                };

                //var shardStrategy = new ShardStrategy(shards);
                //shardStrategy.ShardingOn<Profile>(x => x.Location);

                //CreateDatabase(shard1, databaseName);
                //CreateDatabase(shard2, databaseName);

                //using (var shardedDocumentStore = new ShardedDocumentStore(shardStrategy))
                //{
                //    shardedDocumentStore.Initialize();

                //    var entity1 = new Profile { Id = "bulk1", Name = "Hila", Location = "Shard1" };
                //    var entity2 = new Profile { Name = "Jay", Location = "Shard2" };
                //    var entity3 = new Profile { Name = "Jay", Location = "Shard1" };
                //    using (var bulkInsert = shardedDocumentStore.ShardedBulkInsert(databaseName))
                //    {
                //        bulkInsert.Store(entity1);
                //        bulkInsert.Store(entity2);
                //        bulkInsert.Store(entity3);
                //    }
                //}

                using (var store1 = new DocumentStore { Urls = new[] {server1.WebUrl}, Database = databaseName }.Initialize())
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

                using (var store2 = new DocumentStore { Urls = new[] {server2.WebUrl}, Database = databaseName }.Initialize())
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
        }

        //private static void CreateDatabase(IDocumentStore store, string databaseName)
        //{
        //    store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument(databaseName));
        //}

        private class Profile
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string Location { get; set; }
        }
    }
}
