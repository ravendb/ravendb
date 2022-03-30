// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4708.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    /// <summary>
    /// We test async/sync x sharded/non sharded x single, singleOrDefault, first, firstOrDefault, count, countLazily
    /// </summary>
    public class RavenDB_4708 : RavenTestBase
    {
        public RavenDB_4708(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public void CanUseFirstSyncNonSharded()
        {
            using (var store = SetupNonShardedStore())
            {
                CanUseFirstSync(store);
            }
        }

        [Fact(Skip = "RavenDB-6283")]
        public void CanUseFirstSyncSharded()
        {
            //using (var documentStore = SetupShardedStore())
            //{
            //    CanUseFirstSync(documentStore);
            //}
        }

        [Fact]
        public async Task CanUseFirstAsyncNonSharded()
        {
            using (var store = SetupNonShardedStore())
            {
                await CanUseFirstAsync(store);
            }
        }

        [Fact(Skip = "RavenDB-6283")]
        public void CanUseFirstAsyncSharded()
        {
            //using (var documentStore = SetupShardedStore())
            //{
            //    await CanUseFirstAsync(documentStore);
            //}
        }

        [Fact]
        public void CanUseSingleSyncNonSharded()
        {
            using (var store = SetupNonShardedStore())
            {
                CanUseSingleSync(store);
            }
        }

        [Fact(Skip = "RavenDB-6283")]
        public void CanUseSingleSyncSharded()
        {
            //using (var documentStore = SetupShardedStore())
            //{
            //    CanUseSingleSync(documentStore);
            //}
        }

        [Fact]
        public async Task CanUseSigleAsyncNonSharded()
        {
            using (var store = SetupNonShardedStore())
            {
                await CanUseSingleAsync(store);
            }
        }

        [Fact(Skip = "RavenDB-6283")]
        public void CanUseSingleAsyncSharded()
        {
            //using (var documentStore = SetupShardedStore())
            //{
            //    await CanUseSingleAsync(documentStore);
            //}
        }

        [Fact]
        public void CanUseCountSyncNonSharded()
        {
            using (var store = SetupNonShardedStore())
            {
                CanUseCountAndCountLazilySync(store);
            }
        }

        [Fact(Skip = "RavenDB-6283")]
        public void CanUseCountSyncSharded()
        {
            //using (var documentStore = SetupShardedStore())
            //{
            //    CanUseCountAndCountLazilySync(documentStore);
            //}
        }

        [Fact]
        public async Task CanUseCountAsyncNonSharded()
        {
            using (var store = SetupNonShardedStore())
            {
                await CanUseCountAsync(store);
                await CanUseCountLazilyAsync(store);
            }
        }

        [Fact(Skip = "RavenDB-6283")]
        public void CanUseCountAsyncSharded()
        {
            //using (var documentStore = SetupShardedStore())
            //{
            //    CanUseCountAsync(documentStore).Wait();
            //    Assert.Throws<AggregateException>(() => CanUseCountLazilyAsync(documentStore).Wait());
            //}
        }

        [Fact]
        public void CanUseLazilySyncNonSharded()
        {
            using (var documentStore = SetupNonShardedStore())
            {
                CanUseLazilySync(documentStore);
            }
        }

        [Fact(Skip = "RavenDB-6283")]
        public void CanUseLazilySyncShaded()
        {
            //using (var documentStore = SetupShardedStore())
            //{
            //    CanUseLazilySync(documentStore);
            //}
        }

        [Fact]
        public async Task CanUseLazilyAsyncNonSharded()
        {
            using (var documentStore = SetupNonShardedStore())
            {
                await CanUseLazilyAsync(documentStore);
            }
        }

        [Fact(Skip = "RavenDB-6283")]
        public void UseLazilyAsyncShardedIsNotSupported()
        {
            //using (var documentStore = SetupShardedStore())
            //{
            //    Assert.Throws<AggregateException>(() => CanUseLazilyAsync(documentStore).Wait());
            //}
        }

        private static void CanUseFirstSync(DocumentStoreBase store)
        {
            using (var session = store.OpenSession())
            {
                var profile = session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "Google")
                    .First();

                Assert.NotNull(profile);

                Assert.Throws<InvalidOperationException>(() =>
                {
                    profile = session.Advanced.DocumentQuery<Profile>("ProfileByName")
                        .WhereLucene("Name", "NoSuch")
                        .First();
                });

                profile = session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "Google")
                    .FirstOrDefault();

                Assert.NotNull(profile);

                profile = session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "NoSuch")
                    .FirstOrDefault();

                Assert.Null(profile);
            }
        }

        private static async Task CanUseFirstAsync(DocumentStoreBase store)
        {
            using (var session = store.OpenAsyncSession())
            {
                Profile profile = await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "Google")
                    .FirstAsync();

                Assert.NotNull(profile);

                Assert.Throws<AggregateException>(() =>
                {
                    profile = session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                        .WhereLucene("Name", "NoSuch")
                        .FirstAsync().Result;
                });

                profile = await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "Google")
                    .FirstOrDefaultAsync();

                Assert.NotNull(profile);

                profile = await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "NoSuch")
                    .FirstOrDefaultAsync();

                Assert.Null(profile);
            }
        }

        private static void CanUseSingleSync(DocumentStoreBase store)
        {
            using (var session = store.OpenSession())
            {
                Profile profile;

                profile = session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "Google")
                    .Single();

                Assert.NotNull(profile);

                Assert.Throws<InvalidOperationException>(() =>
                {
                    profile = session.Advanced.DocumentQuery<Profile>("ProfileByName")
                        .Single();
                });

                profile = session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "NoSuch")
                    .SingleOrDefault();

                Assert.Null(profile);

                profile = session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "Google")
                    .SingleOrDefault();

                Assert.NotNull(profile);

                Assert.Throws<InvalidOperationException>(() =>
                {
                    profile = session.Advanced.DocumentQuery<Profile>("ProfileByName")
                        .SingleOrDefault();
                });
            }
        }

        private static async Task CanUseSingleAsync(DocumentStoreBase store)
        {
            using (var session = store.OpenAsyncSession())
            {
                Profile profile = await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "Google")
                    .SingleAsync();

                Assert.NotNull(profile);

                Assert.Throws<AggregateException>(() =>
                {
                    profile = session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                        .SingleAsync().Result;
                });

                profile = await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "NoSuch")
                    .SingleOrDefaultAsync();

                Assert.Null(profile);

                profile = await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "Google")
                    .SingleOrDefaultAsync();

                Assert.NotNull(profile);

                Assert.Throws<AggregateException>(() =>
                {
                    profile = session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                        .SingleOrDefaultAsync().Result;
                });
            }
        }

        private void CanUseCountAndCountLazilySync(DocumentStoreBase store)
        {
            using (var session = store.OpenSession())
            {
                Assert.Equal(0, session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "NoSuch")
                    .Count());

                Assert.Equal(1, session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "Google")
                  .Count());

                Assert.Equal(2, session.Advanced.DocumentQuery<Profile>("ProfileByName")
                  .Count());
            }

            using (var session = store.OpenSession())
            {
                Assert.Equal(0, session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "NoSuch")
                    .CountLazily().Value);

                Assert.Equal(1, session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "Google")
                    .CountLazily().Value);

                Assert.Equal(2, session.Advanced.DocumentQuery<Profile>("ProfileByName")
                  .CountLazily().Value);
            }
        }

        private async Task CanUseCountAsync(DocumentStoreBase store)
        {
            using (var session = store.OpenAsyncSession())
            {
                Assert.Equal(0, await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "NoSuch")
                    .CountAsync());

                Assert.Equal(1, await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "Google")
                  .CountAsync());

                Assert.Equal(2, await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                  .CountAsync());
            }
        }

        private async Task CanUseCountLazilyAsync(DocumentStoreBase store)
        {
            using (var session = store.OpenAsyncSession())
            {
                Assert.Equal(0, await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "NoSuch")
                    .CountLazilyAsync().Value);

                Assert.Equal(1, await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "Google")
                  .CountLazilyAsync().Value);

                Assert.Equal(2, await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                  .CountLazilyAsync().Value);
            }
        }

        private void CanUseLazilySync(DocumentStoreBase store)
        {
            using (var session = store.OpenSession())
            {
                Assert.Equal(0, session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "NoSuch")
                    .Lazily().Value.Count());

                Assert.Equal(1, session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "Google")
                  .Lazily().Value.Count());

                Assert.Equal(2, session.Advanced.DocumentQuery<Profile>("ProfileByName")
                  .Lazily().Value.Count());
            }
        }

        private async Task CanUseLazilyAsync(DocumentStoreBase store)
        {
            using (var session = store.OpenAsyncSession())
            {
                Assert.Equal(0, (await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "NoSuch")
                    .LazilyAsync(null).Value).Count());

                Assert.Equal(1, (await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .WhereLucene("Name", "Google")
                  .LazilyAsync(null).Value).Count());

                Assert.Equal(2, (await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                  .LazilyAsync(null).Value).Count());
            }
        }

        //private ShardedDocumentStore SetupShardedStore()
        //{
        //    var server1 = GetNewServer(8079);
        //    var server2 = GetNewServer(8078);
        //    var shards = new Dictionary<string, IDocumentStore>
        //    {
        //        {"Shard1", new DocumentStore {Url = server1.Configuration.ServerUrls}},
        //        {"Shard2", new DocumentStore {Url = server2.Configuration.ServerUrls}},
        //    };

        //    var shardStrategy = new ShardStrategy(shards);
        //    shardStrategy.ShardingOn<Profile>(x => x.Location);

        //    var shardedDocumentStore = new ShardedDocumentStore(shardStrategy);
        //    shardedDocumentStore.Initialize();

        //    FillDatabase(shardedDocumentStore);

        //    foreach (var documentStore in shards.Values)
        //    {
        //        Indexes.WaitForIndexing(documentStore);
        //    }

        //    return shardedDocumentStore;
        //}

        private DocumentStore SetupNonShardedStore()
        {
            var store = GetDocumentStore();
            FillDatabase(store);
            Indexes.WaitForIndexing(store);
            return store;
        }

        private static void FillDatabase(DocumentStoreBase store)
        {
            store.ExecuteIndex(new ProfileByName());

            var profile = new Profile { Name = "Google", Location = "Shard1" };
            var profile2 = new Profile { Name = "HibernatingRhinos", Location = "Shard2" };

            using (var documentSession = store.OpenSession())
            {
                documentSession.Store(profile, profile.Id);
                documentSession.Store(profile2, profile2.Id);
                documentSession.SaveChanges();
            }
        }

        private class ProfileByName : AbstractIndexCreationTask<Profile>
        {
            public ProfileByName()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Name
                              };
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
