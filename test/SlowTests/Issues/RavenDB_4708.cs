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

namespace SlowTests.Issues
{
    /// <summary>
    /// We test async/sync x sharded/non sharded x single, singleOrDefault, first, firstOrDefault, count, countLazily
    /// </summary>
    public class RavenDB_4708 : RavenTestBase
    {

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
        public void CanUseLazilyAsyncNonSharded()
        {
            using (var documentStore = SetupNonShardedStore())
            {
                CanUseLazilyAsync(documentStore).Wait();
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
                    .Where("Name", "Google")
                    .First();

                Assert.NotNull(profile);

                Assert.Throws<InvalidOperationException>(() =>
                {
                    profile = session.Advanced.DocumentQuery<Profile>("ProfileByName")
                        .Where("Name", "NoSuch")
                        .First();
                });

                profile = session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "Google")
                    .FirstOrDefault();

                Assert.NotNull(profile);

                profile = session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "NoSuch")
                    .FirstOrDefault();

                Assert.Null(profile);
            }
        }

        private static async Task CanUseFirstAsync(DocumentStoreBase store)
        {
            using (var session = store.OpenAsyncSession())
            {
                Profile profile = await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "Google")
                    .FirstAsync();

                Assert.NotNull(profile);

                Assert.Throws<AggregateException>(() =>
                {
                    profile = session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                        .Where("Name", "NoSuch")
                        .FirstAsync().Result;
                });

                profile = await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "Google")
                    .FirstOrDefaultAsync();

                Assert.NotNull(profile);

                profile = await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "NoSuch")
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
                    .Where("Name", "Google")
                    .Single();

                Assert.NotNull(profile);

                Assert.Throws<InvalidOperationException>(() =>
                {
                    profile = session.Advanced.DocumentQuery<Profile>("ProfileByName")
                        .Single();
                });

                profile = session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "NoSuch")
                    .SingleOrDefault();

                Assert.Null(profile);

                profile = session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "Google")
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
                    .Where("Name", "Google")
                    .SingleAsync();

                Assert.NotNull(profile);

                Assert.Throws<AggregateException>(() =>
                {
                    profile = session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                        .SingleAsync().Result;
                });

                profile = await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "NoSuch")
                    .SingleOrDefaultAsync();

                Assert.Null(profile);

                profile = await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "Google")
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
                    .Where("Name", "NoSuch")
                    .Count());

                Assert.Equal(1, session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "Google")
                  .Count());

                Assert.Equal(2, session.Advanced.DocumentQuery<Profile>("ProfileByName")
                  .Count());
            }

            using (var session = store.OpenSession())
            {
                Assert.Equal(0, session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "NoSuch")
                    .CountLazily().Value);

                Assert.Equal(1, session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "Google")
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
                    .Where("Name", "NoSuch")
                    .CountAsync());

                Assert.Equal(1, await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "Google")
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
                    .Where("Name", "NoSuch")
                    .CountLazilyAsync().Value);

                Assert.Equal(1, await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "Google")
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
                    .Where("Name", "NoSuch")
                    .Lazily().Value.Count());

                Assert.Equal(1, session.Advanced.DocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "Google")
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
                    .Where("Name", "NoSuch")
                    .LazilyAsync(null).Value).Count());

                Assert.Equal(1, (await session.Advanced.AsyncDocumentQuery<Profile>("ProfileByName")
                    .Where("Name", "Google")
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
        //        {"Shard1", new DocumentStore {Url = server1.Configuration.ServerUrl}},
        //        {"Shard2", new DocumentStore {Url = server2.Configuration.ServerUrl}},
        //    };

        //    var shardStrategy = new ShardStrategy(shards);
        //    shardStrategy.ShardingOn<Profile>(x => x.Location);

        //    var shardedDocumentStore = new ShardedDocumentStore(shardStrategy);
        //    shardedDocumentStore.Initialize();

        //    FillDatabase(shardedDocumentStore);

        //    foreach (var documentStore in shards.Values)
        //    {
        //        WaitForIndexing(documentStore);
        //    }

        //    return shardedDocumentStore;
        //}

        private DocumentStore SetupNonShardedStore()
        {
            var store = GetDocumentStore();
            FillDatabase(store);
            WaitForIndexing(store);
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