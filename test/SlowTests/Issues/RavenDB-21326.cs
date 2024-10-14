using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Esprima.Ast;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21326 : ClusterTestBase
    {
        public RavenDB_21326(ITestOutputHelper output) : base(output)
        {
        }


        [RavenFact(RavenTestCategory.Revisions)]
        public async Task TestReadAndWriteLastRevisionsBinCleanerState()
        {
            using (var store = GetDocumentStore())
            {
                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    RevisionsStorage.SetLastRevisionsBinCleanerLastEtag(context, 1234567890123456789);
                    tx.Commit();
                }


                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var state = RevisionsStorage.ReadLastRevisionsBinCleanerLastEtag(context.Transaction.InnerTransaction);

                    Assert.Equal(1234567890123456789, state);
                }
            }
        }


        [RavenTheory(RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RevisionsBinCleanerTest(Options options)
        {
            using var store = GetDocumentStore(options);

            var revisionsConfig = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, store.Database, revisionsConfig);

            var user1 = new User { Id = "Users/1-A", Name = "Shahar" };
            var user2 = new User { Id = "Users/2-B", Name = "Shahar" };
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user1);
                await session.StoreAsync(user2);
                await session.SaveChangesAsync();

                for (int i = 1; i <= 10; i++)
                {
                    (await session.LoadAsync<User>(user1.Id)).Name = $"Shahar{i}";
                    (await session.LoadAsync<User>(user2.Id)).Name = $"Shahar{i}";
                    await session.SaveChangesAsync();
                }

                session.Delete(user1.Id);
                session.Delete(user2.Id);
                await session.SaveChangesAsync();

                await session.StoreAsync(user2); // revive user2
                await session.SaveChangesAsync();

                Assert.Equal(12, await session.Advanced.Revisions.GetCountForAsync(user1.Id));
                Assert.Equal(13, await session.Advanced.Revisions.GetCountForAsync(user2.Id));
            }

            var config = new RevisionsBinConfiguration
            {
                MinimumEntriesAgeToKeep = TimeSpan.Zero,
                RefreshFrequency = TimeSpan.FromMilliseconds(200)
            };
            await ConfigRevisionsBinCleaner(store, config);

            await AssertWaitForValueAsync(async () =>
            {
                using (var session = store.OpenAsyncSession())
                {
                    return await session.Advanced.Revisions.GetCountForAsync(user1.Id);
                }
            }, 0);

            // End Cleanup
            config.Disabled = true;
            await ConfigRevisionsBinCleaner(store, config);

            using (var session = store.OpenAsyncSession())
            {
                session.Delete(user2.Id);
                await session.SaveChangesAsync(); // delete user2
            }

            using (var session = store.OpenAsyncSession())
            {
               Assert.Equal(14, await session.Advanced.Revisions.GetCountForAsync(user2.Id));
            }

        }


        [RavenFact(RavenTestCategory.Revisions)]
        public async Task RevisionsBinCleanerMaxReadsTest()
        {
            using var store = GetDocumentStore();

            var revisionsConfig = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, store.Database, revisionsConfig);

            var users = new List<User>();
            for (int i = 1; i <= 10; i++)
                users.Add(new User { Id = $"Users/{i}", Name = "Shahar" });

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.MaxNumberOfRequestsPerSession = Int32.MaxValue;

                foreach (var user in users)
                {
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                    for (int i = 1; i <= 10; i++)
                    {
                        (await session.LoadAsync<User>(user.Id)).Name = $"Shahar{i}";
                        await session.SaveChangesAsync();
                    }
                    session.Delete(user.Id);
                    await session.SaveChangesAsync();

                    Assert.Equal(12, await session.Advanced.Revisions.GetCountForAsync(user.Id));
                }
            }
            
            var config = new RevisionsBinConfiguration
            {
                MinimumEntriesAgeToKeep = TimeSpan.Zero,
                RefreshFrequency = TimeSpan.FromMilliseconds(200)
            };

            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            var cleaner = new RevisionsBinCleaner(db, config);

            var deletes = await cleaner.ExecuteCleanup();
            Assert.Equal(10, deletes);
            deletes = await cleaner.ExecuteCleanup();
            Assert.Equal(0, deletes);
        }


        [RavenFact(RavenTestCategory.Revisions)]
        public async Task RevisionsBinCleanerAgeTest()
        {
            var baseTime = new DateTime(year: 2024, month: 1, day: 1);

            using var store = GetDocumentStore();

            var revisionsConfig = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, store.Database, revisionsConfig);

            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            db.Time.UtcDateTime = () => baseTime - TimeSpan.FromDays(30);

            var users = new List<User>();
            for (int i = 0; i < 10; i++)
                users.Add(new User { Id = $"Users/{i}", Name = "Shahar" });

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.MaxNumberOfRequestsPerSession = Int32.MaxValue;

                for (int u = 0; u < 10; u++)
                {
                    var user = users[u];

                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                    for (int i = 1; i <= 10; i++)
                    {
                        (await session.LoadAsync<User>(user.Id)).Name = $"Shahar{i}";
                        await session.SaveChangesAsync();
                    }
                    session.Delete(user.Id);
                    await session.SaveChangesAsync();

                    Assert.Equal(12, await session.Advanced.Revisions.GetCountForAsync(user.Id));

                    if (u == 4)
                        db.Time.UtcDateTime = () => baseTime;
                }
            }

            // users 0-4 30 days ago, users 5-9 is now

            var config = new RevisionsBinConfiguration
            {
                MinimumEntriesAgeToKeep = TimeSpan.FromDays(15),
                RefreshFrequency = TimeSpan.FromMilliseconds(200)
            };

            var cleaner = new RevisionsBinCleaner(db, config);

            var deletes = await cleaner.ExecuteCleanup();
            Assert.Equal(5, deletes);
            deletes = await cleaner.ExecuteCleanup();
            Assert.Equal(0, deletes);

            using (var session = store.OpenAsyncSession())
            {
                for (int u = 0; u < 5; u++)
                {
                    Assert.Equal(0, await session.Advanced.Revisions.GetCountForAsync(users[u].Id));
                }
            }

            using (var session = store.OpenAsyncSession())
            {
                for (int u = 5; u < 10; u++)
                {
                    Assert.Equal(12, await session.Advanced.Revisions.GetCountForAsync(users[u].Id));
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task RevisionsBinCleanerStateTest()
        {
            var baseTime = new DateTime(year: 2024, month: 1, day: 1);

            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            db.Time.UtcDateTime = () => baseTime - TimeSpan.FromDays(30);

            var revisionsConfig = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, store.Database, revisionsConfig);

            var users = new List<User>();
            for (int i = 0; i < 10; i++)
                users.Add(new User { Id = $"Users/{i}", Name = "Shahar" });

            long lastEtag5;
            long lastEtag9;

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.MaxNumberOfRequestsPerSession = Int32.MaxValue;

                for (int u = 0; u < 10; u++)
                {
                    if(u == 5)
                        db.Time.UtcDateTime = () => baseTime;

                    var user = users[u];

                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                    for (int i = 1; i <= 10; i++)
                    {
                        (await session.LoadAsync<User>(user.Id)).Name = $"Shahar{i}";
                        await session.SaveChangesAsync();
                    }
                    session.Delete(user.Id);
                    await session.SaveChangesAsync();

                    Assert.Equal(12, await session.Advanced.Revisions.GetCountForAsync(user.Id));
                }

                var revisions5MetaData = (await session.Advanced.Revisions.GetMetadataForAsync(users[5].Id));
                Assert.True(revisions5MetaData[0].TryGetValue(Constants.Documents.Metadata.ChangeVector, out string cv5));

                lastEtag5 = ChangeVectorUtils.GetEtagById(cv5, db.DbBase64Id) + 1;

                var revisions9MetaData = (await session.Advanced.Revisions.GetMetadataForAsync(users[9].Id));
                Assert.True(revisions9MetaData[0].TryGetValue(Constants.Documents.Metadata.ChangeVector, out string cv9));

                lastEtag9 = ChangeVectorUtils.GetEtagById(cv9, db.DbBase64Id) + 1;
            }

            db.Time.UtcDateTime = () => baseTime;

            var config = new RevisionsBinConfiguration
            {
                MinimumEntriesAgeToKeep = TimeSpan.FromDays(15)
            };
            var cleaner = new RevisionsBinCleaner(db, config);

            var deletes = await cleaner.ExecuteCleanup();
            Assert.Equal(5, deletes);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var state = RevisionsStorage.ReadLastRevisionsBinCleanerLastEtag(context.Transaction.InnerTransaction);
                Assert.Equal(lastEtag5, state);
            }

            db.Time.UtcDateTime = () => baseTime + TimeSpan.FromDays(30);
            deletes = await cleaner.ExecuteCleanup();
            Assert.Equal(5, deletes);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var state = RevisionsStorage.ReadLastRevisionsBinCleanerLastEtag(context.Transaction.InnerTransaction);
                Assert.Equal(lastEtag9, state);
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task RevisionsBinCleanerStateTest2()
        {
            var baseTime = new DateTime(year: 2024, month: 1, day: 1);

            using var store = GetDocumentStore();
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            db.Time.UtcDateTime = () => baseTime - TimeSpan.FromDays(30);

            var revisionsConfig = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, store.Database, revisionsConfig);

            var users = new List<User>();
            for (int i = 0; i < 10; i++)
                users.Add(new User { Id = $"Users/{i}", Name = "Shahar" });

            long lastEtag0;

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.MaxNumberOfRequestsPerSession = Int32.MaxValue;

                for (int u = 0; u < 10; u++)
                {
                    var user = users[u];

                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                    for (int i = 1; i <= 10; i++)
                    {
                        (await session.LoadAsync<User>(user.Id)).Name = $"Shahar{i}";
                        await session.SaveChangesAsync();
                    }
                    session.Delete(user.Id);
                    await session.SaveChangesAsync();

                    Assert.Equal(12, await session.Advanced.Revisions.GetCountForAsync(user.Id));
                }

                var revisions0MetaData = await session.Advanced.Revisions.GetMetadataForAsync(users[0].Id);
                Assert.True(revisions0MetaData[0].TryGetValue(Constants.Documents.Metadata.ChangeVector, out string cv0));

                lastEtag0 = ChangeVectorUtils.GetEtagById(cv0, db.DbBase64Id) + 1;
            }

            var config = new RevisionsBinConfiguration
            {
                MinimumEntriesAgeToKeep = TimeSpan.FromDays(15)
            };
            var cleaner = new RevisionsBinCleaner(db, config);

            var deletes = await cleaner.ExecuteCleanup();
            Assert.Equal(0, deletes);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var state = RevisionsStorage.ReadLastRevisionsBinCleanerLastEtag(context.Transaction.InnerTransaction);
                Assert.Equal(lastEtag0, state);
            }

            db.Time.UtcDateTime = () => baseTime;
            deletes = await cleaner.ExecuteCleanup();
            Assert.Equal(10, deletes);
        }

        [RavenTheory(RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RevisionsBinCleanerClusterTest(Options options)
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);

            options.Server = leader;
            options.ReplicationFactor = 3;

            using var store = GetDocumentStore(options);

            var revisionsConfig = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, store.Database, revisionsConfig);

            var user1 = new User { Id = "Users/1-A", Name = "Shahar" };
            var user2 = new User { Id = "Users/2-B", Name = "Shahar" };
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user1);
                await session.StoreAsync(user2);
                await session.SaveChangesAsync();

                for (int i = 1; i <= 10; i++)
                {
                    (await session.LoadAsync<User>(user1.Id)).Name = $"Shahar{i}";
                    (await session.LoadAsync<User>(user2.Id)).Name = $"Shahar{i}";
                    await session.SaveChangesAsync();
                }

                session.Delete(user1.Id);
                session.Delete(user2.Id);
                await session.SaveChangesAsync();

                await session.StoreAsync(user2); // revive user2
                await session.SaveChangesAsync();

                Assert.Equal(12, await session.Advanced.Revisions.GetCountForAsync(user1.Id));
                Assert.Equal(13, await session.Advanced.Revisions.GetCountForAsync(user2.Id));
            }

            var config = new RevisionsBinConfiguration
            {
                MinimumEntriesAgeToKeep = TimeSpan.Zero,
                RefreshFrequency = TimeSpan.FromMilliseconds(200)
            };
            await ConfigRevisionsBinCleaner(store, config);

            using var stores = new NodesDocumentStores(nodes, store.Database);

            await AssertWaitForValueAsync(async () =>
            {
                foreach (var node in nodes)
                {
                    using (var session = stores.GetStore(node.ServerStore.NodeTag).OpenAsyncSession())
                    {
                        var count = await session.Advanced.Revisions.GetCountForAsync(user1.Id);
                        if (count != 0)
                        {
                            var db = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                            var topology = db.ReadDatabaseRecord().Topology;
                            return $"node {node} has {count} revisions for 'user1.Id' instead of 0 revisions, Database Topology: {topology}";
                        }
                    }
                }

                return string.Empty;
            }, string.Empty);

            if (options.DatabaseMode == RavenDatabaseMode.Single)
            {
                await AssertDatabaseSingleCleaner(nodes, store.Database);
            }
            else if (options.DatabaseMode == RavenDatabaseMode.Sharded)
            {
                for (int i = 0; i < 3; i++)
                {
                    await AssertDatabaseSingleCleaner(nodes, store.Database+"$"+i);
                }
            }


        }

        private async Task AssertDatabaseSingleCleaner(IEnumerable<RavenServer> nodes, string database)
        {
            var cleanersCount = 0;
            foreach (var node in nodes)
            {
                var db = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                if (db.RevisionsBinCleaner != null)
                    cleanersCount++;
            }

            Assert.Equal(1, cleanersCount);
        }

        private class NodesDocumentStores : IDisposable
        {
            private Dictionary<string, IDocumentStore> _stores;

            public NodesDocumentStores(IEnumerable<RavenServer> nodes, string database)
            {
                _stores = new Dictionary<string, IDocumentStore>();
                foreach (var node in nodes)
                {
                    _stores[node.ServerStore.NodeTag] = GetStoreForServer(node, database);
                }
            }

            public IDocumentStore GetStore(string nodeTag)
            {
                return _stores[nodeTag];
            }

            private IDocumentStore GetStoreForServer(RavenServer server, string database)
            {
                return new DocumentStore
                {
                    Database = database,
                    Urls = new[] { server.WebUrl },
                    Conventions = new DocumentConventions { DisableTopologyUpdates = true }
                }.Initialize();
            }

            public void Dispose()
            {
                foreach (var store in _stores.Values)
                {
                    store?.Dispose();
                }
            }
        }




        private async Task ConfigRevisionsBinCleaner(DocumentStore store, RevisionsBinConfiguration config)
        {
            var result = await store.Maintenance.SendAsync(new ConfigureRevisionsBinCleanerOperation(config));
            await store.Maintenance.SendAsync(new WaitForIndexNotificationOperation(result.RaftCommandIndex.Value));

            await AssertWaitForTrueAsync(async () =>
            {
                var record = await GetDatabaseRecordAsync(store);
                return config.Equals(record.RevisionsBin);
            });
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

    }
}
