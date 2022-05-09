using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15483 : ReplicationTestBase
    {
        public RavenDB_15483(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ExternalReplicationWithRevisionsBin2()
        {
            using (var store1 = GetDocumentStore())

            using (var store2 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database, modifyConfiguration: configuration => configuration.Collections["Users"].PurgeOnDelete = false);

                var externalTask = new ExternalReplication(store2.Database, "ExternalReplication");
                await AddWatcherToReplicationTopology(store1, externalTask);

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User() { Name = "Toli" }, "foo/bar");
                    s1.SaveChanges();
                }

                Assert.True(WaitForDocument(store2, "foo/bar"));
                for (int i = 0; i < 4; i++)
                {
                    var name = "Toli" + i;
                    using (var s1 = store1.OpenSession())
                    {
                        s1.Store(new User() { Name = name }, "foo/bar");
                        s1.SaveChanges();
                    }
                }

                using (var s1 = store1.OpenSession())
                {
                    s1.Delete("foo/bar");
                    s1.SaveChanges();
                }

                await WaitForValueAsync(async () =>
                {
                    using (var s2 = store2.OpenAsyncSession())
                    {
                        var user = await s2.LoadAsync<User>("foo/bar");
                        return user == null;
                    }
                }, true);
                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database);
                var revisionsStorage = database.DocumentsStorage.RevisionsStorage;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var revisions = revisionsStorage.GetRevisionsBinEntries(context, long.MaxValue, 2).Count();
                    Assert.Equal(1, revisions);
                }

                database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store2.Database);
                revisionsStorage = database.DocumentsStorage.RevisionsStorage;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var revisions = revisionsStorage.GetRevisionsBinEntries(context, long.MaxValue, 2).Count();
                    Assert.Equal(1, revisions);
                }
            }
        }

        [Fact]
        public async Task ExternalReplicationWithRevisionsBin3()
        {
            using (var store1 = GetDocumentStore())

            using (var store2 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database, modifyConfiguration: configuration => configuration.Collections["Users"].PurgeOnDelete = false);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database, modifyConfiguration: configuration => configuration.Collections["Users"].PurgeOnDelete = false);

                var externalTask = new ExternalReplication(store2.Database, "ExternalReplication");
                await AddWatcherToReplicationTopology(store1, externalTask);

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User() { Name = "Toli" }, "foo/bar");
                    s1.SaveChanges();
                }

                Assert.True(WaitForDocument(store2, "foo/bar"));
                for (int i = 0; i < 4; i++)
                {
                    var name = "Toli" + i;
                    using (var s1 = store1.OpenSession())
                    {
                        s1.Store(new User() { Name = name }, "foo/bar");
                        s1.SaveChanges();
                    }

                    await WaitForValueAsync(async () =>
                    {
                        using (var s2 = store2.OpenAsyncSession())
                        {
                            var user = await s2.LoadAsync<User>("foo/bar");
                            return name.Equals(user.Name, StringComparison.InvariantCultureIgnoreCase);
                        }
                    }, true);
                }
                using (var s1 = store1.OpenSession())
                {
                    s1.Delete("foo/bar");
                    s1.SaveChanges();
                }
                await WaitForValueAsync(async () =>
                {
                    using (var s2 = store2.OpenAsyncSession())
                    {
                        var user = await s2.LoadAsync<User>("foo/bar");
                        return user == null;
                    }
                }, true);
                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store2.Database);
                var revisionsStorage = database.DocumentsStorage.RevisionsStorage;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var revisions = revisionsStorage.GetRevisionsBinEntries(context, long.MaxValue, 6).Count();
                    Assert.Equal(1, revisions);
                }

                database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database);
                revisionsStorage = database.DocumentsStorage.RevisionsStorage;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var revisions = revisionsStorage.GetRevisionsBinEntries(context, long.MaxValue, 6).Count();
                    Assert.Equal(1, revisions);
                }
            }
        }

        [Fact]
        public async Task ExternalReplicationWithRevisionsBin4()
        {
            using (var store1 = GetDocumentStore())

            using (var store2 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database, modifyConfiguration: configuration => configuration.Collections["Users"].PurgeOnDelete = true);

                var externalTask = new ExternalReplication(store2.Database, "ExternalReplication");
                await AddWatcherToReplicationTopology(store1, externalTask);

                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User() { Name = "Toli" }, "foo/bar");
                    s1.SaveChanges();
                }

                Assert.True(WaitForDocument(store2, "foo/bar"));
                for (int i = 0; i < 4; i++)
                {
                    var name = "Toli" + i;
                    using (var s1 = store1.OpenSession())
                    {
                        s1.Store(new User() { Name = name }, "foo/bar");
                        s1.SaveChanges();
                    }

                    await WaitForValueAsync(async () =>
                    {
                        using (var s2 = store2.OpenAsyncSession())
                        {
                            var user = await s2.LoadAsync<User>("foo/bar");
                            return name.Equals(user.Name, StringComparison.InvariantCultureIgnoreCase);
                        }
                    }, true);
                }

                using (var s1 = store1.OpenSession())
                {
                    s1.Delete("foo/bar");
                    s1.SaveChanges();
                }
                await WaitForValueAsync(async () =>
                {
                    using (var s2 = store2.OpenAsyncSession())
                    {
                        var user = await s2.LoadAsync<User>("foo/bar");
                        return user == null;
                    }
                }, true);
                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store2.Database);
                var revisionsStorage = database.DocumentsStorage.RevisionsStorage;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var revisions = revisionsStorage.GetRevisionsBinEntries(context, long.MaxValue, 6).Count();
                    Assert.Equal(0, revisions);
                }

                database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database);
                revisionsStorage = database.DocumentsStorage.RevisionsStorage;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var revisions = revisionsStorage.GetRevisionsBinEntries(context, long.MaxValue, 6).Count();
                    Assert.Equal(0, revisions);
                }
            }
        }
    }
}
