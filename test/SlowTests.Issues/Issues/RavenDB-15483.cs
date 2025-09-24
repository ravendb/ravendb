using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15483 : ReplicationTestBase
    {
        public RavenDB_15483(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ExternalReplicationWithRevisionsBin2(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                await RevisionsHelper.SetupRevisionsAsync(store1, modifyConfiguration: configuration => configuration.Collections["Users"].PurgeOnDelete = false);

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
                var database = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, "foo/bar");
                var revisionsStorage = database.DocumentsStorage.RevisionsStorage;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var revisions = revisionsStorage.GetRevisionsBinEntries(context, 0, 2).Count();
                    Assert.Equal(1, revisions);
                }

                database = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, "foo/bar");
                revisionsStorage = database.DocumentsStorage.RevisionsStorage;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var revisions = revisionsStorage.GetRevisionsBinEntries(context, 0, 2).Count();
                    Assert.Equal(1, revisions);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ExternalReplicationWithRevisionsBin3(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                await RevisionsHelper.SetupRevisionsAsync(store1, modifyConfiguration: configuration => configuration.Collections["Users"].PurgeOnDelete = false);
                await RevisionsHelper.SetupRevisionsAsync(store2, modifyConfiguration: configuration => configuration.Collections["Users"].PurgeOnDelete = false);

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
                var database = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, "foo/bar");
                var revisionsStorage = database.DocumentsStorage.RevisionsStorage;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var revisions = revisionsStorage.GetRevisionsBinEntries(context, 0, 6).Count();
                    Assert.Equal(1, revisions);
                }

                database = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, "foo/bar");
                revisionsStorage = database.DocumentsStorage.RevisionsStorage;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var revisions = revisionsStorage.GetRevisionsBinEntries(context, 0, 6).Count();
                    Assert.Equal(1, revisions);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ExternalReplicationWithRevisionsBin4(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                await RevisionsHelper.SetupRevisionsAsync(store2, modifyConfiguration: configuration => configuration.Collections["Users"].PurgeOnDelete = true);

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
                var database = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, "foo/bar");
                var revisionsStorage = database.DocumentsStorage.RevisionsStorage;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var revisions = revisionsStorage.GetRevisionsBinEntries(context, 0, 6).Count();
                    Assert.Equal(0, revisions);
                }

                database = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, "foo/bar");
                revisionsStorage = database.DocumentsStorage.RevisionsStorage;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var revisions = revisionsStorage.GetRevisionsBinEntries(context, 0, 6).Count();
                    Assert.Equal(0, revisions);
                }
            }
        }
    }
}
