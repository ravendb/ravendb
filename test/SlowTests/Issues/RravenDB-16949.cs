using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Orders;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.ServerWide;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RravenDB_16949 : RavenTestBase
    {
        public RravenDB_16949(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task limitRevisionDeletion()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 1000000
                    },
                };
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Toli" }, "users/1");
                    await session.StoreAsync(new User { Name = "Mitzi" }, "users/2");
                    await session.StoreAsync(new Order { Employee = "Daniel" }, "orders/1");
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 500; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Toli" + i }, "users/1");
                        await session.StoreAsync(new User { Name = "Mitzi" + i }, "users/2");
                        await session.StoreAsync(new Order { Employee = "Daniel" + i }, "orders/1");
                        await session.SaveChangesAsync();
                    }
                }
                for (int i = 500; i < 630; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Mitzi" + i }, "users/2");
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {

                    var revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/1", 0, 1000).Result.Count;
                    Assert.Equal(501, revisionCount);
                    revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/2", 0, 1000).Result.Count;
                    Assert.Equal(631, revisionCount);
                    revisionCount = session.Advanced.Revisions.GetForAsync<User>("orders/1", 0, 1000).Result.Count;
                    Assert.Equal(501, revisionCount);
                }

                configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 10,
                        MaximumRevisionsToDeleteUponDocumentUpdate = 100
                    },
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        ["Orders"] = new RevisionsCollectionConfiguration
                        {
                            Disabled = false,
                            MinimumRevisionsToKeep = 33,
                            MaximumRevisionsToDeleteUponDocumentUpdate = 35
                        }
                    }

                };
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Toli" }, "users/1");
                    await session.StoreAsync(new User { Name = "Toli" }, "users/2");
                    await session.StoreAsync(new Order { Employee = "Toli" }, "orders/1");
                    await session.SaveChangesAsync();

                    var revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/1", 0, 1000).Result.Count;
                    Assert.Equal(402, revisionCount);
                    revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/2", 0, 1000).Result.Count;
                    Assert.Equal(532, revisionCount);
                    revisionCount = session.Advanced.Revisions.GetForAsync<Order>("orders/1", 0, 1000).Result.Count;
                    Assert.Equal(467, revisionCount);
                }

                for (int i = 0; i < 4; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "A" + i }, "users/1");
                        await session.StoreAsync(new User { Name = "B" + i}, "users/2");
                        await session.StoreAsync(new Order { Employee = "C" + i}, "orders/1");
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/1", 0, 1000).Result.Count;
                    Assert.Equal(10, revisionCount);
                    revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/2", 0, 1000).Result.Count;
                    Assert.Equal(136, revisionCount);
                    revisionCount = session.Advanced.Revisions.GetForAsync<Order>("orders/1", 0, 1000).Result.Count;
                    Assert.Equal(331, revisionCount);
                }

                for (int i = 4; i < 13; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "A" + i }, "users/1");
                        await session.StoreAsync(new User { Name = "B" + i }, "users/2");
                        await session.StoreAsync(new Order { Employee = "C" + i }, "orders/1");
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/1", 0, 1000).Result.Count;
                    Assert.Equal(10, revisionCount);
                    revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/2", 0, 1000).Result.Count;
                    Assert.Equal(10, revisionCount);
                    revisionCount = session.Advanced.Revisions.GetForAsync<Order>("orders/1", 0, 1000).Result.Count;
                    Assert.Equal(33, revisionCount);
                }
                WaitForUserToContinueTheTest(store);
            }
        }

        [Fact]
        public async Task limitRevisionDeletionWithEnforceConfiguration()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 1000000
                    },
                };
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Toli" }, "users/1");
                    await session.StoreAsync(new User { Name = "Mitzi" }, "users/2");
                    await session.StoreAsync(new Order { Employee = "Daniel" }, "orders/1");
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 500; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Toli" + i }, "users/1");
                        await session.StoreAsync(new User { Name = "Mitzi" + i }, "users/2");
                        await session.StoreAsync(new Order { Employee = "Daniel" + i}, "orders/1");
                        await session.SaveChangesAsync();
                    }
                }
                for (int i = 500; i < 630; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Mitzi" + i }, "users/2");
                        await session.SaveChangesAsync();
                    }
                }
                using (var session = store.OpenAsyncSession())
                {

                    var revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/1", 0, 1000).Result.Count;
                    Assert.Equal(501, revisionCount);
                    revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/2", 0, 1000).Result.Count;
                    Assert.Equal(631, revisionCount);
                    revisionCount = session.Advanced.Revisions.GetForAsync<User>("orders/1", 0, 1000).Result.Count;
                    Assert.Equal(501, revisionCount);
                }

                configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 10,
                        MaximumRevisionsToDeleteUponDocumentUpdate = 100
                    },
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        ["Orders"] = new RevisionsCollectionConfiguration
                        {
                            Disabled = false,
                            MinimumRevisionsToKeep = 33,
                            MaximumRevisionsToDeleteUponDocumentUpdate = 35
                        }
                    }

                };
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

                var db = await GetDocumentDatabaseInstanceFor(store);
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                    await db.DocumentsStorage.RevisionsStorage.EnforceConfiguration(_ => { }, token);
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenAsyncSession())
                {
                    var revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/1", 0, 100).Result.Count;
                    Assert.Equal(10, revisionCount);
                    revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/2", 0, 100).Result.Count;
                    Assert.Equal(10, revisionCount);
                    revisionCount = session.Advanced.Revisions.GetForAsync<Order>("orders/1", 0, 100).Result.Count;
                    Assert.Equal(33, revisionCount);
                }
            }
        }
    }
}
