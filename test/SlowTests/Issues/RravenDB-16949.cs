using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
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
                    await session.StoreAsync(new User {Name = "Toli"}, "users/1");
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 500; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Toli" + i }, "users/1");
                        await session.SaveChangesAsync();
                    }
                }
                using (var session = store.OpenAsyncSession())
                {

                    var revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/1", 0, 1000).Result.Count;
                    Assert.Equal(501, revisionCount);
                }

                configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 10,
                        MaxRevisionsToDeleteUponDocumentUpdate = 100
                    },
                };
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Toli" }, "users/1");
                    await session.SaveChangesAsync();

                    var revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/1", 0, 1000).Result.Count;
                    Assert.Equal(402, revisionCount);
                }

                for (int i = 0; i < 4; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Toli" + i }, "users/1");
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/1", 0, 1000).Result.Count;
                    Assert.Equal(10, revisionCount);
                }
            }
        }

        [Fact]
        public async Task DoNotlimitRevisionDeletionWithEnforceConfiguration()
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
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 500; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Toli" + i }, "users/1");
                        await session.SaveChangesAsync();
                    }
                }
                using (var session = store.OpenAsyncSession())
                {

                    var revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/1", 0, 1000).Result.Count;
                    Assert.Equal(501, revisionCount);
                }

                configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 10,
                        MaxRevisionsToDeleteUponDocumentUpdate = 100
                    },

                };
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

                var db = await GetDocumentDatabaseInstanceFor(store);
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                    await db.DocumentsStorage.RevisionsStorage.EnforceConfiguration(_ => { }, token);

                using (var session = store.OpenAsyncSession())
                {
                    var revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/1", 0, 100).Result.Count;
                    Assert.Equal(10, revisionCount);
                }
            }
        }
    }
}
