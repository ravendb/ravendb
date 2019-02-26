using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12477 : RavenTestBase
    {
        [Fact]
        public async Task Can_handle_delete_revision_of_doc_that_changed_collection()
        {
            using (var store = GetDocumentStore())
            {
                // setup revision with PurgeOnDelete = false
                var index = await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration()
                });

                var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                await documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(index, Server.ServerStore.Engine.OperationTimeout);

                // store a document "users/1" under 'Users' collection
                // and create some revisions for it
                using (var session = store.OpenAsyncSession())
                {
                    var user = new User { Name = "Aviv " };
                    await session.StoreAsync(user, "users/1");
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 9; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>("users/1");
                        user.Name += i;
                        await session.StoreAsync(user);
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(10, revisions.Count);
                }

                // delete document "users/1"
                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(11, revisions.Count);
                }

                // store a new document with the same id - "users/1"
                // but under 'Companies' collection
                using (var session = store.OpenAsyncSession())
                {
                    var company = new Company { Name = "HR " };
                    await session.StoreAsync(company, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<object>("users/1");
                    Assert.Equal(12, revisions.Count);
                }

                // enable PurgeOnDelete
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        PurgeOnDelete = true
                    }
                };

                index = await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration);
                await documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(index, Server.ServerStore.Engine.OperationTimeout);

                // make sure we don't have a tombstone for "users/1"
                await documentDatabase.TombstoneCleaner.ExecuteCleanup();

                // delete document "users/1"
                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                // all the revisions for "users/1" should be deleted - 
                // the old ones ('Users' collection) as well as the new ones ('Companies' collection)
                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<object>("users/1");

                    Assert.Empty(revisions);
                }
            }
        }
    }
}
