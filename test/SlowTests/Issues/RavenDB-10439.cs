using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10439 : RavenTestBase
    {
        [Fact]
        public async Task CanCreateDocumentAfterTombstoneCleaner()
        {
            var path = NewDataPath();
            using (var documentStore = GetDocumentStore(new Options
            {
                Path = path
            }))
            {
                var docId = "users/1";
                using (var session = documentStore.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Grishush"
                    }, docId);
                    await session.SaveChangesAsync();
                }

                using (var session = documentStore.OpenAsyncSession())
                {
                    session.Delete(docId);
                    await session.SaveChangesAsync();
                }

                // run the tombstone cleaner
                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(documentStore.Database);
                await database.TombstoneCleaner.ExecuteCleanup();

                // unload the database
                (await Server.ServerStore.DatabasesLandlord.UnloadAndLockDatabase(documentStore.Database, "reloading database in test")).Dispose();

                using (var session = documentStore.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmelush"
                    }, "users/2");
                    await session.SaveChangesAsync();
                }
            }
        }

        [Fact]
        public async Task CanCreateDocumentAfterTombstoneCleanerAndDeletingTheDocumentTwice()
        {
            var path = NewDataPath();
            using (var documentStore = GetDocumentStore(new Options
            {
                Path = path
            }))
            {
                var docId = "users/1";
                using (var session = documentStore.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Grishush"
                    }, docId);
                    await session.SaveChangesAsync();
                }

                using (var session = documentStore.OpenAsyncSession())
                {
                    session.Delete(docId);
                    await session.SaveChangesAsync();
                }

                using (var session = documentStore.OpenAsyncSession())
                {
                    session.Delete(docId);
                    await session.SaveChangesAsync();
                }

                // run the tombstone cleaner
                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(documentStore.Database);
                await database.TombstoneCleaner.ExecuteCleanup();

                // unload the database
                (await Server.ServerStore.DatabasesLandlord.UnloadAndLockDatabase(documentStore.Database, "reloading database in test")).Dispose();

                using (var session = documentStore.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmelush"
                    }, "users/2");
                    await session.SaveChangesAsync();
                }
            }
        }

        private class User
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }
    }
}
