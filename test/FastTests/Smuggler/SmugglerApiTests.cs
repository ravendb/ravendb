using System.IO;
using System.Threading.Tasks;
using Raven.Client.Smuggler;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Smuggler
{
    public class SmugglerApiTests : RavenTestBase
    {
        [Fact]
        public async Task CanExportDirectlyToRemote()
        {
            using (var store1 = await GetDocumentStore("store1"))
            using (var store2 = await GetDocumentStore("store2"))
            {
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Name1", LastName = "LastName1"});
                    await session.StoreAsync(new User {Name = "Name2", LastName = "LastName2"});
                    await session.SaveChangesAsync();
                }

                await store1.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), new DatabaseSmugglerRemoteDestination
                {
                    Url = store2.Url,
                    Database = store2.DefaultDatabase,
                });

                var docs = store2.DatabaseCommands.GetDocuments(0, 10);
                Assert.Equal(3, docs.Length);
            }
        }

        [Fact]
        public async Task CanImportDirectlyFromRemote()
        {
            using (var store1 = await GetDocumentStore("store1"))
            using (var store2 = await GetDocumentStore("store2"))
            {
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Name1", LastName = "LastName1"});
                    await session.StoreAsync(new User {Name = "Name2", LastName = "LastName2"});
                    await session.SaveChangesAsync();
                }
                
                await store2.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), new DatabaseSmugglerRemoteDestination
                {
                    Url = store1.Url,
                    Database = store1.DefaultDatabase,
                });

                var docs = store2.DatabaseCommands.GetDocuments(0, 10);
                Assert.Equal(3, docs.Length);
            }
        }

        [Fact]
        public async Task CanExportAndImport()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store1 = await GetDocumentStore("store1"))
                using (var store2 = await GetDocumentStore("store2"))
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User {Name = "Name1", LastName = "LastName1"});
                        await session.StoreAsync(new User {Name = "Name2", LastName = "LastName2"});
                        await session.SaveChangesAsync();
                    }

                    await store1.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), new DatabaseSmugglerFileDestination
                    {
                        File = file,
                    });

                    await store2.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), new DatabaseSmugglerFileDestination
                    {
                        File = file,
                    });

                    var docs = store2.DatabaseCommands.GetDocuments(0, 10);
                    Assert.Equal(3, docs.Length);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }
    }
}