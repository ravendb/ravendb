using System.IO;
using System.Threading.Tasks;
using FastTests.Server.Documents.Versioning;
using Raven.Client.Bundles.Versioning;
using Raven.Client.Smuggler;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using System.Linq;

namespace FastTests.Smuggler
{
    public class SmugglerApiTests : RavenTestBase
    {
        [Fact]
        public async Task CanExportDirectlyToRemote()
        {
            using (var store1 = await GetDocumentStore(dbSuffixIdentifier: "store1"))
            using (var store2 = await GetDocumentStore(dbSuffixIdentifier: "store2"))
            {
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Name1", LastName = "LastName1"});
                    await session.StoreAsync(new User {Name = "Name2", LastName = "LastName2"});
                    await session.SaveChangesAsync();
                }

                await store1.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), store2.Url, store2.DefaultDatabase);

                var docs = await store2.AsyncDatabaseCommands.GetDocumentsAsync(0, 10);
                Assert.Equal(3, docs.Length);
            }
        }

        [Fact]
        public async Task CanExportAndImport()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store1 = await GetDocumentStore(dbSuffixIdentifier: "store1"))
                using (var store2 = await GetDocumentStore(dbSuffixIdentifier: "store2"))
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User {Name = "Name1", LastName = "LastName1"});
                        await session.StoreAsync(new User {Name = "Name2", LastName = "LastName2"});
                        await session.SaveChangesAsync();
                    }

                    await store1.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), file);

                    await store2.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), file);

                    var stats = await store2.AsyncDatabaseCommands.GetStatisticsAsync();
                    Assert.Equal(3, stats.CountOfDocuments);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task CanExportAndImportWithVersioingRevisionDocuments()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store1 = await GetDocumentStore(dbSuffixIdentifier: "store1"))
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await VersioningHelper.SetupVersioning(store1);

                        await session.StoreAsync(new Person {Name = "Name1"});
                        await session.StoreAsync(new Person {Name = "Name2"});
                        await session.StoreAsync(new Company {Name = "Hibernaitng Rhinos "});
                        await session.SaveChangesAsync();
                    }

                    for (int i = 0; i < 2; i++)
                    {
                        using (var session = store1.OpenAsyncSession())
                        {
                            var company = await session.LoadAsync<Company>("companies/1");
                            var person = await session.LoadAsync<Person>("people/1");
                            company.Name +=  " update " + i;
                            person.Name += " update " + i;
                            await session.StoreAsync(company);
                            await session.StoreAsync(person);
                            await session.SaveChangesAsync();
                        }
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        var person = await session.LoadAsync<Person>("people/2");
                        Assert.NotNull(person);
                        session.Delete(person);
                        await session.SaveChangesAsync();
                    }

                    await store1.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), file);

                    var stats = await store1.AsyncDatabaseCommands.GetStatisticsAsync();
                    Assert.Equal(5, stats.CountOfDocuments);
                    Assert.Equal(7, stats.CountOfRevisionDocuments);
                }

                using (var store2 = await GetDocumentStore(dbSuffixIdentifier: "store2"))
                {
                    await store2.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), file);

                    var stats = await store2.AsyncDatabaseCommands.GetStatisticsAsync();
                    Assert.Equal(5, stats.CountOfDocuments);
                    Assert.Equal(7, stats.CountOfRevisionDocuments);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }
    }
}