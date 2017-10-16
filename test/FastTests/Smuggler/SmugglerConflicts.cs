using System.IO;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Smuggler
{
    public class SmugglerConflicts : ReplicationTestBase
    {
        [Fact]
        public async Task CanExportAndImportWithConflicts()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1"
                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store2"
                }))
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Fitzchak 1", Id = "users/fitzchak"});
                        await session.StoreAsync(new Person { Name = "Name1" });
                        await session.StoreAsync(new Person { Name = "Name2" });
                        await session.StoreAsync(new Company { Name = "Hibernating Rhinos " });
                        await session.SaveChangesAsync();
                    }

                    using (var session = store2.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Fitzchak 2", Id = "users/fitzchak"});
                        await session.StoreAsync(new Person { Name = "Name11" });
                        await session.StoreAsync(new Person { Name = "Name12" });
                        await session.StoreAsync(new Company { Name = "Hibernating Rhinos 2 " });
                        await session.SaveChangesAsync();
                    }

                    for (int i = 0; i < 2; i++)
                    {
                        using (var session = store1.OpenAsyncSession())
                        {
                            var company = await session.LoadAsync<Company>("companies/1-A");
                            var person = await session.LoadAsync<Person>("people/1-A");
                            company.Name += " update " + i;
                            person.Name += " update " + i;
                            await session.StoreAsync(company);
                            await session.StoreAsync(person);
                            await session.SaveChangesAsync();
                        }
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        var person = await session.LoadAsync<Person>("people/2-A");
                        Assert.NotNull(person);
                        session.Delete(person);
                        await session.SaveChangesAsync();
                    }

                    await SetupReplicationAsync(store2, store1);

                    Assert.Equal(2, WaitUntilHasConflict(store1, "users/fitzchak").Length);

                    await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);

                    var stats = await store1.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(4, stats.CountOfConflicts);

                    stats = await store2.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(6, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfConflicts);

                    // Assert import to an existing database
                    await AssertImport(store1, file);

                    // Assert import to a database that has the same document.
                    // We'll delete the document but create the same conflict.
                    await AssertImport(store2, file);

                   /* using (var session = store2.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>("users/fitzchak");
                        user.LastName = "Update to generate another conflict.";
                        await session.SaveChangesAsync();
                    }

                    // Assert import to a database that has a different document.
                    // We'll delete the document but create create another conflict for it.
                    await AssertImport(store2, file);*/
                }

                // Assert import to a new database
                using (var store3 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store3"
                }))
                {
                    await AssertImport(store3, file);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        private async Task AssertImport(DocumentStore store, string file)
        {
            for (int i = 0; i < 3; i++)
            {
                await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);

                var stats = await store.Admin.SendAsync(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfDocuments);
                Assert.Equal(4, stats.CountOfConflicts);
            }
        }
    }
}
