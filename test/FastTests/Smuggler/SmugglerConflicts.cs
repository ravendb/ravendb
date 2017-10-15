using System.IO;
using System.Threading.Tasks;
using FastTests.Server.Documents.Revisions;
using FastTests.Server.Replication;
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

                    await SetupReplicationAsync(store1, store2);
                    await SetupReplicationAsync(store2, store1);

                    Assert.Equal(2, WaitUntilHasConflict(store1, "users/fitzchak").Length);
                    Assert.Equal(2, WaitUntilHasConflict(store2, "users/fitzchak").Length);

                    await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);

                    WaitForUserToContinueTheTest(store1);

                    var stats = await store1.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(4, stats.CountOfDocuments);
                    Assert.Equal(8, stats.CountOfRevisionDocuments);
                }

                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store2"
                }))
                {
                    await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database);

                    await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);

                    var stats = await store2.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(4, stats.CountOfDocuments);
                    Assert.Equal(10, stats.CountOfRevisionDocuments);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }
    }
}
