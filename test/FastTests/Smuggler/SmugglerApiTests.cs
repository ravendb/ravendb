using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Documents.Revisions;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Expiration;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Smuggler
{
    public class SmugglerApiTests : RavenTestBase
    {
        private class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from u in users
                               select new
                               {
                                   u.Name
                               };

                Stores.Add(x => x.Name, FieldStorage.Yes);
            }
        }

        [Fact]
        public async Task CanExportDirectlyToRemote()
        {
            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "store1"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "store2"))
            {
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1" });
                    await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2" });
                    await session.SaveChangesAsync();
                }

                await store1.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), store2.Smuggler);

                using (var commands = store2.Commands())
                {
                    var docs = await commands.GetAsync(0, 10);
                    Assert.Equal(3, docs.Length);
                }
            }
        }

        [Fact]
        public async Task CanExportAndImport()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(dbSuffixIdentifier: "store1"))
                using (var store2 = GetDocumentStore(dbSuffixIdentifier: "store2"))
                {
                    using (var session = store1.OpenSession())
                    {
                        // creating auto-indexes
                        session.Query<User>()
                            .Where(x => x.Age > 10)
                            .ToList();

                        session.Query<User>()
                            .GroupBy(x => x.Name)
                            .Select(x => new { Name = x.Key, Count = x.Count() })
                            .ToList();
                    }

                    new Users_ByName().Execute(store1);

                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1" });
                        await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2" });
                        await session.SaveChangesAsync();
                    }

                    await store1.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), file);

                    await store2.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), file);

                    var stats = await store2.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(3, stats.CountOfDocuments);
                    Assert.Equal(3, stats.CountOfIndexes);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task ShouldReturnCorrectSmugglerResult()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(dbSuffixIdentifier: "store1"))
                using (var store2 = GetDocumentStore(dbSuffixIdentifier: "store2"))
                {
                    using (var session = store1.OpenSession())
                    {
                        // creating auto-indexes
                        session.Query<User>()
                            .Where(x => x.Age > 10)
                            .ToList();

                        session.Query<User>()
                            .GroupBy(x => x.Name)
                            .Select(x => new { Name = x.Key, Count = x.Count() })
                            .ToList();
                    }

                    new Users_ByName().Execute(store1);

                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1" });
                        await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2" });
                        await session.SaveChangesAsync();
                    }

                    var exportOperation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), file);
                    var exportResult = (SmugglerResult)exportOperation.WaitForCompletion();

                    var stats = await store1.Admin.SendAsync(new GetStatisticsOperation());
                    var progress = (SmugglerResult.SmugglerProgress)exportResult.Progress;

                    Assert.Equal(stats.CountOfDocuments, progress.Documents.ReadCount);
                    Assert.Equal(stats.CountOfIndexes, progress.Indexes.ReadCount);

                    var importOperation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), file);
                    var importResult = (SmugglerResult)importOperation.WaitForCompletion();

                    stats = await store2.Admin.SendAsync(new GetStatisticsOperation());
                    progress = (SmugglerResult.SmugglerProgress)importResult.Progress;

                    Assert.Equal(stats.CountOfDocuments, progress.Documents.ReadCount);
                    Assert.Equal(stats.CountOfIndexes, progress.Indexes.ReadCount);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task SkipExpiredDocumentWhenExport()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var exportStore = GetDocumentStore(dbSuffixIdentifier: "exportStore"))
                {
                    var database = await GetDocumentDatabaseInstanceFor(exportStore);

                    using (var session = exportStore.OpenAsyncSession())
                    {
                        await SetupExpiration(exportStore);
                        var person1 = new Person { Name = "Name1" };
                        await session.StoreAsync(person1).ConfigureAwait(false);
                        var metadata = session.Advanced.GetMetadataFor(person1);
                        metadata[Constants.Documents.Expiration.ExpirationDate] = database.Time.GetUtcNow().AddSeconds(10).ToString(Default.DateTimeOffsetFormatsToWrite);

                        await session.SaveChangesAsync().ConfigureAwait(false);
                    }

                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddSeconds(11);

                    await exportStore.Smuggler.ExportAsync(new DatabaseSmugglerOptions { IncludeExpired = false }, file).ConfigureAwait(false);

                }

                using (var importStore = GetDocumentStore(dbSuffixIdentifier: "importStore"))
                {
                    await importStore.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), file);
                    using (var session = importStore.OpenAsyncSession())
                    {
                        var person = await session.LoadAsync<Person>("people/1").ConfigureAwait(false);
                        Assert.Null(person);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task CanExportAndImportWithRevisionDocuments()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(dbSuffixIdentifier: "store1"))
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);

                        await session.StoreAsync(new Person {Name = "Name1"});
                        await session.StoreAsync(new Person {Name = "Name2"});
                        await session.StoreAsync(new Company {Name = "Hibernating Rhinos "});
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

                    await store1.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), file);

                    var stats = await store1.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(4, stats.CountOfDocuments);
                    Assert.Equal(8, stats.CountOfRevisionDocuments);
                }

                using (var store2 = GetDocumentStore(dbSuffixIdentifier: "store2"))
                {
                    await RevisionsHelper.SetupRevisions(Server.ServerStore, store2.Database);

                    await store2.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), file);

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

        [Fact]
        public async Task WillNotCreateMoreRevisionsAfterImport()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(dbSuffixIdentifier: "store1"))
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);

                        await session.StoreAsync(new Person { Name = "Name1" });
                        await session.StoreAsync(new Person { Name = "Name2" });
                        await session.StoreAsync(new Company { Name = "Hibernating Rhinos " });
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

                    await store1.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), file);

                    var stats = await store1.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(4, stats.CountOfDocuments);
                    Assert.Equal(8, stats.CountOfRevisionDocuments);

                    await store1.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), file);

                    stats = await store1.Admin.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(4, stats.CountOfDocuments);
                    Assert.Equal(8, stats.CountOfRevisionDocuments);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        private static async Task SetupExpiration(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                var config = new ExpirationConfiguration
                {
                    Active = true,
                    DeleteFrequencyInSec = 100,
                };
                await store.Admin.Server.SendAsync(new ConfigureExpirationOperation(config, store.Database));
                await session.SaveChangesAsync();
            }
        }
    }
}
