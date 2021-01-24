using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Issues;
using Sparrow;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;
using Company = Raven.Tests.Core.Utils.Entities.Company;
using Employee = Raven.Tests.Core.Utils.Entities.Employee;

namespace SlowTests.Smuggler
{
    public class SmugglerApiTests : RavenTestBase
    {
        public SmugglerApiTests(ITestOutputHelper output) : base(output)
        {
        }

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
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_1"
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_2"
            }))
            {
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1" });
                    await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2" });
                    await session.SaveChangesAsync();
                }

                var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), store2.Smuggler);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                using (var commands = store2.Commands())
                {
                    var docs = await commands.GetAsync(0, 10);
                    Assert.Equal(3, docs.Count());
                }
            }
        }

        [Fact]
        public async Task CanExportAndImport()
        {
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_1"
                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_2"
                }))
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

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
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
        public async Task CanExportAndImportEncrypted()
        {
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_1"
                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_2"
                }))
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

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
                    {
                        EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs="
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions
                    {
                        EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs="
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
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
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_1"
                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_2"
                }))
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

                    var exportOperation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    var exportResult = (SmugglerResult)exportOperation.WaitForCompletion();

                    var stats = await store1.Maintenance.SendAsync(new GetStatisticsOperation());
                    var progress = (SmugglerResult.SmugglerProgress)exportResult.Progress;

                    Assert.Equal(stats.CountOfDocuments, progress.Documents.ReadCount);
                    Assert.Equal(stats.CountOfIndexes, progress.Indexes.ReadCount);

                    var importOperation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    var importResult = (SmugglerResult)importOperation.WaitForCompletion();

                    stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
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
            var file = GetTempFileName();
            try
            {
                using (var exportStore = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_exportStore"
                }))
                {
                    var database = await GetDocumentDatabaseInstanceFor(exportStore);

                    using (var session = exportStore.OpenAsyncSession())
                    {
                        await SetupExpiration(exportStore);
                        var person1 = new Person { Name = "Name1" };
                        await session.StoreAsync(person1).ConfigureAwait(false);
                        var metadata = session.Advanced.GetMetadataFor(person1);
                        metadata[Constants.Documents.Metadata.Expires] = database.Time.GetUtcNow().AddSeconds(10).ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);

                        await session.SaveChangesAsync().ConfigureAwait(false);
                    }

                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddSeconds(11);

                    var operation = await exportStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions { IncludeExpired = false }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                }

                using (var importStore = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_importStore"
                }))
                {
                    var operation = await importStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
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
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1"
                }))
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

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store1.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(4, stats.CountOfDocuments);
                    Assert.Equal(8, stats.CountOfRevisionDocuments);
                }

                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store2"
                }))
                {
                    var operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
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
        public async Task CanExportAndImportWithRevisionDocumentsFromCollection()
        {
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1"
                }))
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

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
                    {
                        Collections = new List<string>() { "Companies" }
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store1.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(5, stats.CountOfDocuments);
                    Assert.Equal(7, stats.CountOfRevisionDocuments);
                }

                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store2"
                }))
                {
                    var operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                    {
                        SkipRevisionCreation = true
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                    WaitForUserToContinueTheTest(store2);
                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(3, stats.CountOfRevisionDocuments);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task ImportCountersWithoutDocuments()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), Guid.NewGuid().ToString());
            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                        await session.StoreAsync(new User { Name = "Name2" }, "users/2");

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        session.CountersFor("users/1").Increment("likes", 100);
                        session.CountersFor("users/1").Increment("dislikes", 200);
                        session.CountersFor("users/2").Increment("downloads", 500);

                        await session.SaveChangesAsync();
                    }
                    var stats = await store1.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(2, stats.CountOfCounterEntries);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
                    {
                        OperateOnTypes = DatabaseItemType.CounterGroups | DatabaseItemType.DatabaseRecord
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(0, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfCounterEntries);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task ImportRevisionDocumentsWithoutDocuments()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), Guid.NewGuid().ToString());
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1"
                }))
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
                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
                    {
                        OperateOnTypes = DatabaseItemType.RevisionDocuments | DatabaseItemType.DatabaseRecord
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store1.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(4, stats.CountOfDocuments);
                    Assert.Equal(8, stats.CountOfRevisionDocuments);
                }

                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store2"
                }))
                {
                    var operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(0, stats.CountOfDocuments);
                    Assert.Equal(10, stats.CountOfRevisionDocuments);
                    using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    {
                        var command = new GetRevisionsBinEntryCommand(long.MaxValue, 5);
                        await store2.GetRequestExecutor().ExecuteAsync(command, context);
                        Assert.Equal(3, command.Result.Results.Length);
                    }
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
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1"
                }))
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

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store1.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(4, stats.CountOfDocuments);
                    Assert.Equal(8, stats.CountOfRevisionDocuments);

                    using (var store2 = GetDocumentStore(new Options
                    {
                        ModifyDatabaseName = s => $"{s}_store2"
                    }))
                    {
                        operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                        {
                            SkipRevisionCreation = true
                        }, file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                        Assert.Equal(4, stats.CountOfDocuments);
                        Assert.Equal(8, stats.CountOfRevisionDocuments);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task CanExportAndImportCounters()
        {
            var file = GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                        await session.StoreAsync(new User { Name = "Name2" }, "users/2");

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        session.CountersFor("users/1").Increment("likes", 100);
                        session.CountersFor("users/1").Increment("dislikes", 200);
                        session.CountersFor("users/2").Increment("downloads", 500);

                        await session.SaveChangesAsync();
                    }

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(2, stats.CountOfCounterEntries);

                    using (var session = store2.OpenAsyncSession())
                    {
                        var user1 = await session.LoadAsync<User>("users/1");
                        var user2 = await session.LoadAsync<User>("users/2");

                        Assert.Equal("Name1", user1.Name);
                        Assert.Equal("Name2", user2.Name);

                        var dic = await session.CountersFor(user1).GetAllAsync();
                        Assert.Equal(2, dic.Count);
                        Assert.Equal(100, dic["likes"]);
                        Assert.Equal(200, dic["dislikes"]);

                        var val = await session.CountersFor(user2).GetAsync("downloads");
                        Assert.Equal(500, val);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task CanImportLegacyCounters()
        {
            var assembly = typeof(SmugglerApiTests).Assembly;

            using (var fs = assembly.GetManifestResourceStream("SlowTests.Data.legacy-counters.4.1.5.ravendbdump"))
            using (var store = GetDocumentStore())
            {
                var options = new DatabaseSmugglerImportOptions();
                options.OperateOnTypes &= ~DatabaseItemType.CounterGroups;

#pragma warning disable 618
                options.OperateOnTypes |= DatabaseItemType.Counters;
#pragma warning restore 618

                var operation = await store.Smuggler.ImportAsync(options, fs);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());

                Assert.Equal(1059, stats.CountOfDocuments);
                Assert.Equal(3, stats.CountOfIndexes);
                Assert.Equal(4645, stats.CountOfRevisionDocuments);
                Assert.Equal(17, stats.CountOfAttachments);

                Assert.Equal(29, stats.CountOfCounterEntries);

                using (var session = store.OpenSession())
                {
                    var q = session.Query<Supplier>().ToList();
                    Assert.Equal(29, q.Count);

                    foreach (var supplier in q)
                    {
                        var counters = session.CountersFor(supplier).GetAll();
                        Assert.Equal(1, counters.Count);
                        Assert.Equal(10, counters["likes"]);
                    }
                }
            }
        }

        [Fact]
        public async Task ShouldAvoidCreatingNewRevisionsDuringImport()
        {
            var file = GetTempFileName();

            try
            {
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1"
                }))
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

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store1.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(4, stats.CountOfDocuments);
                    Assert.Equal(8, stats.CountOfRevisionDocuments);
                }

                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store2"
                }))
                {
                    var operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions
                    {
                        SkipRevisionCreation = true
                    }, file);

                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(4, stats.CountOfDocuments);
                    Assert.Equal(8, stats.CountOfRevisionDocuments);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task ExportShouldSkipDeadSegments()
        {
            var file = GetTempFileName();
            var baseline = DateTime.Today;

            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1" }, "users/1");

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        for (int i = 0; i < 360; i++)
                        {
                            session.TimeSeriesFor("users/1", "Heartrate").Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/1");
                        }
                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        session.TimeSeriesFor("users/1", "Heartrate").Delete();
                        await session.SaveChangesAsync();
                    }

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfTimeSeriesSegments);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task CanExportAndImportTimeSeriesWithRollups()
        {
            var file = GetTempFileName();
            var baseline = DateTime.Now.AddMinutes(-5);
            baseline = new DateTime(baseline.Year, baseline.Month, baseline.Day, baseline.Hour, baseline.Minute, 0, baseline.Kind);

            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    var config = new TimeSeriesConfiguration
                    {
                        Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                        {
                            ["users"] = new TimeSeriesCollectionConfiguration
                            {
                                Policies = new List<TimeSeriesPolicy> { new TimeSeriesPolicy("EveryMinute", TimeValue.FromMinutes(1)) }
                            }
                        }
                    };

                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.TimeSeriesFor("users/1", "Heartrate").Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/1");
                        }

                        await session.SaveChangesAsync();
                    }
                    var db = await GetDocumentDatabaseInstanceFor(store1);
                    await store1.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                    using (var session = store1.OpenAsyncSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.TimeSeriesFor("users/1", "Heartrate").Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/1");
                        }

                        await session.SaveChangesAsync();
                    }

                    await db.TimeSeriesPolicyRunner.HandleChanges();
                    var total = await db.TimeSeriesPolicyRunner.RunRollups();
                    Assert.True(1 == total, $"actual {total}, baseline:{baseline} ({baseline.Ticks}, {baseline.Kind}), now:{db.Time.GetUtcNow()} ({db.Time.GetUtcNow().Ticks})");

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    var exportResult = await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1)) as SmugglerResult;
                    Assert.NotNull(exportResult);
                    Assert.Equal(12, exportResult.TimeSeries.ReadCount);

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    var importResult = await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1)) as SmugglerResult;
                    Assert.NotNull(importResult);
                    Assert.Equal(12, importResult.TimeSeries.ReadCount);

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(2, stats.CountOfTimeSeriesSegments);

                    using (var session = store2.OpenAsyncSession())
                    {
                        var user1 = await session.LoadAsync<User>("users/1");
                        Assert.Equal("Name1", user1.Name);

                        var values = await session.TimeSeriesFor("users/1", "Heartrate").GetAsync();

                        var count = 0;
                        foreach (var val in values)
                        {
                            Assert.Equal(baseline.AddSeconds(count * 10), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(1, val.Values.Length);
                            Assert.Equal(count++ % 60, val.Values[0]);
                        }

                        Assert.Equal(10, count);

                        values = await session.TimeSeriesFor("users/1", "Heartrate@EveryMinute").GetAsync();
                        Assert.Equal(2, values.Length);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task CanExportAndImportTimeSeries()
        {
            var file = GetTempFileName();
            var baseline = DateTime.Today;

            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                        await session.StoreAsync(new User { Name = "Name2" }, "users/2");

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        for (int i = 0; i < 360; i++)
                        {
                            session.TimeSeriesFor("users/1", "Heartrate").Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/1");
                            session.TimeSeriesFor("users/2", "Heartrate").Append(baseline.AddSeconds(i * 10), new[] { i % 60d, i % 60d + 5 }, "watches/2");
                            session.TimeSeriesFor("users/1", "Heartrate2").Append(baseline.AddSeconds(i * 10), new[] { i % 60d, i % 60d + 5, i % 60d + 10 }, "watches/3");
                        }

                        await session.SaveChangesAsync();
                    }

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(4, stats.CountOfTimeSeriesSegments);

                    using (var session = store2.OpenAsyncSession())
                    {
                        var user1 = await session.LoadAsync<User>("users/1");
                        var user2 = await session.LoadAsync<User>("users/2");

                        Assert.Equal("Name1", user1.Name);
                        Assert.Equal("Name2", user2.Name);

                        var values = await session.TimeSeriesFor("users/1", "Heartrate").GetAsync(DateTime.MinValue, DateTime.MaxValue);

                        var count = 0;
                        foreach (var val in values)
                        {
                            Assert.Equal(baseline.AddSeconds(count * 10), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(1, val.Values.Length);
                            Assert.Equal(count++ % 60, val.Values[0]);
                        }

                        Assert.Equal(360, count);

                        values = await session.TimeSeriesFor("users/2", "Heartrate").GetAsync(DateTime.MinValue, DateTime.MaxValue);

                        count = 0;
                        foreach (var val in values)
                        {
                            Assert.Equal(baseline.AddSeconds(count * 10), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(2, val.Values.Length);
                            Assert.Equal(count % 60, val.Values[0]);
                            Assert.Equal(count++ % 60 + 5, val.Values[1]);
                        }

                        Assert.Equal(360, count);

                        values = await session.TimeSeriesFor("users/1", "Heartrate2").GetAsync(DateTime.MinValue, DateTime.MaxValue);

                        count = 0;
                        foreach (var val in values)
                        {
                            Assert.Equal(baseline.AddSeconds(count * 10), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(3, val.Values.Length);
                            Assert.Equal(count % 60, val.Values[0]);
                            Assert.Equal(count % 60 + 5, val.Values[1]);
                            Assert.Equal(count++ % 60 + 10, val.Values[2]);
                        }

                        Assert.Equal(360, count);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task CanExportAndImportTimeSeriesWithMultipleSegments()
        {
            var file = GetTempFileName();
            var baseline = DateTime.Today;

            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                        await session.StoreAsync(new User { Name = "Name2" }, "users/2");

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        for (int i = 0; i < 360; i++)
                        {
                            session.TimeSeriesFor("users/1", "Heartrate").Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/1");
                        }

                        for (int i = 0; i < 360; i++)
                        {
                            session.TimeSeriesFor("users/1", "Heartrate").Append(baseline.AddMonths(3).AddSeconds(i * 10), new[] { i % 60d }, "watches/2");
                        }

                        for (int i = 0; i < 360; i++)
                        {
                            session.TimeSeriesFor("users/1", "Heartrate").Append(baseline.AddMonths(6).AddSeconds(i * 10), new[] { i % 60d }, "watches/3");
                        }

                        await session.SaveChangesAsync();
                    }

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(3, stats.CountOfTimeSeriesSegments);

                    using (var session = store2.OpenAsyncSession())
                    {
                        var user1 = await session.LoadAsync<User>("users/1");
                        var user2 = await session.LoadAsync<User>("users/2");

                        Assert.Equal("Name1", user1.Name);
                        Assert.Equal("Name2", user2.Name);

                        var values = (await session.TimeSeriesFor("users/1", "Heartrate").GetAsync(DateTime.MinValue, DateTime.MaxValue)).ToList();

                        Assert.Equal(360 * 3, values.Count);

                        for (int j = 0; j < 3; j++)
                        {
                            for (var i = 0; i < 360; i++)
                            {
                                var val = values[j * 360 + i];
                                Assert.Equal(baseline.AddMonths(j * 3).AddSeconds(i * 10), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                                Assert.Equal(i % 60, val.Values[0]);
                            }
                        }
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task CanSkipTimeSeriesOnExport()
        {
            var file = GetTempFileName();
            var baseline = DateTime.Today;

            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                        await session.StoreAsync(new User { Name = "Name2" }, "users/2");

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        for (int i = 0; i < 360; i++)
                        {
                            session.TimeSeriesFor("users/1", "Heartrate").Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/1");
                            session.TimeSeriesFor("users/2", "Heartrate").Append(baseline.AddSeconds(i * 10), new[] { i % 60d, i % 60d + 5 }, "watches/2");
                        }

                        await session.SaveChangesAsync();
                    }

                    // export just documents without timeseries,

                    var exportOptions = new DatabaseSmugglerExportOptions();
                    exportOptions.OperateOnTypes &= ~DatabaseItemType.TimeSeries;

                    var operation = await store1.Smuggler.ExportAsync(exportOptions, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfTimeSeriesSegments);

                    using (var session = store2.OpenAsyncSession())
                    {
                        var user1 = await session.LoadAsync<User>("users/1");
                        var user2 = await session.LoadAsync<User>("users/2");

                        Assert.Equal("Name1", user1.Name);
                        Assert.Equal("Name2", user2.Name);

                        // verify that the documents don't have
                        // timeseries names in their metadata

                        var tsNames = session.Advanced.GetTimeSeriesFor(user1);
                        Assert.Empty(tsNames);

                        tsNames = session.Advanced.GetTimeSeriesFor(user2);
                        Assert.Empty(tsNames);

                        var values = await session.TimeSeriesFor(user1, "Heartrate")
                            .GetAsync(DateTime.MinValue, DateTime.MaxValue);

                        Assert.Null(values);

                        values = await session.TimeSeriesFor(user2, "Heartrate")
                            .GetAsync(DateTime.MinValue, DateTime.MaxValue);

                        Assert.Null(values);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task CanSkipTimeSeriesOnImport()
        {
            var file = GetTempFileName();
            var baseline = DateTime.Today;

            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                        await session.StoreAsync(new User { Name = "Name2" }, "users/2");

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        for (int i = 0; i < 360; i++)
                        {
                            session.TimeSeriesFor("users/1", "Heartrate").Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/1");
                            session.TimeSeriesFor("users/2", "Heartrate").Append(baseline.AddSeconds(i * 10), new[] { i % 60d, i % 60d + 5 }, "watches/2");
                        }

                        await session.SaveChangesAsync();
                    }

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    // import just documents without timeseries,
                    // verify that the documents don't have timeseries names in their metadata

                    var importOptions = new DatabaseSmugglerImportOptions();
                    importOptions.OperateOnTypes &= ~DatabaseItemType.TimeSeries;

                    operation = await store2.Smuggler.ImportAsync(importOptions, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfTimeSeriesSegments);

                    using (var session = store2.OpenAsyncSession())
                    {
                        var user1 = await session.LoadAsync<User>("users/1");
                        var user2 = await session.LoadAsync<User>("users/2");

                        Assert.Equal("Name1", user1.Name);
                        Assert.Equal("Name2", user2.Name);

                        var tsNames = session.Advanced.GetTimeSeriesFor(user1);
                        Assert.Empty(tsNames);

                        tsNames = session.Advanced.GetTimeSeriesFor(user2);
                        Assert.Empty(tsNames);

                        var values = await session.TimeSeriesFor(user1, "Heartrate")
                            .GetAsync(DateTime.MinValue, DateTime.MaxValue);

                        Assert.Null(values);

                        values = await session.TimeSeriesFor(user2, "Heartrate")
                            .GetAsync(DateTime.MinValue, DateTime.MaxValue);

                        Assert.Null(values);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task Timeseries_export_should_respect_collection_selection_1()
        {
            var file = GetTempFileName();
            try
            {
                var baseline = DateTime.Today;
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), "users/1");
                        await session.StoreAsync(new User(), "users/2");
                        await session.StoreAsync(new User(), "users/3");

                        await session.StoreAsync(new Order(), "orders/1");
                        await session.StoreAsync(new Order(), "orders/2");

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        session.TimeSeriesFor("users/1", "Heartrate").Append(baseline, new[] { 72d }, "watches/1");
                        session.TimeSeriesFor("users/1", "Heartrate").Append(baseline.AddMinutes(1), new[] { 72d }, "watches/1");
                        session.TimeSeriesFor("users/2", "Heartrate").Append(baseline, new[] { 70d }, "watches/1");
                        session.TimeSeriesFor("users/3", "Heartrate").Append(baseline, new[] { 75d }, "watches/1");

                        await session.SaveChangesAsync();
                    }

                    var exportOptions = new DatabaseSmugglerExportOptions
                    {
                        Collections = new List<string>
                        {
                            "Orders"
                        }
                    };

                    var operation = await store1.Smuggler.ExportAsync(exportOptions, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfTimeSeriesSegments);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task Timeseries_export_should_respect_collection_selection_2()
        {
            var file = GetTempFileName();
            try
            {
                var baseline = DateTime.Today;
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), "users/1");
                        await session.StoreAsync(new User(), "users/2");
                        await session.StoreAsync(new User(), "users/3");

                        await session.StoreAsync(new Order(), "orders/1");
                        await session.StoreAsync(new Order(), "orders/2");

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        session.TimeSeriesFor("users/1", "Heartrate").Append(baseline, new[] { 72d }, "watches/1");
                        session.TimeSeriesFor("users/1", "Heartrate").Append(baseline.AddMinutes(1), new[] { 72d }, "watches/2");
                        session.TimeSeriesFor("users/2", "Heartrate").Append(baseline, new[] { 70d }, "watches/1");
                        session.TimeSeriesFor("users/3", "Heartrate").Append(baseline, new[] { 75d }, "watches/1");

                        session.TimeSeriesFor("orders/1", "Heartrate").Append(baseline, new[] { 72d }, "watches/1");
                        session.TimeSeriesFor("orders/2", "Heartrate").Append(baseline, new[] { 70d, 67d }, "watches/2");

                        await session.SaveChangesAsync();
                    }

                    var exportOptions = new DatabaseSmugglerExportOptions
                    {
                        Collections = new List<string>
                        {
                            "Orders"
                        }
                    };

                    var operation = await store1.Smuggler.ExportAsync(exportOptions, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(2, stats.CountOfTimeSeriesSegments);

                    using (var session = store2.OpenSession())
                    {
                        var order = session.Load<Order>("orders/1");
                        var tsNames = session.Advanced.GetTimeSeriesFor(order);
                        Assert.Equal(1, tsNames.Count);
                        Assert.Equal("Heartrate", tsNames[0]);

                        var values = session.TimeSeriesFor(order, "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                        Assert.Equal(1, values.Count);
                        Assert.Equal(1, values[0].Values.Length);
                        Assert.Equal(72d, values[0].Values[0]);
                        Assert.Equal(baseline, values[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal("watches/1", values[0].Tag);

                        order = session.Load<Order>("orders/2");
                        tsNames = session.Advanced.GetTimeSeriesFor(order);
                        Assert.Equal(1, tsNames.Count);
                        Assert.Equal("Heartrate", tsNames[0]);

                        values = session.TimeSeriesFor(order, "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                        Assert.Equal(1, values.Count);
                        Assert.Equal(2, values[0].Values.Length);
                        Assert.Equal(70d, values[0].Values[0]);
                        Assert.Equal(67d, values[0].Values[1]);
                        Assert.Equal(baseline, values[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal("watches/2", values[0].Tag);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        private async Task SetupExpiration(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                var config = new ExpirationConfiguration
                {
                    Disabled = false,
                    DeleteFrequencyInSec = 100,
                };

                await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config);

                await session.SaveChangesAsync();
            }
        }

        // Smuggler Export and Import need to work with ForDatabase method when store database name is null
        [Fact]
        public async Task Smuggler_Export_And_Import_Should_Work_With_ForDatabase()
        {
            using (var server = GetNewServer())
            {
                using (var store = new DocumentStore
                {
                    Urls = new[] { server.WebUrl }
                }.Initialize())
                {
                    var createSrcDatabase = new CreateDatabaseOperation(new DatabaseRecord("SrcDatabase"));
                    await store.Maintenance.Server.SendAsync(createSrcDatabase);

                    var createDestDatabase = new CreateDatabaseOperation(new DatabaseRecord("DestDatabase"));
                    await store.Maintenance.Server.SendAsync(createDestDatabase);

                    const int documentCount = 10000;
                    using (var session = store.OpenAsyncSession("SrcDatabase"))
                    {
                        for (var i = 0; i < documentCount; i++)
                        {
                            var user = new User { Name = $"User {i}" };
                            await session.StoreAsync(user);
                        }

                        await session.SaveChangesAsync();
                    }

                    var exportOptions = new DatabaseSmugglerExportOptions
                    {
                        OperateOnTypes = DatabaseItemType.Documents
                    };
                    var destination = store.Smuggler.ForDatabase("DestDatabase");
                    var operation = await store.Smuggler.ForDatabase("SrcDatabase").ExportAsync(exportOptions, destination);
                    await operation.WaitForCompletionAsync();

                    var stats = await store.Maintenance.ForDatabase("DestDatabase").SendAsync(new GetStatisticsOperation());
                    Assert.True(stats.CountOfDocuments >= documentCount);

                    await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord("ImportDest")));

                    using (var stream = GetDump("RavenDB_11664.1.ravendbdump"))
                    {
                        operation = await store.Smuggler.ForDatabase("ImportDest").ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                    }

                    using (var session = store.OpenAsyncSession("ImportDest"))
                    {
                        var employee = await session.LoadAsync<Employee>("employees/9-A");
                        Assert.NotNull(employee);
                    }
                }
            }
        }

        [Fact]
        public async Task Keep_The_Same_Document_Id_After_Counters_Import()
        {
            var file = GetTempFileName();

            try
            {
                using (var store = GetDocumentStore())
                {
                    const string userId = "Users/1-A";
                    using (var session = store.OpenSession())
                    {
                        var user = new User { Name = "Grisha" };
                        session.Store(user, userId);
                        session.CountersFor(user).Increment("Likes", 1);
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        var user = session.Load<User>(userId);
                        Assert.Equal(userId, user.Id);
                    }

                    var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                    using (var innerStore = GetDocumentStore())
                    {
                        operation = await innerStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                        using (var session = innerStore.OpenSession())
                        {
                            var user = session.Load<User>(userId);
                            Assert.Equal(userId, user.Id);
                        }
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task Smugller_WhenContainRevisionWithoutConfiguration_ShouldExportImportRevisions()
        {
            using var src = GetDocumentStore();
            using var dest = GetDocumentStore();

            var user = new User();

            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(user);
                await session.SaveChangesAsync();

                session.Advanced.Revisions.ForceRevisionCreationFor(user.Id);
                await session.SaveChangesAsync();
            }

            var operation = await src.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), dest.Smuggler);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            using (var session = dest.OpenAsyncSession())
            {
                var revision = await session.Advanced.Revisions.GetForAsync<User>(user.Id);
                Assert.NotNull(revision);
                Assert.NotEmpty(revision);
            }
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_9912).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }
    }
}
