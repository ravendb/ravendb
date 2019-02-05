using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Issues;
using Sparrow;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Smuggler
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
            var file = Path.GetTempFileName();
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
            var file = Path.GetTempFileName();
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
            var file = Path.GetTempFileName();
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
            var file = Path.GetTempFileName();
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
                    Assert.Equal(3, stats.CountOfCounters);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
                    {
                        OperateOnTypes = DatabaseItemType.Counters | DatabaseItemType.DatabaseRecord

                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(0, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfCounters);

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
                        OperateOnTypes =  DatabaseItemType.RevisionDocuments | DatabaseItemType.DatabaseRecord

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
            var file = Path.GetTempFileName();
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

                    operation = await store1.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    stats = await store1.Maintenance.SendAsync(new GetStatisticsOperation());
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
        public async Task CanExportAndImportCounters()
        {
            var file = Path.GetTempFileName();
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
                    Assert.Equal(3, stats.CountOfCounters);

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
        public async Task CanExportAndImportCounterTombstones()
        {
            var file = Path.GetTempFileName();
            try
            {
                using (var store1 = GetDocumentStore())
                using (var store2 = GetDocumentStore())
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Name1" }, "users/1");
                        await session.StoreAsync(new User { Name = "Name2" }, "users/2");
                        await session.StoreAsync(new User { Name = "Name3" }, "users/3");

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        session.CountersFor("users/1").Increment("likes", 100);
                        session.CountersFor("users/1").Increment("dislikes", 200);
                        session.CountersFor("users/2").Increment("downloads", 500);
                        session.CountersFor("users/2").Increment("votes", 1000);

                        await session.SaveChangesAsync();
                    }

                    using (var session = store1.OpenAsyncSession())
                    {
                        session.Delete("users/3");
                        session.CountersFor("users/1").Delete("dislikes");
                        session.CountersFor("users/2").Delete("votes");
                        await session.SaveChangesAsync();
                    }

                    var db = await GetDocumentDatabaseInstanceFor(store1);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var tombstones = db.DocumentsStorage.GetTombstonesFrom(ctx, 0, 0, int.MaxValue).ToList();
                        Assert.Equal(3, tombstones.Count);
                        Assert.Equal(Tombstone.TombstoneType.Document, tombstones[0].Type);
                        Assert.Equal(Tombstone.TombstoneType.Counter, tombstones[1].Type);
                        Assert.Equal(Tombstone.TombstoneType.Counter, tombstones[2].Type);
                    }

                    var exportOptions = new DatabaseSmugglerExportOptions();
                    var importOptions = new DatabaseSmugglerImportOptions();
                    exportOptions.OperateOnTypes |= DatabaseItemType.Tombstones;
                    importOptions.OperateOnTypes |= DatabaseItemType.Tombstones;

                    var operation = await store1.Smuggler.ExportAsync(exportOptions, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store2.Smuggler.ImportAsync(importOptions, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfCounters);
                    Assert.Equal(3, stats.CountOfTombstones);

                    db = await GetDocumentDatabaseInstanceFor(store2);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var tombstones = db.DocumentsStorage.GetTombstonesFrom(ctx, 0, 0, int.MaxValue).ToList();
                        Assert.Equal(3, tombstones.Count);
                        Assert.Equal(Tombstone.TombstoneType.Document, tombstones[0].Type);
                        Assert.Equal(Tombstone.TombstoneType.Counter, tombstones[1].Type);
                        Assert.Equal(Tombstone.TombstoneType.Counter, tombstones[2].Type);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [Fact]
        public async Task ShouldAvoidCreatingNewRevisionsDuringImport()
        {
            var file = Path.GetTempFileName();
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

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_9912).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }
    }
}
