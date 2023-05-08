using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Smuggler
{
    public class SmugglerConflicts : ReplicationTestBase
    {
        public SmugglerConflicts(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanExportAndImportWithConflicts_ToTheSameDatabase(Options options)
        {
            var file = GetTempFileName();
            try
            {
                var modifyDatabaseRecord = options.ModifyDatabaseRecord;
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1",
                    ModifyDatabaseRecord = record =>
                    {
                        modifyDatabaseRecord(record);
                        record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>() };
                    }
                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store2",
                    ModifyDatabaseRecord = record =>
                    {
                        modifyDatabaseRecord(record);
                        record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>() };
                    }
                }))
                {
                    await GenerateConflict(store1, store2);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    operation = await store1.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    await AssertImport(store1);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanExportAndImportWithConflicts_ToNewDatabase(Options options)
        {
            var file = GetTempFileName();
            try
            {
                var modifyDatabaseRecord = options.ModifyDatabaseRecord;
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1",
                    ModifyDatabaseRecord = record =>
                    {
                        modifyDatabaseRecord(record);
                        record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>() };
                    }
                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store2",
                    ModifyDatabaseRecord = record =>
                    {
                        modifyDatabaseRecord(record);
                        record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>() };
                    }
                }))
                {
                    await GenerateConflict(store1, store2);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    using (var store3 = GetDocumentStore(new Options(options)
                    {
                        ModifyDatabaseName = s => $"{s}_store3"
                    }))
                    {
                        operation = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        await AssertImport(store3);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanExportAndImportWithConflicts_ToNewDatabase_JustOneCollection(Options options)
        {
            var file = GetTempFileName();
            try
            {
                var modifyDatabaseRecord = options.ModifyDatabaseRecord;
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1",
                    ModifyDatabaseRecord = record =>
                    {
                        modifyDatabaseRecord(record);
                        record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>() };
                    }
                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store2",
                    ModifyDatabaseRecord = record =>
                    {
                        modifyDatabaseRecord(record);
                        record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>() };
                    }
                }))
                {
                    await GenerateConflict(store1, store2);

                    var exportOperation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    using (var store3 = GetDocumentStore(new Options
                    {
                        ModifyDatabaseName = s => $"{s}_store3"
                    }))
                    {
                        var importOptions = new DatabaseSmugglerImportOptions { Collections = new List<string> { "Users" } };

                        var importOperation = await store3.Smuggler.ImportAsync(importOptions, file);
                        await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        var stats = await GetDatabaseStatisticsAsync(store3);
                        Assert.Equal(0, stats.CountOfDocuments);          // Should be 0 Documents of 3 exported
                        Assert.Equal(1, stats.CountOfDocumentsConflicts); // only 1 DocumentConflict of 4
                        Assert.Equal(2, stats.CountOfConflicts);          // and only 2 Conflicts of 8

                        await AssertConflicts(store3);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ToDatabaseWithSameDocumentWithoutConflicts_DeleteTheDocumentAndGenerateTheSameConflicts(Options options)
        {
            var file = GetTempFileName();
            try
            {
                var modifyDatabaseRecord = options.ModifyDatabaseRecord;
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1",
                    ModifyDatabaseRecord = record =>
                    {
                        modifyDatabaseRecord(record);
                        record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>() };
                    }
                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store2",
                    ModifyDatabaseRecord = record =>
                    {
                        modifyDatabaseRecord(record);
                        record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>() };
                    }
                }))
                {
                    await GenerateConflict(store1, store2);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    using (var store3 = GetDocumentStore(new Options
                    {
                        ModifyDatabaseName = s => $"{s}_store3"
                    }))
                    {
                        await SetupReplicationAsync(store2, store3);
                        WaitForDocument(store3, "people/1-A");

                        var stats = await GetDatabaseStatisticsAsync(store3);
                        Assert.Equal(7, stats.CountOfDocuments);
                        Assert.Equal(0, stats.CountOfDocumentsConflicts);
                        Assert.Equal(0, stats.CountOfConflicts);
                        Assert.Equal(0, stats.CountOfTombstones);

                        operation = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        await AssertImport(store3);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanExportAndImportWithConflicts_ToDatabaseWithDifferentDocument_DeleteTheDocumentWithoutCreatingConflictForIt(Options options)
        {
            var file = GetTempFileName();
            try
            {
                var modifyDatabaseRecord = options.ModifyDatabaseRecord;
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1",
                    ModifyDatabaseRecord = record =>
                    {
                        modifyDatabaseRecord(record);
                        record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>() };
                    }
                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store2",
                    ModifyDatabaseRecord = record =>
                    {
                        modifyDatabaseRecord(record);
                        record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>() };
                    }
                }))
                {
                    await GenerateConflict(store1, store2);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    using (var session = store2.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>("users/fitzchak");
                        user.LastName = "Update to generate another conflict.";
                        user.Name = "Fitzchak 3";
                        await session.SaveChangesAsync();
                    }

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    await AssertImport(store2);
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ToDatabaseWithDifferentConflicts_AndTheImportedConflictsInAdditionToTheExistingConflicts(Options options)
        {
            var file = GetTempFileName();
            try
            {
                var modifyDatabaseRecord = options.ModifyDatabaseRecord;
                using (var store1 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store1",
                    ModifyDatabaseRecord = record =>
                    {
                        modifyDatabaseRecord(record);
                        record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>() };
                    }
                }))
                using (var store2 = GetDocumentStore(new Options
                {
                    ModifyDatabaseName = s => $"{s}_store2",
                    ModifyDatabaseRecord = record =>
                    {
                        modifyDatabaseRecord(record);
                        record.ConflictSolverConfig = new ConflictSolver { ResolveToLatest = false, ResolveByCollection = new Dictionary<string, ScriptResolver>() };
                    }
                }))
                {
                    await GenerateConflict(store1, store2);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    using (var store3 = GetDocumentStore(new Options
                    {
                        ModifyDatabaseName = s => $"{s}_store3",
                        ModifyDatabaseRecord = record =>
                        {
                            record.ConflictSolverConfig = new ConflictSolver
                            {
                                ResolveToLatest = false,
                                ResolveByCollection = new Dictionary<string, ScriptResolver>()
                            };
                        }
                    }))
                    using (var store4 = GetDocumentStore(new Options
                    {
                        ModifyDatabaseName = s => $"{s}_store4",
                        ModifyDatabaseRecord = record =>
                        {
                            record.ConflictSolverConfig = new ConflictSolver
                            {
                                ResolveToLatest = false,
                                ResolveByCollection = new Dictionary<string, ScriptResolver>()
                            };
                        }
                    }))
                    {
                        await GenerateConflict2(store3, store4);

                        operation = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                        await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                        await AssertImport2(store3);
                    }
                }
            }
            finally
            {
                File.Delete(file);
            }
        }

        private async Task GenerateConflict(DocumentStore store1, DocumentStore store2)
        {
            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Fitzchak 1", Id = "users/fitzchak" });
                await session.StoreAsync(new Person { Name = "Name1" });
                await session.StoreAsync(new Person { Name = "Name2" });
                await session.StoreAsync(new Person { Name = "Name - No conflict" });
                await session.StoreAsync(new Company { Name = "Hibernating Rhinos " });
                await session.SaveChangesAsync();
            }

            using (var session = store2.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Fitzchak 2", Id = "users/fitzchak" });
                await session.StoreAsync(new Person { Name = "Name11" });
                await session.StoreAsync(new Person { Name = "Name12" });
                await session.StoreAsync(new Person { Name = "Name - No conflict" });
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

            var stats = await GetDatabaseStatisticsAsync(store1);
            Assert.Equal(3, stats.CountOfDocuments);
            Assert.Equal(4, stats.CountOfDocumentsConflicts);
            Assert.Equal(8, stats.CountOfConflicts);
            Assert.Equal(0, stats.CountOfTombstones);

            stats = await GetDatabaseStatisticsAsync(store2);
            Assert.Equal(7, stats.CountOfDocuments);
            Assert.Equal(0, stats.CountOfDocumentsConflicts);
            Assert.Equal(0, stats.CountOfConflicts);
            Assert.Equal(0, stats.CountOfTombstones);
        }

        private async Task GenerateConflict2(DocumentStore store3, DocumentStore store4)
        {
            using (var session = store3.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Fitzchak 3", Id = "users/fitzchak" });
                await session.StoreAsync(new Person { Name = "Name13" });
                await session.StoreAsync(new Person { Name = "Name23" });
                await session.StoreAsync(new Person { Name = "Name - No conflict" });
                await session.StoreAsync(new Company { Name = "Hibernating Rhinos " });
                await session.SaveChangesAsync();
            }

            using (var session = store4.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Fitzchak 4", Id = "users/fitzchak" });
                await session.StoreAsync(new Person { Name = "Name14" });
                await session.StoreAsync(new Person { Name = "Name14" });
                await session.StoreAsync(new Person { Name = "Name - No conflict" });
                await session.StoreAsync(new Company { Name = "Hibernating Rhinos 2 " });
                await session.SaveChangesAsync();
            }

            for (int i = 0; i < 2; i++)
            {
                using (var session = store3.OpenAsyncSession())
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

            using (var session = store3.OpenAsyncSession())
            {
                var person = await session.LoadAsync<Person>("people/2-A");
                Assert.NotNull(person);
                session.Delete(person);
                await session.SaveChangesAsync();
            }

            await SetupReplicationAsync(store4, store3);

            Assert.Equal(2, WaitUntilHasConflict(store3, "users/fitzchak").Length);

            var stats = await GetDatabaseStatisticsAsync(store3);
            Assert.Equal(3, stats.CountOfDocuments);
            Assert.Equal(4, stats.CountOfDocumentsConflicts);
            Assert.Equal(8, stats.CountOfConflicts);
            Assert.Equal(0, stats.CountOfTombstones);

            stats = await GetDatabaseStatisticsAsync(store4);
            Assert.Equal(7, stats.CountOfDocuments);
            Assert.Equal(0, stats.CountOfDocumentsConflicts);
            Assert.Equal(0, stats.CountOfConflicts);
            Assert.Equal(0, stats.CountOfTombstones);
        }

        private async Task AssertConflicts(DocumentStore store)
        {
            var conflicts = (await store.Commands().GetConflictsForAsync("users/fitzchak")).ToList();
            Assert.Equal(2, conflicts.Count);

            var names = new string[2];

            for (var i = 0; i < conflicts.Count; i++)
            {
                Assert.True(conflicts[i].Doc.TryGet(nameof(User.Name), out string name));
                names[i] = name;
            }

            Array.Sort(names);
            Assert.Equal("Fitzchak 1", names[0]);
            Assert.Equal("Fitzchak 2", names[1]);
        }

        private async Task AssertConflicts2(DocumentStore store)
        {
            var conflicts = (await store.Commands().GetConflictsForAsync("users/fitzchak")).ToList();
            Assert.Equal(4, conflicts.Count);

            var names = new string[4];

            for (var i = 0; i < conflicts.Count; i++)
            {
                Assert.True(conflicts[i].Doc.TryGet(nameof(User.Name), out string name));
                names[i] = name;
            }

            Array.Sort(names);
            Assert.Equal("Fitzchak 1", names[0]);
            Assert.Equal("Fitzchak 2", names[1]);
            Assert.Equal("Fitzchak 3", names[2]);
            Assert.Equal("Fitzchak 4", names[3]);
        }

        private async Task AssertImport(DocumentStore store)
        {
            var stats = await GetDatabaseStatisticsAsync(store);
            Assert.Equal(3, stats.CountOfDocuments);
            Assert.Equal(4, stats.CountOfDocumentsConflicts);
            Assert.Equal(8, stats.CountOfConflicts);
            Assert.Equal(0, stats.CountOfTombstones);

            await AssertConflicts(store);
        }

        private async Task AssertImport2(DocumentStore store)
        {
            var stats = await GetDatabaseStatisticsAsync(store);
            Assert.Equal(3, stats.CountOfDocuments);
            Assert.Equal(4, stats.CountOfDocumentsConflicts);
            Assert.Equal(13, stats.CountOfConflicts);
            Assert.Equal(0, stats.CountOfTombstones);

            await AssertConflicts2(store);
        }
    }
}
