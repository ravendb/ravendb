using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Smuggler
{
    public class SmugglerConflicts : ReplicationTestBase
    {
        private readonly string _file;
        private readonly DocumentStore _store1, _store2;

        public SmugglerConflicts()
        {
            _file = GetTempFileName();

            _store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_store1",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            });
            _store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_store2",
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            });
        }
        public override void Dispose()
        {
            _store1.Dispose();
            _store2.Dispose();
            File.Delete(_file);
            base.Dispose();
        }

        [Fact]
        public async Task CanExportAndImportWithConflicts_ToTheSameDatabase()
        {
            await GenerateConflict(_store1, _store2);

            var operation = await _store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), _file);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            await AssertImport(_store1);
        }

        [Fact]
        public async Task CanExportAndImportWithConflicts_ToNewDatabase()
        {
            await GenerateConflict(_store1, _store2);

            var operation = await _store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), _file);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            using (var store3 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_store3"
            }))
            {
                await AssertImport(store3);
            }
        }

        [Fact]
        public async Task ToDatabaseWithSameDocumentWithoutConflicts_DeleteTheDocumentAndGenerateTheSameConflicts()
        {
            await GenerateConflict(_store1, _store2);

            var operation = await _store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), _file);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            using (var store3 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_store3"
            }))
            {
                await SetupReplicationAsync(_store2, store3);
                WaitForDocument(store3, "people/1-A");

                var stats = await store3.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(7, stats.CountOfDocuments);
                Assert.Equal(0, stats.CountOfDocumentsConflicts);
                Assert.Equal(0, stats.CountOfConflicts);
                Assert.Equal(0, stats.CountOfTombstones);

                await AssertImport(store3);
            }
        }

        [Fact]
        public async Task CanExportAndImportWithConflicts_ToDatabaseWithDifferentDocument_DeleteTheDocumentWithoutCreatingConflictForIt()
        {
            await GenerateConflict(_store1, _store2);

            var operation = await _store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), _file);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            using (var session = _store2.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/fitzchak");
                user.LastName = "Update to generate another conflict.";
                user.Name = "Fitzchak 3";
                await session.SaveChangesAsync();
            }

            await AssertImport(_store2);
        }

        [Fact]
        public async Task ToDatabaseWithDifferentConflicts_AndTheImportedConflictsInAdditionToTheExistingConflicts()
        {
            await GenerateConflict(_store1, _store2);

            var operation = await _store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), _file);
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

                await AssertImport2(store3);
            }
        }

        private async Task GenerateConflict(DocumentStore store1, DocumentStore store2)
        {
            await SetDatabaseId(store1, new Guid("11111111-1111-1111-1111-111111111111"));
            await SetDatabaseId(store2, new Guid("22222222-2222-2222-2222-222222222222"));

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

            var stats = await store1.Maintenance.SendAsync(new GetStatisticsOperation());
            Assert.Equal(3, stats.CountOfDocuments);
            Assert.Equal(4, stats.CountOfDocumentsConflicts);
            Assert.Equal(8, stats.CountOfConflicts);
            Assert.Equal(0, stats.CountOfTombstones);

            stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
            Assert.Equal(7, stats.CountOfDocuments);
            Assert.Equal(0, stats.CountOfDocumentsConflicts);
            Assert.Equal(0, stats.CountOfConflicts);
            Assert.Equal(0, stats.CountOfTombstones);
        }

        private async Task GenerateConflict2(DocumentStore store3, DocumentStore store4)
        {
            await SetDatabaseId(store3, new Guid("33333333-3333-3333-3333-333333333333"));
            await SetDatabaseId(store4, new Guid("44444444-4444-4444-4444-444444444444"));

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

            var stats = await store3.Maintenance.SendAsync(new GetStatisticsOperation());
            Assert.Equal(3, stats.CountOfDocuments);
            Assert.Equal(4, stats.CountOfDocumentsConflicts);
            Assert.Equal(8, stats.CountOfConflicts);
            Assert.Equal(0, stats.CountOfTombstones);

            stats = await store4.Maintenance.SendAsync(new GetStatisticsOperation());
            Assert.Equal(7, stats.CountOfDocuments);
            Assert.Equal(0, stats.CountOfDocumentsConflicts);
            Assert.Equal(0, stats.CountOfConflicts);
            Assert.Equal(0, stats.CountOfTombstones);
        }

        private async Task AssertImport(DocumentStore store)
        {
            for (int i = 0; i < 3; i++)
            {
                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), _file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(3, stats.CountOfDocuments);
                Assert.Equal(4, stats.CountOfDocumentsConflicts);
                Assert.Equal(8, stats.CountOfConflicts);
                Assert.Equal(0, stats.CountOfTombstones);

                var conflicts = (await store.Commands().GetConflictsForAsync("users/fitzchak")).ToList();
                Assert.Equal(2, conflicts.Count);

                Assert.Equal("A:3-EREREREREREREREREREREQ", conflicts[0].ChangeVector);
                Assert.True(conflicts[0].Doc.TryGet(nameof(User.Name), out string name));
                Assert.Equal("Fitzchak 1", name);

                Assert.Equal("A:3-IiIiIiIiIiIiIiIiIiIiIg", conflicts[1].ChangeVector);
                Assert.True(conflicts[1].Doc.TryGet(nameof(User.Name), out name));
                Assert.Equal("Fitzchak 2", name);
            }
        }

        private async Task AssertImport2(DocumentStore store)
        {
            for (int i = 0; i < 3; i++)
            {
                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), _file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(3, stats.CountOfDocuments);
                Assert.Equal(4, stats.CountOfDocumentsConflicts);
                Assert.Equal(13, stats.CountOfConflicts);
                Assert.Equal(0, stats.CountOfTombstones);

                var conflicts = (await store.Commands().GetConflictsForAsync("users/fitzchak")).ToList();
                Assert.Equal(4, conflicts.Count);

                Assert.Equal("A:3-EREREREREREREREREREREQ", conflicts[0].ChangeVector);
                Assert.True(conflicts[0].Doc.TryGet(nameof(User.Name), out string name));
                Assert.Equal("Fitzchak 1", name);

                Assert.Equal("A:3-IiIiIiIiIiIiIiIiIiIiIg", conflicts[1].ChangeVector);
                Assert.True(conflicts[1].Doc.TryGet(nameof(User.Name), out name));
                Assert.Equal("Fitzchak 2", name);

                Assert.Equal("A:3-MzMzMzMzMzMzMzMzMzMzMw", conflicts[2].ChangeVector);
                Assert.True(conflicts[2].Doc.TryGet(nameof(User.Name), out name));
                Assert.Equal("Fitzchak 3", name);

                Assert.Equal("A:3-RERERERERERERERERERERA", conflicts[3].ChangeVector);
                Assert.True(conflicts[3].Doc.TryGet(nameof(User.Name), out name));
                Assert.Equal("Fitzchak 4", name);
            }
        }
    }
}
