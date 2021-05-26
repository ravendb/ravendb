using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13490 : RavenTestBase
    {
        public RavenDB_13490(ITestOutputHelper output) : base(output)
        {
        }

        private class MapReduce_WithOutput : AbstractIndexCreationTask<Company, MapReduce_WithOutput.Result>
        {
            public class Result
            {
                public string Id { get; set; }

                public string Name { get; set; }

                public int Count { get; set; }
            }

            public MapReduce_WithOutput()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       Name = c.Name,
                                       Count = 1
                                   };

                Reduce = results => from r in results
                                    group r by r.Name into g
                                    select new
                                    {
                                        Name = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };

                OutputReduceToCollection = "Results";
            }
        }

        [Fact]
        public async Task IncludeArtificialDocuments_Smuggler_ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new MapReduce_WithOutput().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                string artificialDocumentId = null;

                Assert.True(WaitForValue(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var result = session.Query<MapReduce_WithOutput.Result>().SingleOrDefault();
                        artificialDocumentId = result.Id;
                        return result != null;
                    }
                }, true));

                Assert.NotNull(artificialDocumentId);

                var toFileWithoutArtificial = Path.Combine(NewDataPath(), "export_without_artificial.ravendbdump");
                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), toFileWithoutArtificial);
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                var toFileWithArtificial = Path.Combine(NewDataPath(), "export_with_artificial.ravendbdump");
                operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions { IncludeArtificial = true }, toFileWithArtificial);
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                var importOptionsWithoutArtificial = new DatabaseSmugglerImportOptions();
                var importOptionsWithArtificial = new DatabaseSmugglerImportOptions { IncludeArtificial = true };

                importOptionsWithoutArtificial.OperateOnTypes |= ~DatabaseItemType.Indexes;
                importOptionsWithArtificial.OperateOnTypes |= ~DatabaseItemType.Indexes;

                // no artificial in file
                // include artificial is false
                using (var innerStore = GetDocumentStore())
                {
                    operation = await innerStore.Smuggler.ImportAsync(importOptionsWithoutArtificial, toFileWithoutArtificial);
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                    using (var session = innerStore.OpenSession())
                    {
                        Assert.Null(session.Load<MapReduce_WithOutput.Result>(artificialDocumentId));
                    }
                }

                // no artificial in file
                // include artificial is true
                using (var innerStore = GetDocumentStore())
                {
                    operation = await innerStore.Smuggler.ImportAsync(importOptionsWithArtificial, toFileWithoutArtificial);
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                    using (var session = innerStore.OpenSession())
                    {
                        Assert.Null(session.Load<MapReduce_WithOutput.Result>(artificialDocumentId));
                    }
                }

                // artificial in file
                // include artificial is false
                using (var innerStore = GetDocumentStore())
                {
                    operation = await innerStore.Smuggler.ImportAsync(importOptionsWithoutArtificial, toFileWithArtificial);
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                    using (var session = innerStore.OpenSession())
                    {
                        Assert.Null(session.Load<MapReduce_WithOutput.Result>(artificialDocumentId));
                    }
                }

                // artificial in file
                // include artificial is true
                using (var innerStore = GetDocumentStore())
                {
                    operation = await innerStore.Smuggler.ImportAsync(importOptionsWithArtificial, toFileWithArtificial);
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                    using (var session = innerStore.OpenSession())
                    {
                        Assert.NotNull(session.Load<MapReduce_WithOutput.Result>(artificialDocumentId));
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task IncludeArtificialDocuments_Backup_ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new MapReduce_WithOutput().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                string artificialDocumentId = null;

                Assert.True(WaitForValue(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var result = session.Query<MapReduce_WithOutput.Result>().SingleOrDefault();
                        artificialDocumentId = result.Id;
                        return result != null;
                    }
                }, true));

                Assert.NotNull(artificialDocumentId);

                var toFolderWithArtificial = Path.Combine(NewDataPath(), "BackupFolder");

                var config = Backup.CreateBackupConfiguration(toFolderWithArtificial);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                toFolderWithArtificial = Directory.GetDirectories(toFolderWithArtificial).First();
                var toDatabaseName = store.Database + "_restored";

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = toFolderWithArtificial,
                    DatabaseName = toDatabaseName,
                    SkipIndexes = true
                }))
                {
                    using (var session = store.OpenSession(toDatabaseName))
                    {
                        Assert.Null(session.Load<MapReduce_WithOutput.Result>(artificialDocumentId));
                    }
                }

                var importOptionsWithoutArtificial = new DatabaseSmugglerImportOptions();
                var importOptionsWithArtificial = new DatabaseSmugglerImportOptions { IncludeArtificial = true };

                importOptionsWithoutArtificial.OperateOnTypes |= ~DatabaseItemType.Indexes;
                importOptionsWithArtificial.OperateOnTypes |= ~DatabaseItemType.Indexes;

                var toFileWithArtificial = Directory.GetFiles(toFolderWithArtificial).First();

                // artificial in file
                // include artificial is false
                using (var innerStore = GetDocumentStore())
                {
                    var operation = await innerStore.Smuggler.ImportAsync(importOptionsWithoutArtificial, toFileWithArtificial);
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                    using (var session = innerStore.OpenSession())
                    {
                        Assert.Null(session.Load<MapReduce_WithOutput.Result>(artificialDocumentId));
                    }
                }

                // artificial in file
                // include artificial is true
                using (var innerStore = GetDocumentStore())
                {
                    var operation = await innerStore.Smuggler.ImportAsync(importOptionsWithArtificial, toFileWithArtificial);
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                    using (var session = innerStore.OpenSession())
                    {
                        Assert.NotNull(session.Load<MapReduce_WithOutput.Result>(artificialDocumentId));
                    }
                }
            }
        }
    }
}
