using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Identities;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class RavenDB_12012 : RavenTestBase
    {

        [Fact]
        public async Task CreateFullAndIncrementalBackupWithIndexInTheMiddle()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Toli"
                    }, "users/1");
                    session.SaveChanges();
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "* */6 * * *",
                    BackupType = BackupType.Backup
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                RunBackup(result.TaskId, documentDatabase, true, store);

              
                var input = new IndexDefinition
                {
                    Name = "Users_ByName",
                    Maps =
                    {
                        "from user in docs.Users select new { user.Name }"
                    },
                    Type = IndexType.Map
                };

                await store
                    .Maintenance
                    .SendAsync(new PutIndexesOperation(new[] { input }));
                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                RunBackup(result.TaskId, documentDatabase, false, store);
                
                var backupDirectory = Directory.GetDirectories(backupPath).First();

               var databaseName = GetDatabaseName() + "restore";

                RestoreBackupConfiguration config2 = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).Last()
                };

                RestoreBackupOperation restoreOperation = new RestoreBackupOperation(config2);
                    store.Maintenance.Server.Send(restoreOperation)
                        .WaitForCompletion(TimeSpan.FromSeconds(30));
                
                using (var store2 = GetDocumentStore(new Options()
                {
                    CreateDatabase = false,
                    ModifyDatabaseName = s => databaseName
                }))
                {
                    var stats = store2.Maintenance.ForDatabase(databaseName).Send(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfIndexes);
                    Assert.Equal(1, stats.CountOfDocuments);
                };
            }
        }

        private void RunBackup(long taskId, Raven.Server.Documents.DocumentDatabase documentDatabase, bool isFullBackup ,DocumentStore store )
        {
            var periodicBackupRunner = documentDatabase.PeriodicBackupRunner;
            var op = periodicBackupRunner.StartBackupTask(taskId, isFullBackup);
            var value = WaitForValue(() =>
            {
                var status = store.Maintenance.Send(new GetOperationStateOperation(op)).Status;
                return status;
            }, OperationStatus.Completed);

            Assert.Equal(OperationStatus.Completed, value);
        }

        [Fact (Skip = "RavenDB-12349")]
        public async Task CreateFullAndIncrementalBackupWithCompareExchangeInTheMiddle()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Toli"
                    }, "users/1");
                    session.SaveChanges();
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "* */6 * * *",
                    BackupType = BackupType.Backup
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                RunBackup(result.TaskId, documentDatabase, true, store);


                CompareExchangeResult<string> compareExchangeResult
                    = store.Operations.Send(
                        new PutCompareExchangeValueOperation<string>("users/1", "Mitzi", 0));

                config.IncrementalBackupFrequency = "* */2 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                RunBackup(result.TaskId, documentDatabase, false, store);

                var backupDirectory = Directory.GetDirectories(backupPath).First();

                var databaseName = GetDatabaseName() + "restore";

                RestoreBackupConfiguration config2 = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).Last()
                };

                RestoreBackupOperation restoreOperation = new RestoreBackupOperation(config2);
                store.Maintenance.Server.Send(restoreOperation)
                    .WaitForCompletion();

                using (var store2 = GetDocumentStore(new Options()
                {
                    CreateDatabase = false,
                    ModifyDatabaseName = s => databaseName
                }))
                {
                    using (var session = store2.OpenSession())
                    {
                        var doc = session.Load<User>("users/1");
                        Assert.NotNull(doc);
                    };
                    CompareExchangeValue<string> readResult =
                        store2.Operations.Send(new GetCompareExchangeValueOperation<string>("users/1"));

                    Assert.Equal("Mitzi", readResult.Value);

                };
            }
        }

        [Fact(Skip = "RavenDB-12350")]
        public async Task CreateFullAndIncrementalBackupWithIdentitiesInTheMiddle()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Id = "users|",
                        Name = "Toli"
                    });

                    session.Store(new User
                    {
                        Id = "users|",
                        Name = "Mitzi"
                    });
                    session.SaveChanges();
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "* */6 * * *",
                    BackupType = BackupType.Backup
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                RunBackup(result.TaskId, documentDatabase, true, store);

                Dictionary<string, long> identities = store.Maintenance.Send(new GetIdentitiesOperation());

                config.IncrementalBackupFrequency = "* */2 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                RunBackup(result.TaskId, documentDatabase, false, store);

                var backupDirectory = Directory.GetDirectories(backupPath).First();

                var databaseName = GetDatabaseName() + "restore";

                RestoreBackupConfiguration config2 = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).Last()
                };

                RestoreBackupOperation restoreOperation = new RestoreBackupOperation(config2);
                store.Maintenance.Server.Send(restoreOperation)
                    .WaitForCompletion();

                using (var store2 = GetDocumentStore(new Options()
                {
                    CreateDatabase = false,
                    ModifyDatabaseName = s => databaseName
                }))
                {

                    Dictionary<string, long> identities2 = store2.Maintenance.Send(new GetIdentitiesOperation());

                    Assert.Equal(identities.First().Key, identities2.First().Key);
                    Assert.Equal(identities.First().Value, identities2.First().Value);

                };
            }
        }
    }
}
