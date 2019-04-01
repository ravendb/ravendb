using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class RavenDB_13229 : RavenTestBase
    {
        [Fact]
        public async Task BackupWithIdentityAndCompareExchangeShouldHaveOnlyOwnValues()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder1");
            var cmpXchg1 = new User { Name = "👺" };

            using (var store = GetDocumentStore(new Options { ModifyDatabaseName = s => "a" }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => "aa"
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    var bestUser = new User
                    {
                        Name = "Egor1"
                    };
                    await session.StoreAsync(bestUser, "a|");
                    await session.SaveChangesAsync();
                }

                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/goblin", cmpXchg1, 0));

                using (var session = store2.OpenAsyncSession())
                {
                    var bestUser = new User
                    {
                        Name = "Egor2"
                    };
                    await session.StoreAsync(bestUser, "aa|");
                    await session.SaveChangesAsync();
                }

                var cmpXchg2 = new User { Name = "🤡" };
                await store2.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/clown", cmpXchg2, 0));

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
                RunBackup(result.TaskId, documentDatabase, true, store); // FULL BACKUP
            }

            var backupDirectory = Directory.GetDirectories(backupPath).First();
            var databaseName = GetDatabaseName() + "restore";

            var files = Directory.GetFiles(backupDirectory)
                .Where(BackupUtils.IsBackupFile)
                .OrderBackups()
                .ToArray();

            var restoreConfig = new RestoreBackupConfiguration
            {
                BackupLocation = backupDirectory,
                DatabaseName = databaseName,
                LastFileNameToRestore = files.Last()
            };

            using (var store2 = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                ModifyDatabaseName = s => databaseName
            }))
            {
                var restoreOperation = new RestoreBackupOperation(restoreConfig);
                store2.Maintenance.Server.Send(restoreOperation)
                    .WaitForCompletion(TimeSpan.FromSeconds(30));
                using (var session = store2.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var stats = store2.Maintenance.ForDatabase(databaseName).Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(1, stats.CountOfIdentities);
                    Assert.Equal(1, stats.CountOfCompareExchange);
                    Assert.Equal(1, stats.CountOfDocuments);

                    var bestUser = await session.LoadAsync<User>("a/1");
                    var mediocreUser1 = await session.LoadAsync<User>("aa/1");

                    Assert.NotNull(bestUser);
                    Assert.Null(mediocreUser1);

                    Assert.Equal("Egor1", bestUser.Name);

                    var cmpXchg = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("emojis/goblin");
                    Assert.Equal(cmpXchg1.Name, cmpXchg.Value.Name);
                    var cmpXchg2 = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("emojis/clown");
                    Assert.Null(cmpXchg2);
                }
            }
        }

        private void RunBackup(long taskId, Raven.Server.Documents.DocumentDatabase documentDatabase, bool isFullBackup, DocumentStore store)
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
    }
}
