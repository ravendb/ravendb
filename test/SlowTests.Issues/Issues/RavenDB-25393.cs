using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Revisions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25393 : ReplicationTestBase
    {
        public RavenDB_25393(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RestoreIncrementalBackupCreatesExtraRevision(Options options)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using var store = GetDocumentStore(options);

            using (var source = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(source, Server.ServerStore, configuration: new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 100
                    }
                });

                var config = new PeriodicBackupConfiguration { LocalSettings = new LocalSettings { FolderPath = backupPath }, IncrementalBackupFrequency = "0 0 */12 * *" };
                var backupTaskId = (await source.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                Backup.WaitForResponsibleNodeUpdate(Server.ServerStore, source.Database, backupTaskId);

                using (var session = source.OpenAsyncSession())
                {
                    await session.StoreAsync(new User()
                    {
                        Name = "Shahar"
                    }, "Users/1");
                    await session.SaveChangesAsync();
                }

                var backupStatus = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                await backupStatus.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                using (var session = source.OpenAsyncSession())
                {
                    await session.StoreAsync(new User()
                    {
                        Name = "Shahar Hikri"
                    }, "Users/1");
                    await session.SaveChangesAsync();

                    await session.SaveChangesAsync();
                }

                var backupStatus2 = await source.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                await backupStatus2.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                await Backup.GetBackupFilesAndAssertCountAsync(backupPath, 2, backupStatus2.Id, source.Database);

                Assert.Equal(2, (await source.Maintenance.ForDatabase(source.Database).SendAsync(new GetStatisticsOperation())).CountOfRevisionDocuments);
            }

            var restoredDatabaseName = GetDatabaseName() + "_Restore";
            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
            {
                BackupLocation = Directory.GetDirectories(backupPath).First(),
                DatabaseName = restoredDatabaseName
            }))
            {
                var afterRestoreStats = await store.Maintenance.ForDatabase(restoredDatabaseName).SendAsync(new GetStatisticsOperation());
                Assert.Equal(2, afterRestoreStats.CountOfRevisionDocuments);
            }
        }
    }
}
