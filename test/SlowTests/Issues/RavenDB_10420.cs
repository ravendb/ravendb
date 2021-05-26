using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Config;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10420 : RavenTestBase
    {
        public RavenDB_10420(ITestOutputHelper output) : base(output)
        {
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ShouldWork()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.PerformanceHints.MaxNumberOfResults)] = "1"
            }))
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.Query<Employee>().ToListAsync(); // this will generate performance hint
                }

                var database = await GetDatabase(store.Database);
                database.NotificationCenter.Paging.UpdatePaging(null);

                int beforeBackupAlertCount;
                using (database.NotificationCenter.GetStored(out var actions))
                    beforeBackupAlertCount = actions.Count();

                Assert.True(beforeBackupAlertCount > 0);

                var beforeBackupStats = store.Maintenance.Send(new GetStatisticsOperation());

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                // restore the database with a different name
                var restoredDatabaseName = GetDatabaseName();

                string backupLocation = Directory.GetDirectories(backupPath).First();
                var filesInBackupFolder = Directory.GetFiles(backupLocation);
                Assert.True(filesInBackupFolder.Where(RestorePointsBase.IsBackupOrSnapshot).Any(), 
                    $"The backup folder \"{backupLocation}\" contains no backup or snapshot files.\n{string.Join(", ","filesInBackupFolder")}");
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = backupLocation,
                    DatabaseName = restoredDatabaseName
                }))
                {
                    var afterRestoreStats = store.Maintenance.ForDatabase(restoredDatabaseName).Send(new GetStatisticsOperation());

                    var restoredDatabase = await GetDatabase(restoredDatabaseName);
                
                    int afterRestoreAlertCount;
                    using (restoredDatabase.NotificationCenter.GetStored(out var actions))
                        afterRestoreAlertCount = actions.Count();

                    Assert.True(afterRestoreAlertCount > 0);

                    var indexesPath = restoredDatabase.Configuration.Indexing.StoragePath;
                    var indexesDirectory = new DirectoryInfo(indexesPath.FullPath);
                    Assert.True(indexesDirectory.Exists);
                    Assert.Equal(afterRestoreStats.CountOfIndexes, indexesDirectory.GetDirectories().Length);

                    Assert.NotEqual(beforeBackupStats.DatabaseId, afterRestoreStats.DatabaseId);
                    Assert.Equal(beforeBackupStats.CountOfAttachments, afterRestoreStats.CountOfAttachments);
                    Assert.Equal(beforeBackupStats.CountOfConflicts, afterRestoreStats.CountOfConflicts);
                    Assert.Equal(beforeBackupStats.CountOfDocuments, afterRestoreStats.CountOfDocuments);
                    Assert.Equal(beforeBackupStats.CountOfDocumentsConflicts, afterRestoreStats.CountOfDocumentsConflicts);
                    Assert.Equal(beforeBackupStats.CountOfIndexes, afterRestoreStats.CountOfIndexes);
                    Assert.Equal(beforeBackupStats.CountOfRevisionDocuments, afterRestoreStats.CountOfRevisionDocuments);
                    Assert.Equal(beforeBackupStats.CountOfTombstones, afterRestoreStats.CountOfTombstones);
                    Assert.Equal(beforeBackupStats.CountOfUniqueAttachments, afterRestoreStats.CountOfUniqueAttachments);
                }
            }
        }
    }
}
