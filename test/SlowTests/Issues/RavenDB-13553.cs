using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13553 : RavenTestBase
    {
        public RavenDB_13553(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Test()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Yonatan"
                    });

                    session.SaveChanges();
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = @"C:\RavenBackups"
                    },

                    //Full Backup period (Cron expression for a 3-hours period)
                    FullBackupFrequency = "0 */3 * * *",

                    //An incremental-backup will run every 20 minutes (Cron expression)
                    IncrementalBackupFrequency = "*/20 * * * *",

                    BackupType = BackupType.Backup,

                    Name = "fullBackupTask",
                };

                var operation = new UpdatePeriodicBackupOperation(config);
                var result = await store.Maintenance.SendAsync(operation);

                var backupStatus = await store.Maintenance.SendAsync(new StartBackupOperation(true, result.TaskId));
                var x = await backupStatus.WaitForCompletionAsync(TimeSpan.FromMinutes(5));

                config.BackupType = BackupType.Snapshot;
                config.TaskId = result.TaskId;

                operation = new UpdatePeriodicBackupOperation(config);
                var result2 = await store.Maintenance.SendAsync(operation);

                var documentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store);
                documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateFailedBackup = true;

                await Assert.ThrowsAsync<Raven.Client.Exceptions.RavenException>(async () =>
                {
                    var backupStatus2 = await store.Maintenance.SendAsync(new StartBackupOperation(true, result2.TaskId));
                    await backupStatus2.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                });

                documentDatabase.PeriodicBackupRunner._forTestingPurposes = null;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Yonatan2"
                    });

                    session.SaveChanges();
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                var status = documentDatabase.PeriodicBackupRunner.GetBackupStatus(config.TaskId);
                var nextBackupDetails = documentDatabase.PeriodicBackupRunner.GetNextBackupDetails(record, record.PeriodicBackups.First(), status, Server.ServerStore.NodeTag);
                
                Assert.True(nextBackupDetails.IsFull);
            }
        }
    }
}
