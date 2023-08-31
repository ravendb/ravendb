using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
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
            var backupPath = NewDataPath(suffix: "BackupFolder");
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

                var config = Backup.CreateBackupConfiguration(backupPath: backupPath, backupType: BackupType.Backup);
                var operation = new UpdatePeriodicBackupOperation(config);
                var result = await store.Maintenance.SendAsync(operation);

                await Backup.RunBackupAsync(Server, result.TaskId, store);

                config.BackupType = BackupType.Snapshot;
                config.TaskId = result.TaskId;

                operation = new UpdatePeriodicBackupOperation(config);
                await store.Maintenance.SendAsync(operation);

                var documentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store);
                documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateFailedBackup = true;

                await Backup.RunBackupAsync(Server, result.TaskId, store, opStatus: OperationStatus.Faulted);

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
