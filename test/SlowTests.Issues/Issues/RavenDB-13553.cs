using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide.Backups;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13553 : RavenTestBase
    {
        public RavenDB_13553(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Test()
        {
            DoNotReuseServer();
            using var server = GetNewServer();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore(new Options{Server = server}))
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

                await Backup.RunBackupAsync(server, result.TaskId, store);

                config.BackupType = BackupType.Snapshot;
                config.TaskId = result.TaskId;

                operation = new UpdatePeriodicBackupOperation(config);
                await store.Maintenance.SendAsync(operation);

                Backup.WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, result.TaskId);

                var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                var testingStuff = new ServerBackupRunner.TestingStuffInternal() { SimulateFailedBackup = true };
                documentDatabase.ServerStore.ServerBackupRunner.ForTestingPurposesOnly().DatabaseTestingStuffInternals[documentDatabase.Name] = testingStuff;

                await Backup.RunBackupAsync(server, result.TaskId, store, opStatus: OperationStatus.Faulted);

                documentDatabase.ServerStore.ServerBackupRunner._forTestingPurposes = null;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Yonatan2"
                    });

                    session.SaveChanges();
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var status = documentDatabase.ServerStore.ServerBackupRunner.GetMostUpdatedClusterBackupStatus(store.Database, config.TaskId);
                var nextBackupDetails = documentDatabase.ServerStore.ServerBackupRunner.GetNextBackupDetails(record.PeriodicBackups.First().TaskId, documentDatabase.Name, status, out var responsibleNode);
                
                Assert.True(nextBackupDetails.IsFull);
                Assert.Equal("A", responsibleNode);
            }
        }
    }
}
