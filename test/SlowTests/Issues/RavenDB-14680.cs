using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14680 : RavenTestBase
    {
        public RavenDB_14680(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task DeleteCompareExchangeTombstones()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var indexesList = new List<long>();
            using (var store = GetDocumentStore())
            {
                for (int i = 0; i < 8; i++)
                {
                    var user = new User {Name = $"name_{i}"};

                    var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>($"ce/{i}", user, 0));
                    indexesList.Add(res.Index);
                }

                var stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(8, stats.CountOfCompareExchange);

                for (int i = 0; i < 8; i++)
                {
                    var res = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>($"ce/{i}", indexesList[i]));
                }

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var numOfCompareExchangeTombstones = Server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);

                    Assert.Equal(8, numOfCompareExchangeTombstones);
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Backup
                };

                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                RunBackup(result.TaskId, documentDatabase, true, store); // FULL BACKUP

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    //clean tombstones
                    await Server.ServerStore.Observer.CleanUpCompareExchangeTombstones(store.Database, context);
                }
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var numOfCompareExchangeTombstones = Server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);

                    Assert.Equal(0, numOfCompareExchangeTombstones);
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
