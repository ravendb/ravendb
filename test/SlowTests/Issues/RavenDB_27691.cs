using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Logging;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Logging;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Issues
{
    public class RavenDB_27691 : RavenTestBase
    {
        public RavenDB_27691(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task SavingBackupStatusOfDeletedDatabaseShouldNotLeaveLocalStatus()
        {
            using (var store = GetDocumentStore())
            {
                var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"));
                var taskId = await Backup.UpdateConfigAsync(Server, config, store);

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: true));
                var deleted = await WaitForValueAsync(async () => await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database)) == null, true);
                Assert.True(deleted, $"Database '{store.Database}' was not deleted");

                var status = new PeriodicBackupStatus
                {
                    TaskId = taskId,
                    NodeTag = Server.ServerStore.NodeTag,
                    BackupType = BackupType.Backup,
                    LastFullBackup = DateTime.UtcNow,
                    LastFullBackupInternal = DateTime.UtcNow
                };

                BackupUtils.SaveBackupStatus(status, store.Database, Server.ServerStore, RavenLogManager.Instance.GetLoggerForDatabase<RavenDB_27691>(store.Database));

                Assert.Null(Server.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(store.Database, taskId));
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task IdleDatabaseBackupStatusUpdateShouldWaitForFreeSlotAndKeepItsBackupTime()
        {
            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                    [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3"
                }
            });
            using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
            var taskId = await CreateBackedUpDatabaseAsync(server, store);

            server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;
            Assert.True(await WaitForValueAsync(() => server.ServerStore.IdleDatabases.ContainsKey(store.Database), true, timeout: 90_000, interval: 500),
                "precondition: the database should become idle");

            var backupState = BackupTestBase.GetBackupState(server.ServerStore.ServerBackupRunner, store.Database, taskId);
            Assert.NotNull(backupState);

            Assert.True(await WaitForValueAsync(() => backupState.StatusUpdateInProgress.Raise(), true, timeout: 30_000, interval: 50),
                "precondition: the backup status update slot should be taken by the test");

            DateTime? backupTime;
            try
            {
                var nextBackup = backupState.NextBackup;
                Assert.True(nextBackup is { IsFull: false }, "precondition: the next backup should be an incremental backup");
                backupTime = nextBackup.DateTime;

                var waitUntil = backupTime.Value + TimeSpan.FromSeconds(5);
                Assert.False(await WaitForValueAsync(() => GetLastIncrementalBackup(server, store.Database, taskId) == backupTime, true,
                        timeout: (int)Math.Max(0, (waitUntil - DateTime.UtcNow).TotalMilliseconds), interval: 500),
                    "The backup status was updated while there was no free slot");
            }
            finally
            {
                backupState.StatusUpdateInProgress.Lower();
            }

            Assert.Equal(backupTime, await WaitForValueAsync(() => Task.FromResult(GetLastIncrementalBackup(server, store.Database, taskId)), backupTime, timeout: 30_000));
        }

        private async Task<long> CreateBackedUpDatabaseAsync(RavenServer server, DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Grisha" }, "users/1");
                await session.SaveChangesAsync();
            }

            var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "0 0 1 1 *", incrementalBackupFrequency: "* * * * *");
            return await Backup.UpdateConfigAndRunBackupAsync(server, config, store);
        }

        private static DateTime? GetLastIncrementalBackup(RavenServer server, string databaseName, long taskId)
        {
            return server.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(databaseName, taskId)?.LastIncrementalBackup;
        }
    }
}
