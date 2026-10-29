using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_27691 : RavenTestBase
    {
        public RavenDB_27691(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task IdleDatabaseBackupStatusUpdateShouldWaitForFreeSlotAndKeepItsBackupTime()
        {
            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Backup.MaxNumberOfConcurrentBackups)] = "1"
                }
            });
            using var store = GetDocumentStore(new Options { Server = server });
            var (taskId, lastEtag) = await CreateBackedUpDatabaseAsync(server, store);

            var landlord = server.ServerStore.DatabasesLandlord;
            landlord._dueTimeOnRetry = 1000;
            using var rescheduled = new ManualResetEventSlim();
            landlord.ForTestingPurposesOnly().RescheduleIdleBackupStatusUpdateMre = rescheduled;

            DateTime? backupTime = DateTime.UtcNow;
            var slot = server.ServerStore.ConcurrentBackupsCounter.TryStartIdleBackupStatusUpdate();
            Assert.NotNull(slot);

            try
            {
                MakeIdle(server, store.Database, new IdleDatabaseActivity(IdleDatabaseActivityType.UpdateBackupStatusOnly, backupTime.Value, taskId, lastEtag));

                Assert.True(await WaitForValueAsync(() => rescheduled.IsSet, true, timeout: 30_000), "The backup status update wasn't rescheduled while there was no free slot");
                Assert.NotEqual(backupTime, GetLastIncrementalBackup(server, store.Database, taskId));
            }
            finally
            {
                slot.Dispose();
            }

            Assert.Equal(backupTime, await WaitForValueAsync(() => Task.FromResult(GetLastIncrementalBackup(server, store.Database, taskId)), backupTime));
        }

        private async Task<(long TaskId, long LastEtag)> CreateBackedUpDatabaseAsync(RavenServer server, DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Grisha" }, "users/1");
                await session.SaveChangesAsync();
            }

            var config = Backup.CreateBackupConfiguration(NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "0 0 1 1 *", incrementalBackupFrequency: "30 * * * *");
            var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(server, store);
            var activity = database.PeriodicBackupRunner.GetNextIdleDatabaseActivity(store.Database);
            Assert.Equal(IdleDatabaseActivityType.UpdateBackupStatusOnly, activity.Type);

            return (taskId, activity.LastEtag);
        }

        private static void MakeIdle(RavenServer server, string databaseName, IdleDatabaseActivity activity)
        {
            var landlord = server.ServerStore.DatabasesLandlord;
            landlord.ForTestingPurposesOnly().SkipShouldContinueDisposeCheck = true;
            server.ServerStore.IdleDatabases[databaseName] = new Dictionary<string, long>();
            Assert.True(landlord.UnloadDirectly(databaseName, activity), $"Failed to unload database '{databaseName}'");
        }

        private static DateTime? GetLastIncrementalBackup(RavenServer server, string databaseName, long taskId)
        {
            return server.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(databaseName, taskId)?.LastIncrementalBackup;
        }
    }
}
