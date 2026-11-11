using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide.Backups;
using Raven.Server.ServerWide.Backups.Policies.Database;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues;

public class RavenDB_18420 : RavenTestBase
{
    public RavenDB_18420(ITestOutputHelper output) : base(output)
    {

    }
    [RavenFact(RavenTestCategory.Core)]
    public async Task ShouldRescheduleBackupTimerIfDocumentDatabaseFailedToLoad()
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                [RavenConfiguration.GetKey(x => x.Backup.KeepIdleDatabaseLoadedBeforeBackup)] = "0",
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
            }
        });

        var backupPath = NewDataPath(forceCreateDir: true);
        using (var store = GetDocumentStore(new Options { Server = server, RunInMemory = false }))
        {
            Assert.Equal(1, WaitForValue(() => server.ServerStore.IdleDatabases.Count, 1, timeout: 60000, interval: 1000));
            var putConfiguration = new ServerWideBackupConfiguration
            {
                FullBackupFrequency = "*/1 * * * *",
                Disabled = true,
                LocalSettings = new LocalSettings { FolderPath = backupPath }
            };

            var result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
            var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result.Name));
            putConfiguration.Name = serverWideConfiguration.Name;
            putConfiguration.TaskId = serverWideConfiguration.TaskId;
            putConfiguration.Disabled = false;

            var landlord = server.ServerStore.DatabasesLandlord;
            var old_databaseSemaphore = landlord._databaseSemaphore;
            var old_concurrentDatabaseLoadTimeout = landlord._concurrentDatabaseLoadTimeout;
            landlord._databaseSemaphore = new SemaphoreSlim(0);
            landlord._concurrentDatabaseLoadTimeout = TimeSpan.Zero;

            try
            {
                Backup.WaitForResponsibleNodeUpdate(server.ServerStore, store.Database, putConfiguration.TaskId);
                Assert.Equal(1, server.ServerStore.IdleDatabases.Count);

                var startUtc = DateTime.UtcNow;
                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));

                Assert.True(await WaitForValueAsync(() => Task.FromResult(GetDecisions(server, store.Database, putConfiguration.TaskId)
                        .Any(d => d.Category == BackupDecisionCategory.Waiting && d.Detail == DatabaseLoadedPolicy.Instance.Name && d.LastTime >= startUtc)), true,
                        timeout: 90_000, interval: 500),
                    "the scheduled backup should wait for the next run when the database fails to load");
            }
            finally
            {
                landlord._databaseSemaphore = old_databaseSemaphore;
                landlord._concurrentDatabaseLoadTimeout = old_concurrentDatabaseLoadTimeout;
            }

            Assert.Equal(0, WaitForValue(() => server.ServerStore.IdleDatabases.Count, 0, timeout: 90_000, interval: 333));

            PeriodicBackupStatus status = null;
            var val = await WaitForValueAsync(async () =>
            {
                var operation = new GetPeriodicBackupStatusOperation(putConfiguration.TaskId);
                status = (await store.Maintenance.SendAsync(operation)).Status;
                return status?.LastOperationId != null;
            }, true, interval: 1000);
            Assert.True(val);
            Assert.NotNull(status);
            Assert.NotNull(status.LastFullBackup);
        }
    }

    private static List<BackupDecision> GetDecisions(RavenServer server, string database, long taskId)
    {
        return BackupTestBase.GetDecisionLog(server.ServerStore.ServerBackupRunner, database, taskId) ?? new List<BackupDecision>();
    }
}
