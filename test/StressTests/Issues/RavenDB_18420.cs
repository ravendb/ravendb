using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues;

public class RavenDB_18420 : RavenTestBase
{
    public RavenDB_18420(ITestOutputHelper output) : base(output)
    {

    }
    [Fact]
    public async Task ShouldRescheduleBackupTimerIfDocumentDatabaseFailedToLoad()
    {
        var db2 = GetDatabaseName();
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false",
            }
        });
        server.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = true;
        try
        {
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
                // update the backup configuration
                putConfiguration.Name = serverWideConfiguration.Name;
                putConfiguration.TaskId = serverWideConfiguration.TaskId;
                putConfiguration.Disabled = false;

                var old_databaseSemaphore = server.ServerStore.DatabasesLandlord._databaseSemaphore;
                var old_concurrentDatabaseLoadTimeout = server.ServerStore.DatabasesLandlord._concurrentDatabaseLoadTimeout;
                var old_dueTimeOnRetry = server.ServerStore.DatabasesLandlord._dueTimeOnRetry;
                server.ServerStore.DatabasesLandlord._databaseSemaphore = new SemaphoreSlim(0);
                server.ServerStore.DatabasesLandlord._concurrentDatabaseLoadTimeout = TimeSpan.Zero;
                server.ServerStore.DatabasesLandlord._dueTimeOnRetry = 5000;

                var mre = server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().RescheduleDatabaseWakeupMre = new ManualResetEventSlim();

                // enable backup
                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));

                Assert.True(mre.Wait(TimeSpan.FromSeconds(65)));
                server.ServerStore.DatabasesLandlord._databaseSemaphore = old_databaseSemaphore;
                server.ServerStore.DatabasesLandlord._concurrentDatabaseLoadTimeout = old_concurrentDatabaseLoadTimeout;
                server.ServerStore.DatabasesLandlord._dueTimeOnRetry = old_dueTimeOnRetry;

                // db should wake up after dueTimeOnRetry is hit
                Assert.Equal(0, WaitForValue(() => server.ServerStore.IdleDatabases.Count, 0, interval: 333));

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
        finally
        {
            server.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = false;
        }
    }
}
