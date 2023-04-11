using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Config;
using Raven.Server.ServerWide.Commands;
using StressTests.Issues;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Server.Documents.PeriodicBackup
{
    public class ServerWideBackupStress : RavenTestBase
    {
        public ServerWideBackupStress(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(2)]
        public async Task ServerWideBackupShouldBackupIdleDatabaseStress(int rounds)
        {
            // ReSharper disable once UseAwaitUsing
            using var stress = new RavenDB_14292(Output);
            await stress.ServerWideBackupShouldBackupIdleDatabase(rounds);
        }

        [NightlyBuildTheory]
        [InlineData(5)]
        public async Task ServerWideBackupShouldBackupIdleDatabaseStressNightly(int rounds)
        {
            // ReSharper disable once UseAwaitUsing
            using var stress = new RavenDB_14292(Output);
            await stress.ServerWideBackupShouldBackupIdleDatabase(rounds);
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanStoreAndEditServerWideBackupForIdleDatabase()
        {
            using var server = GetNewServer(new TestBase.ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "3",
                    [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
                }
            });
            using (var store = GetDocumentStore(new RavenTestBase.Options { Server = server, RunInMemory = false }))
            using (var excludedStore = GetDocumentStore(new RavenTestBase.Options { Server = server, RunInMemory = false }))
            {
                await AssertWaitForGreaterAsync(() => server.ServerStore.IdleDatabases.Count, 1, timeout: 60000, interval: 1000);

                var fullFreq = "0 2 1 1 *";
                var incFreq = "0 2 * * 0";
                var putConfiguration = new ServerWideBackupConfiguration
                {
                    FullBackupFrequency = fullFreq,
                    IncrementalBackupFrequency = incFreq,
                    LocalSettings = new LocalSettings { FolderPath = "test/folder" },
                    ExcludedDatabases = new[] { excludedStore.Database }
                };
                var result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
                var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result.Name));
                Assert.NotNull(serverWideConfiguration);
                Assert.Equal(fullFreq, serverWideConfiguration.FullBackupFrequency);
                Assert.Equal(incFreq, serverWideConfiguration.IncrementalBackupFrequency);

                await BackupNow(store, serverWideConfiguration.Name);

                await AssertWaitForGreaterAsync(() => server.ServerStore.IdleDatabases.Count, 1, timeout: 60000, interval: 1000);

                // update the backup configuration
                putConfiguration.Name = serverWideConfiguration.Name;
                putConfiguration.TaskId = serverWideConfiguration.TaskId;
                putConfiguration.FullBackupFrequency = "0 2 * * 0";

                var oldName = result.Name;
                result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
                await server.ServerStore.Cluster.WaitForIndexNotification(result.RaftCommandIndex, TimeSpan.FromMinutes(1));

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record.PeriodicBackups.Count);
                var periodicBackupConfiguration = record.PeriodicBackups.First();

                var newServerWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result.Name));

                // compare with periodic backup task 
                Assert.NotEqual(newServerWideConfiguration.TaskId, periodicBackupConfiguration.TaskId); // backup task id in db record doesn't change
                Assert.Equal(PutServerWideBackupConfigurationCommand.GetTaskName(oldName), periodicBackupConfiguration.Name);
                Assert.Equal(incFreq, periodicBackupConfiguration.FullBackupFrequency);
                Assert.Equal(incFreq, periodicBackupConfiguration.IncrementalBackupFrequency);
                Assert.NotEqual(serverWideConfiguration.FullBackupFrequency, periodicBackupConfiguration.FullBackupFrequency);

                // compare with previous server wide backup
                Assert.NotEqual(serverWideConfiguration.TaskId, newServerWideConfiguration.TaskId); // task id in server storage get increased with each change
                Assert.Equal(oldName, result.Name);
                Assert.Equal(incFreq, newServerWideConfiguration.FullBackupFrequency);
                Assert.Equal(incFreq, newServerWideConfiguration.IncrementalBackupFrequency);
                Assert.NotEqual(serverWideConfiguration.FullBackupFrequency, newServerWideConfiguration.FullBackupFrequency);

                using (var createdAfter = GetDocumentStore(new RavenTestBase.Options { Server = server, RunInMemory = false }))
                {
                    await BackupNow(createdAfter, serverWideConfiguration.Name);

                    await AssertWaitForGreaterAsync(() => server.ServerStore.IdleDatabases.Count, 2, timeout: 60000, interval: 1000);

                    putConfiguration.TaskId = result.RaftCommandIndex;
                    putConfiguration.RetentionPolicy = new RetentionPolicy { MinimumBackupAgeToKeep = TimeSpan.FromDays(10) };
                    result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
                    await server.ServerStore.Cluster.WaitForIndexNotification(result.RaftCommandIndex, TimeSpan.FromMinutes(1));
                }
            }

            async Task BackupNow(DocumentStore store, string backupName)
            {
                var res = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation($"Server Wide Backup, {backupName}", OngoingTaskType.Backup));
                await store.Maintenance.SendAsync(new StartBackupOperation(false, res.TaskId));
            }
        }

    }
}
