using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Voron.Util;
using Xunit;

namespace StressTests.Issues
{
    public class RavenDB_13987 : ReplicationTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(60 * 5);

        public RavenDB_13987(ITestOutputHelper output) : base(output)
        {
            DoNotReuseServer();
        }

        [NightlyBuildFact]
        public async Task ServerWideBackupShouldNotWakeupIdleDatabases()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            const int clusterSize = 3;
            var databaseName = GetDatabaseName();
            Dictionary<string, DocumentStore> documentStores = new();
            Dictionary<string, PeriodicBackupStatus> perNodeBackupStatuses;

            (List<RavenServer> nodes, RavenServer leaderNode) = await CreateRaftCluster(numberOfNodes: clusterSize, shouldRunInMemory: false, customSettings: new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "20",
                [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3"
            });

            using var dispose = new DisposableAction(() =>
            {
                foreach (var server in nodes)
                {
                    server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipShouldContinueDisposeCheck = false;
                    server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = false;
                    documentStores[server.ServerStore.NodeTag].Dispose();
                }
            });

            foreach (var server in nodes)
            {
                server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipShouldContinueDisposeCheck = true;
                server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;
            }

            await CreateDatabaseInCluster(databaseName, clusterSize, leaderNode.WebUrl);
            var storeCreationOptions = new Options
            {
                Server = leaderNode,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                ModifyDatabaseName = s => databaseName,
                RunInMemory = false,
                CreateDatabase = false
            };

            foreach (var server in nodes)
            {
                storeCreationOptions.Server = server;
                documentStores[server.ServerStore.NodeTag] = GetDocumentStore(storeCreationOptions);
            }

            while (DateTime.Now.Second > 10)
                await Task.Delay(1_000);

            using (var session = documentStores[leaderNode.ServerStore.NodeTag].OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Egor" }, "foo/bar");
                await session.SaveChangesAsync();
            }

            await WaitAndAssertForValueAsync(IdleCount, expectedVal: 3, timeout: (int)_reasonableWaitTime.TotalMilliseconds, interval: 1_000);

            var putConfiguration = new ServerWideBackupConfiguration
            {
                FullBackupFrequency = "* * * * *",
                LocalSettings = new LocalSettings { FolderPath = backupPath },
            };

            // Check if put server wide backup configuration will wake up the database
            var putServerWideBackupConfigurationResponse = await documentStores[leaderNode.ServerStore.NodeTag].Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
            var serverWideConfiguration = await documentStores[leaderNode.ServerStore.NodeTag].Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(putServerWideBackupConfigurationResponse.Name));
            Assert.NotNull(serverWideConfiguration);
            var databaseRecord = await documentStores[leaderNode.ServerStore.NodeTag].Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(documentStores[leaderNode.ServerStore.NodeTag].Database));

            var periodicBackups = databaseRecord.PeriodicBackups;
            long taskId = periodicBackups.First().TaskId;
            Assert.Equal(taskId, serverWideConfiguration.TaskId);
            Assert.Equal(1, periodicBackups.Count);
            Assert.Equal(3, IdleCount());

            // wait for the backup occurrence
            Assert.True(3 == await WaitForValueAsync(() =>
            {
                perNodeBackupStatuses = new Dictionary<string, PeriodicBackupStatus>();
                foreach ((string nodeTag, DocumentStore store) in documentStores)
                {
                    var status = store.Maintenance.Send(new GetPeriodicBackupStatusOperation(taskId)).Status;
                    perNodeBackupStatuses[nodeTag] = status;
                }

                return perNodeBackupStatuses.Values.Count(status => status?.LastFullBackup != null);
            },
                expectedVal: 3, timeout: 75_000, interval: 1_000), userMessage: CollectDiagnostics());

            Assert.Equal(2, IdleCount());

            return;
            int IdleCount() => nodes.Sum(server => server.ServerStore.IdleDatabases.Count);
            string CollectDiagnostics()
            {
                var sb = new StringBuilder();
                foreach ((string nodeTag, DocumentStore store) in documentStores)
                {
                    var status = store.Maintenance.Send(new GetPeriodicBackupStatusOperation(taskId)).Status;
                    sb.AppendLine($"Node `{nodeTag}`, backup status:{Environment.NewLine}{BackupTestBase.PrintBackupStatusAndResult(status, result: null)}");
                    sb.AppendLine();
                }
                return sb.ToString();
            }
        }

        // RDBCL-1478
        [NightlyBuildFact]
        public async Task DatabaseWithBackupTaskShouldNotGetIdleBeforeBackupOccurrence()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                    [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
                }
            });

            using var dispose = new DisposableAction(() =>
            {
                server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = false;
                server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipShouldContinueDisposeCheck = false;
            });

            server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;
            server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipShouldContinueDisposeCheck = true;

            using var store = GetDocumentStore(new Options { Server = server, RunInMemory = false });
            while (DateTime.Now.Second > 10)
                await Task.Delay(1_000);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "EGOR" }, "su/1");
                await session.SaveChangesAsync();
            }

            var putServerWideBackupConfigurationResponse = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
            {
                FullBackupFrequency = "* * * * *",
                LocalSettings = new LocalSettings { FolderPath = backupPath }
            }));
            var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(putServerWideBackupConfigurationResponse.Name));
            Assert.NotNull(serverWideConfiguration);
            var backupTaskId = serverWideConfiguration.TaskId;

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "LEV" }, "su/2");
                await session.SaveChangesAsync();
            }

            WaitForUserToContinueTheTest(store);

            var getPeriodicBackupStatusOperation = new GetPeriodicBackupStatusOperation(backupTaskId);
            Assert.True(1 <= await WaitForGreaterThanAsync(async () =>
            {
                Assert.Equal(0, server.ServerStore.IdleDatabases.Count);
                var status = await store.Maintenance.SendAsync(getPeriodicBackupStatusOperation);
                return status?.Status?.LastEtag ?? 0;
            }, val: 1, timeout: 75000, interval: 300));

            Assert.Equal(1, WaitForValue(() => server.ServerStore.IdleDatabases.Count, 1, timeout: 60_000, interval: 3_000));
        }

        internal static int WaitForCount(TimeSpan seconds, int excepted, Func<int> func)
        {
            var now = DateTime.Now;
            var nextNow = now + seconds;
            var count = func();
            while (now < nextNow && count != excepted)
            {
                Thread.Sleep(500);
                now = DateTime.Now;
                count = func();
            }

            return count;
        }




    }
}
