using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Raven.Client.Server.Operations;
using Raven.Client.Server.Operations.ConnectionStrings;
using Raven.Client.Server.Operations.ETL;
using Raven.Client.Server.PeriodicBackup;
using Xunit;

namespace RachisTests.DatabaseCluster
{
    public class OngoingTasks : ReplicationTestsBase
    {
        [Fact]
        public async Task CanGetTaskInfo()
        {
            var clusterSize = 3;
            var databaseName = "TestDB";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
            ModifyOngoingTaskResult addWatcherRes;
            UpdatePeriodicBackupOperationResult updateBackupResult;
            AddEtlOperationResult addEtlREsult;
            RavenEtlConfiguration etlConfiguration;
            ExternalReplication watcher;
            RavenConnectionString ravenConnectionString;

            using (var store = new DocumentStore
            {
                Urls = leader.WebUrls,
                Database = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.AllNodes.Count());
                foreach (var server in Servers)
                {
                    await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
                }
                foreach (var server in Servers)
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                }

                watcher = new ExternalReplication
                {
                    Database = "Watcher1",
                    Url = "http://127.0.0.1:9090",
                    Name = "MyExternalReplication"
                };

                addWatcherRes = await AddWatcherToReplicationTopology((DocumentStore)store, watcher);

                var backupConfig = new PeriodicBackupConfiguration
                {
                    Name = "backup1",
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = NewDataPath(suffix: "BackupFolder")
                    },
                    AzureSettings = new AzureSettings
                    {
                        StorageContainer = "abc"
                    },
                    FullBackupFrequency = "* */1 * * *",
                    IncrementalBackupFrequency = "* */2 * * *",
                    Disabled = true
                };

                updateBackupResult = await store.Admin.Server.SendAsync(new UpdatePeriodicBackupOperation(backupConfig, store.Database));

                ravenConnectionString = new RavenConnectionString()
                {
                    Name = "cs",
                    Url = "http://127.0.0.1:8080",
                    Database = "Northwind",
                };
                store.Admin.Server.Send(new AddConnectionStringOperation<RavenConnectionString>(ravenConnectionString, store.Database));

                etlConfiguration = new RavenEtlConfiguration()
                {
                    Name = "tesst",
                    ConnectionStringName = "cs",
                    Transforms =
                    {
                        new Transformation()
                        {
                            Collections = {"Users"}
                        }
                    }
                };

                addEtlREsult = store.Admin.Server.Send(new AddEtlOperation<RavenConnectionString>(etlConfiguration, store.Database));
            }

            using (var store = new DocumentStore
            {
                Urls = leader.WebUrls,
                Database = databaseName,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            { 
                var taskId = addWatcherRes.TaskId;
                var result = await GetTaskInfo((DocumentStore)store, taskId, OngoingTaskType.Replication);

                Assert.Equal(watcher.Database, result.DestinationDatabase);
                Assert.Equal(watcher.Url, result.DestinationUrl);
                Assert.Equal(watcher.Name, result.TaskName);

                taskId = updateBackupResult.TaskId;
                result = await GetTaskInfo((DocumentStore)store, taskId, OngoingTaskType.Backup);

                Assert.Equal("Local", result.BackupDestinations[0]);
                Assert.Equal("Azure", result.BackupDestinations[1]);
                Assert.Equal("backup1", result.TaskName);
                Assert.Equal(OngoingTaskState.Disabled, result.TaskState);

                taskId = addEtlREsult.TaskId;

                result = await GetTaskInfo((DocumentStore)store, taskId, OngoingTaskType.RavenEtl);              
                Assert.Equal(result?.DestinationDatabase, ravenConnectionString.Database);
                Assert.Equal(result?.DestinationUrl, ravenConnectionString.Url);
                Assert.Equal(result?.TaskName, etlConfiguration.Name);
            }
        }

        [Fact]
        public async Task CanToggleTaskState()
        {
            var clusterSize = 3;
            var databaseName = "TestDB";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
            ModifyOngoingTaskResult addWatcherRes;
            UpdatePeriodicBackupOperationResult updateBackupResult;

            using (var store = new DocumentStore
            {
                Urls = leader.WebUrls,
                Database = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.AllNodes.Count());
                foreach (var server in Servers)
                {
                    await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
                }
                foreach (var server in Servers)
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                }

                var watcher = new ExternalReplication
                {
                    Database = "Watcher1",
                    Url = "http://127.0.0.1:9090"
                };

                addWatcherRes = await AddWatcherToReplicationTopology((DocumentStore)store, watcher);

                var backupConfig = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = NewDataPath(suffix: "BackupFolder")
                    },
                    FullBackupFrequency = "* */1 * * *",
                    IncrementalBackupFrequency = "* */2 * * *",
                    Disabled = true
                };

                updateBackupResult = await store.Admin.Server.SendAsync(new UpdatePeriodicBackupOperation(backupConfig, store.Database));
            }

            using (var store = new DocumentStore
            {
                Urls = leader.WebUrls,
                Database = databaseName,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var taskId = addWatcherRes.TaskId;
                var op = new ToggleTaskStateOperation(store.Database, taskId, OngoingTaskType.Replication, true);
                await store.Admin.Server.SendAsync(op);

                var result = await GetTaskInfo((DocumentStore)store, taskId, OngoingTaskType.Replication);
                Assert.Equal(OngoingTaskState.Disabled, result.TaskState);

                taskId = updateBackupResult.TaskId;
                op = new ToggleTaskStateOperation(store.Database, taskId, OngoingTaskType.Backup, false);
                await store.Admin.Server.SendAsync(op);

                result = await GetTaskInfo((DocumentStore)store, taskId, OngoingTaskType.Backup);
                Assert.Equal(OngoingTaskState.Enabled, result.TaskState);

            }
        }
    }
}
