using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Client.ServerWide.Operations.ETL;
using Raven.Client.ServerWide.PeriodicBackup;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests.DatabaseCluster
{
    public class OngoingTasks : ReplicationTestBase
    {
        [NightlyBuildFact]
        public async Task CanGetTaskInfo()
        {
            var clusterSize = 3;
            var databaseName = "TestDB";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
            ModifyOngoingTaskResult addWatcherRes;
            UpdatePeriodicBackupOperationResult updateBackupResult;
            AddEtlOperationResult addRavenEtlResult;
            AddEtlOperationResult addSqlEtlResult;
            RavenEtlConfiguration etlConfiguration;
            SqlEtlConfiguration sqlConfiguration;
            ExternalReplication watcher;
            SqlConnectionString sqlConnectionString;

            var sqlScript = @"
var orderData = {
    Id: __document_id,
    OrderLinesCount: this.OrderLines.length,
    TotalCost: 0
};

loadToOrders(orderData);
";

            using (var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
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

                watcher = new ExternalReplication(new []{ "http://127.0.0.1:9090" })
                {
                    Database = "Watcher1",
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

                store.Admin.Server.Send(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = "cs",
                    Url = "http://127.0.0.1:8080",
                    Database = "Northwind",
                }, store.Database));

                etlConfiguration = new RavenEtlConfiguration()
                {
                    Name = "tesst",
                    ConnectionStringName = "cs",
                    Transforms =
                    {
                        new Transformation()
                        {
                            Name = "loadAll",
                            Collections = {"Users"},
                            Script = "loadToUsers(this)"
                        }
                    }
                };

                addRavenEtlResult = store.Admin.Server.Send(new AddEtlOperation<RavenConnectionString>(etlConfiguration, store.Database));

                sqlConnectionString = new SqlConnectionString
                {
                    Name = "abc",
                    ConnectionString = @"Data Source=localhost\sqlexpress;Integrated Security=SSPI;Connection Timeout=3" + $";Initial Catalog=SqlReplication-{store.Database};"
                };
                store.Admin.Server.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString, store.Database));


                sqlConfiguration = new SqlEtlConfiguration()
                {
                    Name = "abc",
                    ConnectionStringName = "abc",
                    FactoryName = "System.Data.SqlClient",
                    SqlTables =
                    {
                        new SqlEtlTable {TableName = "Orders", DocumentIdColumn = "Id", InsertOnlyMode = false},
                        new SqlEtlTable {TableName = "OrderLines", DocumentIdColumn = "OrderId", InsertOnlyMode = false},
                    },
                    Transforms =
                    {
                        new Transformation()
                        {
                            Name = "OrdersAndLines",
                            Collections =  new List<string> {"Orders"},
                            Script = sqlScript
                        }
                    }
                };
                addSqlEtlResult = store.Admin.Server.Send(new AddEtlOperation<SqlConnectionString>(sqlConfiguration, store.Database));
            }

            using (var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var taskId = addWatcherRes.TaskId;
                var replicationResult = (OngoingTaskReplication)await GetTaskInfo((DocumentStore)store, taskId, OngoingTaskType.Replication);

                Assert.Equal(watcher.Database, replicationResult.DestinationDatabase);
                Assert.Equal(watcher.Url, replicationResult.DestinationUrl);
                Assert.Equal(watcher.Name, replicationResult.TaskName);

                taskId = updateBackupResult.TaskId;
                var backupResult = (OngoingTaskBackup)await GetTaskInfo((DocumentStore)store, taskId, OngoingTaskType.Backup);

                Assert.Equal("Local", backupResult.BackupDestinations[0]);
                Assert.Equal("Azure", backupResult.BackupDestinations[1]);
                Assert.Equal("backup1", backupResult.TaskName);
                Assert.Equal(OngoingTaskState.Disabled, backupResult.TaskState);

                taskId = addRavenEtlResult.TaskId;

                var etlResult = (OngoingTaskRavenEtl)await GetTaskInfo((DocumentStore)store, taskId, OngoingTaskType.RavenEtl);
                Assert.Equal("cs", etlResult.Configuration.ConnectionStringName);
                Assert.Equal("tesst", etlResult.Configuration.Name);
                Assert.Equal("loadAll", etlResult.Configuration.Transforms[0].Name);
                Assert.Equal("loadToUsers(this)", etlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Users", etlResult.Configuration.Transforms[0].Collections[0]);
                Assert.Equal(etlConfiguration.Name, etlResult?.TaskName);

                taskId = addSqlEtlResult.TaskId;

                var sqlResult = (OngoingTaskSqlEtl)await GetTaskInfo((DocumentStore)store, taskId, OngoingTaskType.SqlEtl);
                Assert.Equal("abc", sqlResult.Configuration.ConnectionStringName);
                Assert.Equal("abc", sqlResult.Configuration.Name);
                Assert.Equal("OrdersAndLines", sqlResult.Configuration.Transforms[0].Name);
                Assert.Equal(sqlScript, sqlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Orders", sqlResult.Configuration.Transforms[0].Collections[0]);
                Assert.NotNull(sqlResult.Configuration.SqlTables);
                Assert.Equal(sqlConfiguration.Name, sqlResult?.TaskName);
            }
        }

        [NightlyBuildFact]
        public async Task CanToggleTaskState()
        {
            var clusterSize = 3;
            var databaseName = "TestDB";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
            ModifyOngoingTaskResult addWatcherRes;
            UpdatePeriodicBackupOperationResult updateBackupResult;

            using (var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
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

                var watcher = new ExternalReplication(new []{ "http://127.0.0.1:9090" })
                {
                    Database = "Watcher1",
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
                Urls = new[] {leader.WebUrl},
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
