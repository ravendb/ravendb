using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests.DatabaseCluster
{
    public class OngoingTasks : ReplicationTestBase
    {
        public OngoingTasks(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
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
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.AllNodes.Count());
                foreach (var server in Servers)
                {
                    await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
                }
                foreach (var server in Servers)
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                }

                watcher = new ExternalReplication("Watcher1", "Connection")
                {
                    Name = "MyExternalReplication"
                };

                addWatcherRes = await AddWatcherToReplicationTopology((DocumentStore)store, watcher, new[] { leader.WebUrl });

                var backupConfig = Backup.CreateBackupConfiguration(backupPath: NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", azureSettings: new AzureSettings
                {
                    StorageContainer = "abc"
                }, disabled: true, name: "backup1");

                updateBackupResult = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfig));

                var result = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = "cs",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = "Northwind",
                }));
                Assert.NotNull(result.RaftCommandIndex);

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

                addRavenEtlResult = store.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(etlConfiguration));

                sqlConnectionString = new SqlConnectionString
                {
                    Name = "abc",
                    ConnectionString = @"Data Source=localhost\sqlexpress;Integrated Security=SSPI;Connection Timeout=3" + $";Initial Catalog=SqlReplication-{store.Database};",
                    FactoryName = "System.Data.SqlClient"
                };
                var result2 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result2.RaftCommandIndex);

                sqlConfiguration = new SqlEtlConfiguration()
                {
                    Name = "abc",
                    ConnectionStringName = "abc",
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
                addSqlEtlResult = store.Maintenance.Send(new AddEtlOperation<SqlConnectionString>(sqlConfiguration));
            }

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
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

                var etlResult = (OngoingTaskRavenEtlDetails)await GetTaskInfo((DocumentStore)store, taskId, OngoingTaskType.RavenEtl);
                Assert.Equal("cs", etlResult.Configuration.ConnectionStringName);
                Assert.Equal("tesst", etlResult.Configuration.Name);
                Assert.Equal("loadAll", etlResult.Configuration.Transforms[0].Name);
                Assert.Equal("loadToUsers(this)", etlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Users", etlResult.Configuration.Transforms[0].Collections[0]);
                Assert.Equal(etlConfiguration.Name, etlResult?.TaskName);

                taskId = addSqlEtlResult.TaskId;

                var sqlResult = (OngoingTaskSqlEtlDetails)await GetTaskInfo((DocumentStore)store, taskId, OngoingTaskType.SqlEtl);
                Assert.Equal("abc", sqlResult.Configuration.ConnectionStringName);
                Assert.Equal("abc", sqlResult.Configuration.Name);
                Assert.Equal("OrdersAndLines", sqlResult.Configuration.Transforms[0].Name);
                Assert.Equal(sqlScript, sqlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Orders", sqlResult.Configuration.Transforms[0].Collections[0]);
                Assert.NotNull(sqlResult.Configuration.SqlTables);
                Assert.Equal(sqlConfiguration.Name, sqlResult?.TaskName);
            }
        }

        [Fact]
        public async Task CanGetTaskInfoByName()
        {
            var clusterSize = 3;
            var databaseName = "TestDB";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
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
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.AllNodes.Count());
                foreach (var server in Servers)
                {
                    await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
                }
                foreach (var server in Servers)
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                }

                watcher = new ExternalReplication("Watcher1", "Connection")
                {
                    Name = "MyExternalReplication"
                };

                await AddWatcherToReplicationTopology((DocumentStore)store, watcher, new[] { leader.WebUrl });

                var backupConfig = Backup.CreateBackupConfiguration(backupPath: NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", azureSettings: new AzureSettings
                {
                    StorageContainer = "abc"
                }, disabled: true, name: "backup1");

                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfig));

                var result = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = "cs",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = "Northwind",
                }));
                Assert.NotNull(result.RaftCommandIndex);

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

                store.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(etlConfiguration));

                sqlConnectionString = new SqlConnectionString
                {
                    Name = "abc",
                    ConnectionString = @"Data Source=localhost\sqlexpress;Integrated Security=SSPI;Connection Timeout=3" + $";Initial Catalog=SqlReplication-{store.Database};",
                    FactoryName = "System.Data.SqlClient"
                };
                var result2 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result2.RaftCommandIndex);

                sqlConfiguration = new SqlEtlConfiguration()
                {
                    Name = "abc",
                    ConnectionStringName = "abc",
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
                store.Maintenance.Send(new AddEtlOperation<SqlConnectionString>(sqlConfiguration));
            }

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var replicationResult = (OngoingTaskReplication)await GetTaskInfo((DocumentStore)store, "MyExternalReplication", OngoingTaskType.Replication);

                Assert.Equal(watcher.Database, replicationResult.DestinationDatabase);
                Assert.Equal(watcher.Url, replicationResult.DestinationUrl);
                Assert.Equal(watcher.Name, replicationResult.TaskName);

                var backupResult = (OngoingTaskBackup)await GetTaskInfo((DocumentStore)store, "backup1", OngoingTaskType.Backup);

                Assert.Equal("Local", backupResult.BackupDestinations[0]);
                Assert.Equal("Azure", backupResult.BackupDestinations[1]);
                Assert.Equal("backup1", backupResult.TaskName);
                Assert.Equal(OngoingTaskState.Disabled, backupResult.TaskState);

                var etlResult = (OngoingTaskRavenEtlDetails)await GetTaskInfo((DocumentStore)store, "tesst", OngoingTaskType.RavenEtl);
                Assert.Equal("cs", etlResult.Configuration.ConnectionStringName);
                Assert.Equal("tesst", etlResult.Configuration.Name);
                Assert.Equal("loadAll", etlResult.Configuration.Transforms[0].Name);
                Assert.Equal("loadToUsers(this)", etlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Users", etlResult.Configuration.Transforms[0].Collections[0]);
                Assert.Equal(etlConfiguration.Name, etlResult?.TaskName);

                var sqlResult = (OngoingTaskSqlEtlDetails)await GetTaskInfo((DocumentStore)store, "abc", OngoingTaskType.SqlEtl);
                Assert.Equal("abc", sqlResult.Configuration.ConnectionStringName);
                Assert.Equal("abc", sqlResult.Configuration.Name);
                Assert.Equal("OrdersAndLines", sqlResult.Configuration.Transforms[0].Name);
                Assert.Equal(sqlScript, sqlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Orders", sqlResult.Configuration.Transforms[0].Collections[0]);
                Assert.NotNull(sqlResult.Configuration.SqlTables);
                Assert.Equal(sqlConfiguration.Name, sqlResult?.TaskName);
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
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.AllNodes.Count());
                foreach (var server in Servers)
                {
                    await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
                }
                foreach (var server in Servers)
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                }

                var watcher = new ExternalReplication("Watcher1", "Connection");

                addWatcherRes = await AddWatcherToReplicationTopology((DocumentStore)store, watcher, new[] { "http://127.0.0.1:9090" });

                var backupConfig = Backup.CreateBackupConfiguration(backupPath: NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", disabled: true);

                updateBackupResult = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfig));
            }

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var taskId = addWatcherRes.TaskId;
                var op = new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.Replication, true);
                var res = await store.Maintenance.SendAsync(op);
                Assert.NotNull(res);
                Assert.True(res.RaftCommandIndex > 0);
                Assert.True(res.TaskId > 0);

                var result = await GetTaskInfo((DocumentStore)store, taskId, OngoingTaskType.Replication);
                Assert.Equal(OngoingTaskState.Disabled, result.TaskState);

                taskId = updateBackupResult.TaskId;
                op = new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.Backup, false);
                res = await store.Maintenance.SendAsync(op);
                Assert.NotNull(res);
                Assert.True(res.RaftCommandIndex > 0);
                Assert.True(res.TaskId > 0);

                result = await GetTaskInfo((DocumentStore)store, taskId, OngoingTaskType.Backup);
                Assert.Equal(OngoingTaskState.Enabled, result.TaskState);

            }
        }
    }
}
