using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests.DatabaseCluster
{
    public class OngoingTasks : ReplicationTestBase
    {
        public OngoingTasks(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanGetTaskInfo(Options options)
        {
            var clusterSize = 3;
            var (_, leader) = await CreateRaftCluster(clusterSize, watcherCluster: true);
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
            options.Server = leader;
            options.ReplicationFactor = clusterSize;
            using (var store = GetDocumentStore(options))
            {
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
                    FactoryName = "Microsoft.Data.SqlClient"
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

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanGetTaskInfoByName(Options options)
        {
            var clusterSize = 3;
            var (_, leader) = await CreateRaftCluster(clusterSize, watcherCluster: true);
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
            options.Server = leader;
            options.ReplicationFactor = clusterSize;

            using (var store = GetDocumentStore(options))
            {
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
                    FactoryName = "Microsoft.Data.SqlClient"
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
            
                var replicationResult = (OngoingTaskReplication)await GetTaskInfo((DocumentStore)store, "MyExternalReplication", OngoingTaskType.Replication);

                Assert.Equal(watcher.Database, replicationResult.DestinationDatabase);
                Assert.Equal(watcher.Url, replicationResult.DestinationUrl);
                Assert.Equal(watcher.Name, replicationResult.TaskName);

                var backupResult = (OngoingTaskBackup)await GetTaskInfo((DocumentStore)store, "backup1", OngoingTaskType.Backup);

                Assert.Equal("Local", backupResult.BackupDestinations[0]);
                Assert.Equal("Azure", backupResult.BackupDestinations[1]);
                Assert.Equal("backup1", backupResult.TaskName);
                Assert.Equal(OngoingTaskState.Disabled, backupResult.TaskState);

                var etlResult = (OngoingTaskRavenEtl)await GetTaskInfo((DocumentStore)store, "tesst", OngoingTaskType.RavenEtl);
                Assert.Equal("cs", etlResult.Configuration.ConnectionStringName);
                Assert.Equal("tesst", etlResult.Configuration.Name);
                Assert.Equal("loadAll", etlResult.Configuration.Transforms[0].Name);
                Assert.Equal("loadToUsers(this)", etlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Users", etlResult.Configuration.Transforms[0].Collections[0]);
                Assert.Equal(etlConfiguration.Name, etlResult?.TaskName);

                var sqlResult = (OngoingTaskSqlEtl)await GetTaskInfo((DocumentStore)store, "abc", OngoingTaskType.SqlEtl);
                Assert.Equal("abc", sqlResult.Configuration.ConnectionStringName);
                Assert.Equal("abc", sqlResult.Configuration.Name);
                Assert.Equal("OrdersAndLines", sqlResult.Configuration.Transforms[0].Name);
                Assert.Equal(sqlScript, sqlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Orders", sqlResult.Configuration.Transforms[0].Collections[0]);
                Assert.NotNull(sqlResult.Configuration.SqlTables);
                Assert.Equal(sqlConfiguration.Name, sqlResult?.TaskName);
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanToggleTaskState(Options options)
        {
            var clusterSize = 3;
            var (_, leader) = await CreateRaftCluster(clusterSize, watcherCluster: true);
            ModifyOngoingTaskResult addWatcherRes;
            UpdatePeriodicBackupOperationResult updateBackupResult;
            options.ReplicationFactor = clusterSize;
            options.Server = leader;
            using (var store = GetDocumentStore(options))
            {
                var watcher = new ExternalReplication("Watcher1", "Connection");

                addWatcherRes = await AddWatcherToReplicationTopology((DocumentStore)store, watcher, new[] { "http://127.0.0.1:9090" });

                var backupConfig = Backup.CreateBackupConfiguration(backupPath: NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", disabled: true);

                updateBackupResult = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfig));
            
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
