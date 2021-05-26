using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class OnGoingTask : RavenTestBase
    {
        public OnGoingTask(ITestOutputHelper output) : base(output)
        {
        }

        [LicenseRequiredFact, Trait("Category", "Smuggler")]
        public void GetBackupTaskInfo()
        {
            var backupConfig = Backup.CreateBackupConfiguration(backupPath: NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", azureSettings: new AzureSettings
            {
                StorageContainer = "abc"
            }, disabled: true, name: "backup1");

            using (var store = GetDocumentStore())
            {
                var updateBackupResult = store.Maintenance.Send(new UpdatePeriodicBackupOperation(backupConfig));

                var taskId = updateBackupResult.TaskId;

                var op = new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup);
                var backupResult = (OngoingTaskBackup)store.Maintenance.Send(op);
                
                Assert.Equal("Local", backupResult.BackupDestinations[0]);
                Assert.Equal("Azure", backupResult.BackupDestinations[1]);
                Assert.Equal("backup1", backupResult.TaskName);
                Assert.Equal(OngoingTaskState.Disabled, backupResult.TaskState);

                op = new GetOngoingTaskInfoOperation("backup1", OngoingTaskType.Backup);
                backupResult = (OngoingTaskBackup)store.Maintenance.Send(op);

                Assert.Equal("Local", backupResult.BackupDestinations[0]);
                Assert.Equal("Azure", backupResult.BackupDestinations[1]);
                Assert.Equal(taskId, backupResult.TaskId);
                Assert.Equal(OngoingTaskState.Disabled, backupResult.TaskState);
            }
        }

        [LicenseRequiredFact]
        public void GetExternalReplicationTaskInfo()
        {
            var watcher = new ExternalReplication("Watcher1", "Connection")
            {
                Name = "MyExternalReplication"
            };

            using (var store = GetDocumentStore())
            {
                var result = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = watcher.ConnectionStringName,
                    Database = watcher.Database,
                    TopologyDiscoveryUrls = store.Urls
                }));
                Assert.NotNull(result.RaftCommandIndex);

                var replicationOperation = new UpdateExternalReplicationOperation(watcher);
                var replication = store.Maintenance.Send(replicationOperation);
                
                var taskId = replication.TaskId;

                var op = new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Replication);
                var replicationResult = (OngoingTaskReplication)store.Maintenance.Send(op);

                Assert.Equal(watcher.Database, replicationResult.DestinationDatabase);
                Assert.Equal(watcher.Url, replicationResult.DestinationUrl);
                Assert.Equal(watcher.Name, replicationResult.TaskName);

                op = new GetOngoingTaskInfoOperation("MyExternalReplication", OngoingTaskType.Replication);
                replicationResult = (OngoingTaskReplication)store.Maintenance.Send(op);

                Assert.Equal(watcher.Database, replicationResult.DestinationDatabase);
                Assert.Equal(watcher.Url, replicationResult.DestinationUrl);
                Assert.Equal(taskId, replicationResult.TaskId);
            }
        }

        [LicenseRequiredFact]
        public void GetRavenEtlTaskInfo()
        {
            var etlConfiguration = new RavenEtlConfiguration()
            {
                Name = "test",
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

            using (var store = GetDocumentStore())
            {
                var result = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = "cs",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = "Northwind",
                }));
                Assert.NotNull(result.RaftCommandIndex);

                var ravenEtlResult = store.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(etlConfiguration));

                var taskId = ravenEtlResult.TaskId;

                var op = new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.RavenEtl);
                var etlResult = (OngoingTaskRavenEtlDetails)store.Maintenance.Send(op);

                Assert.Equal("cs", etlResult.Configuration.ConnectionStringName);
                Assert.Equal("test", etlResult.Configuration.Name);
                Assert.Equal("loadAll", etlResult.Configuration.Transforms[0].Name);
                Assert.Equal("loadToUsers(this)", etlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Users", etlResult.Configuration.Transforms[0].Collections[0]);
                Assert.Equal(etlConfiguration.Name, etlResult?.TaskName);

                op = new GetOngoingTaskInfoOperation("test", OngoingTaskType.RavenEtl);
                etlResult = (OngoingTaskRavenEtlDetails)store.Maintenance.Send(op);

                Assert.Equal("cs", etlResult.Configuration.ConnectionStringName);
                Assert.Equal(taskId, etlResult.TaskId);
                Assert.Equal("loadAll", etlResult.Configuration.Transforms[0].Name);
                Assert.Equal("loadToUsers(this)", etlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Users", etlResult.Configuration.Transforms[0].Collections[0]);
                Assert.Equal(etlConfiguration.Name, etlResult?.TaskName);
            }
        }

        [LicenseRequiredFact]
        public void GetSqlEtlTaskInfo()
        {
            var sqlScript = @"
var orderData = {
    Id: __document_id,
    OrderLinesCount: this.OrderLines.length,
    TotalCost: 0
};

loadToOrders(orderData);
";

            var sqlConfiguration = new SqlEtlConfiguration()
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

            using (var store = GetDocumentStore())
            {
                var sqlConnectionString = new SqlConnectionString
                {
                    Name = "abc",
                    ConnectionString = @"Data Source=localhost\sqlexpress;Integrated Security=SSPI;Connection Timeout=3" + $";Initial Catalog=SqlReplication-{store.Database};",
                    FactoryName = "System.Data.SqlClient"
                };

                var result = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result.RaftCommandIndex);

                var sqlEtlResult = store.Maintenance.Send(new AddEtlOperation<SqlConnectionString>(sqlConfiguration));

                var taskId = sqlEtlResult.TaskId;

                var op = new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.SqlEtl);
                var sqlResult = (OngoingTaskSqlEtlDetails)store.Maintenance.Send(op);

                Assert.Equal("abc", sqlResult.Configuration.ConnectionStringName);
                Assert.Equal("abc", sqlResult.Configuration.Name);
                Assert.Equal("OrdersAndLines", sqlResult.Configuration.Transforms[0].Name);
                Assert.Equal(sqlScript, sqlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Orders", sqlResult.Configuration.Transforms[0].Collections[0]);
                Assert.NotNull(sqlResult.Configuration.SqlTables);
                Assert.Equal(sqlConfiguration.Name, sqlResult?.TaskName);

                op = new GetOngoingTaskInfoOperation("abc", OngoingTaskType.SqlEtl);
                sqlResult = (OngoingTaskSqlEtlDetails)store.Maintenance.Send(op);

                Assert.Equal("abc", sqlResult.Configuration.ConnectionStringName);
                Assert.Equal("abc", sqlResult.Configuration.Name);
                Assert.Equal("OrdersAndLines", sqlResult.Configuration.Transforms[0].Name);
                Assert.Equal(sqlScript, sqlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Orders", sqlResult.Configuration.Transforms[0].Collections[0]);
                Assert.NotNull(sqlResult.Configuration.SqlTables);
                Assert.Equal(taskId, sqlResult.TaskId);
            }
        }

        [Fact]
        public void GetSubscriptionTaskInfo()
        {
            var subscriptionOption = new SubscriptionCreationOptions<Query.Order>
            {
                Name  = "sub",
                Filter = x => x.Employee.StartsWith("e"),
                MentorNode = "B"
            };

            using (var store = GetDocumentStore())
            {
                var sub = store.Subscriptions.Create(subscriptionOption);
                
                var op = new GetOngoingTaskInfoOperation(subscriptionOption.Name, OngoingTaskType.Subscription);
                var subscriptionResult = (OngoingTaskSubscription)store.Maintenance.Send(op);

                Assert.Equal(subscriptionOption.MentorNode, subscriptionResult.MentorNode);
                Assert.NotNull(subscriptionResult.Query);

                var state = store.Subscriptions.GetSubscriptionState(subscriptionOption.Name);
                op = new GetOngoingTaskInfoOperation(state.SubscriptionId, OngoingTaskType.Subscription);
                subscriptionResult = (OngoingTaskSubscription)store.Maintenance.Send(op);

                Assert.Equal(subscriptionOption.MentorNode, subscriptionResult.MentorNode);
                Assert.Equal(subscriptionOption.Name, subscriptionResult.SubscriptionName);
                Assert.NotNull(subscriptionResult.Query);

                op = new GetOngoingTaskInfoOperation(state.SubscriptionId - 1, OngoingTaskType.Subscription);
                Assert.Throws<SubscriptionDoesNotExistException>(() => store.Maintenance.Send(op));

            }
        }
    }
}
