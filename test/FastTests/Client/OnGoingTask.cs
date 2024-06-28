using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Serialization;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class OnGoingTask : RavenTestBase
    {
        public OnGoingTask(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Studio | RavenTestCategory.ClientApi | RavenTestCategory.BackupExportImport, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void GetBackupTaskInfo(Options options)
        {
            var backupConfig = Backup.CreateBackupConfiguration(backupPath: NewDataPath(suffix: "BackupFolder"), fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *", azureSettings: new AzureSettings
            {
                StorageContainer = "abc"
            }, disabled: true, name: "backup1");

            using (var store = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Studio | RavenTestCategory.ClientApi | RavenTestCategory.Replication, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task GetExternalReplicationTaskInfo(Options options)
        {
            var destDatabaseName = GetDatabaseName();
            var watcher = new ExternalReplication(destDatabaseName, "Connection")
            {
                Name = "MyExternalReplication"
            };

            using (var store = GetDocumentStore(options))
            using (var dest = GetDocumentStore(new Options()
                   {
                       ModifyDatabaseName = _ => destDatabaseName
                   }))
            {
                var result = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = watcher.ConnectionStringName,
                    Database = watcher.Database,
                    TopologyDiscoveryUrls = dest.Urls
                }));
                Assert.NotNull(result.RaftCommandIndex);

                var replicationOperation = new UpdateExternalReplicationOperation(watcher);
                var replication = store.Maintenance.Send(replicationOperation);
                
                var taskId = replication.TaskId;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                var res = WaitForValue(() =>
                {
                    using (var session = dest.OpenSession())
                    {
                        try
                        {
                            return session.Load<User>("users/1") != null;
                        }
                        catch
                        {
                            return false;
                        }
                    }
                }, true);
                
                Assert.True(res);

                if (options.DatabaseMode == RavenDatabaseMode.Sharded)
                {
                    var shards = await Sharding.GetShardingConfigurationAsync(store);
                    foreach (var shardNumber in shards.Shards.Keys)
                    {
                        ValidateReplicationTaskDetails(store.Maintenance.ForShard(shardNumber), taskId, watcher, dest);
                    }
                    return;
                }

                ValidateReplicationTaskDetails(store.Maintenance, taskId, watcher, dest);
            }
        }

        private static void ValidateReplicationTaskDetails(MaintenanceOperationExecutor executor, long taskId, ExternalReplication watcher, DocumentStore dest)
        {
            var op = new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Replication);
            var replicationResult = (OngoingTaskReplication)executor.Send(op);

            Assert.Equal(watcher.Database, replicationResult.DestinationDatabase);
            Assert.Equal(dest.Urls.Single(), replicationResult.DestinationUrl);
            Assert.Equal(watcher.Name, replicationResult.TaskName);

            op = new GetOngoingTaskInfoOperation("MyExternalReplication", OngoingTaskType.Replication);
            replicationResult = (OngoingTaskReplication)executor.Send(op);

            Assert.Equal(watcher.Database, replicationResult.DestinationDatabase);
            Assert.Equal(dest.Urls.Single(), replicationResult.DestinationUrl);
            Assert.Equal(taskId, replicationResult.TaskId);
        }

        [RavenTheory(RavenTestCategory.Studio | RavenTestCategory.ClientApi | RavenTestCategory.Etl, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void GetRavenEtlTaskInfo(Options options) => GetRavenEtlTaskInfoBase(GetDocumentStore(options));
        
        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Etl, LicenseRequired = true)]
        public void GetRavenEtlTaskInfoWithCustomConventions() => GetRavenEtlTaskInfoBase(GetDocumentStore(new Options()
        {
            ModifyDocumentStore = documentStore =>
            {
                documentStore.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                {
                    CustomizeJsonSerializer = (serializer) =>
                    {
                        serializer.ContractResolver = new CamelCasePropertyNamesContractResolver();
                    }
                };
                documentStore.Conventions.PropertyNameConverter = mi => $"{Char.ToLower(mi.Name[0])}{mi.Name.Substring(1)}";
                documentStore.Conventions.ShouldApplyPropertyNameConverter = info => true;
            }
        }));
        
        private void GetRavenEtlTaskInfoBase(IDocumentStore store)
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

            using (store)
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
                var etlResult = (OngoingTaskRavenEtl)store.Maintenance.Send(op);

                Assert.Equal("cs", etlResult.Configuration.ConnectionStringName);
                Assert.Equal("test", etlResult.Configuration.Name);
                Assert.Equal("loadAll", etlResult.Configuration.Transforms[0].Name);
                Assert.Equal("loadToUsers(this)", etlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Users", etlResult.Configuration.Transforms[0].Collections[0]);
                Assert.Equal(etlConfiguration.Name, etlResult?.TaskName);

                op = new GetOngoingTaskInfoOperation("test", OngoingTaskType.RavenEtl);
                etlResult = (OngoingTaskRavenEtl)store.Maintenance.Send(op);

                Assert.Equal("cs", etlResult.Configuration.ConnectionStringName);
                Assert.Equal(taskId, etlResult.TaskId);
                Assert.Equal("loadAll", etlResult.Configuration.Transforms[0].Name);
                Assert.Equal("loadToUsers(this)", etlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Users", etlResult.Configuration.Transforms[0].Collections[0]);
                Assert.Equal(etlConfiguration.Name, etlResult?.TaskName);
            }
        }

        [RavenTheory(RavenTestCategory.Studio | RavenTestCategory.ClientApi | RavenTestCategory.Etl, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void GetSqlEtlTaskInfo(Options options)
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

            using (var store = GetDocumentStore(options))
            {
                var sqlConnectionString = new SqlConnectionString
                {
                    Name = "abc",
                    ConnectionString = @"Data Source=localhost\sqlexpress;Integrated Security=SSPI;Connection Timeout=3" + $";Initial Catalog=SqlReplication-{store.Database};",
                    FactoryName = "Microsoft.Data.SqlClient"
                };

                var result = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result.RaftCommandIndex);

                var sqlEtlResult = store.Maintenance.Send(new AddEtlOperation<SqlConnectionString>(sqlConfiguration));

                var taskId = sqlEtlResult.TaskId;

                var op = new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.SqlEtl);
                var sqlResult = (OngoingTaskSqlEtl)store.Maintenance.Send(op);

                Assert.Equal("abc", sqlResult.Configuration.ConnectionStringName);
                Assert.Equal("abc", sqlResult.Configuration.Name);
                Assert.Equal("OrdersAndLines", sqlResult.Configuration.Transforms[0].Name);
                Assert.Equal(sqlScript, sqlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Orders", sqlResult.Configuration.Transforms[0].Collections[0]);
                Assert.NotNull(sqlResult.Configuration.SqlTables);
                Assert.Equal(sqlConfiguration.Name, sqlResult?.TaskName);

                op = new GetOngoingTaskInfoOperation("abc", OngoingTaskType.SqlEtl);
                sqlResult = (OngoingTaskSqlEtl)store.Maintenance.Send(op);

                Assert.Equal("abc", sqlResult.Configuration.ConnectionStringName);
                Assert.Equal("abc", sqlResult.Configuration.Name);
                Assert.Equal("OrdersAndLines", sqlResult.Configuration.Transforms[0].Name);
                Assert.Equal(sqlScript, sqlResult.Configuration.Transforms[0].Script);
                Assert.Equal("Orders", sqlResult.Configuration.Transforms[0].Collections[0]);
                Assert.NotNull(sqlResult.Configuration.SqlTables);
                Assert.Equal(taskId, sqlResult.TaskId);
            }
        }

        [RavenTheory(RavenTestCategory.Studio | RavenTestCategory.ClientApi | RavenTestCategory.Subscriptions, LicenseRequired = true)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void GetSubscriptionTaskInfo(Options options)
        {
            var subscriptionOption = new SubscriptionCreationOptions<Order>
            {
                Name  = "sub",
                Filter = x => x.Employee.StartsWith("e"),
                MentorNode = "B"
            };

            using (var store = GetDocumentStore(options))
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
