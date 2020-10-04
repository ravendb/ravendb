using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Replication
{
    public class ServerWideReplication : RavenTestBase
    {
        public ServerWideReplication(ITestOutputHelper output) : base(output)
        {
            DoNotReuseServer();
        }

        [Fact]
        public async Task CanStoreServerWideExternalReplication()
        {
            using (var store = GetDocumentStore())
            {
                var putConfiguration = new ServerWideExternalReplication
                {
                    Disabled = true,
                    TopologyDiscoveryUrls = new[] { store.Urls.First() }
                };

                var result = await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));
                var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationOperation(result.Name));
                Assert.NotNull(serverWideConfiguration);

                ValidateServerWideConfiguration(serverWideConfiguration, putConfiguration);

                // the configuration is applied to existing databases
                var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var externalReplications1 = record1.ExternalReplications;
                Assert.Equal(1, externalReplications1.Count);
                ValidateConfiguration(serverWideConfiguration, externalReplications1.First(), store.Database);

                // the configuration is applied to new databases
                var newDbName = store.Database + "-testDatabase";
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDbName)));
                var externalReplications = record1.ExternalReplications;
                Assert.Equal(1, externalReplications.Count);
                var record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                ValidateConfiguration(serverWideConfiguration, record2.ExternalReplications.First(), newDbName);

                // update the external replication configuration
                putConfiguration.TopologyDiscoveryUrls = new[] {store.Urls.First(), "http://localhost:8080"};
                putConfiguration.Name = serverWideConfiguration.Name;

                result = await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));
                serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationOperation(result.Name));
                ValidateServerWideConfiguration(serverWideConfiguration, putConfiguration);

                record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record1.ExternalReplications.Count);
                ValidateConfiguration(serverWideConfiguration, record1.ExternalReplications.First(), store.Database);

                record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                Assert.Equal(1, record2.ExternalReplications.Count);
                ValidateConfiguration(serverWideConfiguration, record2.ExternalReplications.First(), newDbName);
            }
        }

        [Fact]
        public async Task UpdateServerWideReplicationThroughUpdateReplicationTaskFails()
        {
            using (var store = GetDocumentStore())
            {
                var putConfiguration = new ServerWideExternalReplication
                {
                    Disabled = true,
                    TopologyDiscoveryUrls = new[] { store.Urls.First() },
                    Name = store.Database
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));

                var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var externalReplicationFromDatabaseRecord = databaseRecord.ExternalReplications.First();
                var taskId = externalReplicationFromDatabaseRecord.TaskId;

                var externalReplication = new ExternalReplication
                {
                    Disabled = true,
                    TaskId = externalReplicationFromDatabaseRecord.TaskId,
                    Name = externalReplicationFromDatabaseRecord.Name,
                    ConnectionStringName = externalReplicationFromDatabaseRecord.ConnectionStringName
                };

                var e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(externalReplication)));
                Assert.Contains("A regular (non server-wide) external replication name can't start with prefix 'Server Wide External Replication'", e.Message);

                e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new DeleteOngoingTaskOperation(taskId, OngoingTaskType.Replication)));
                var expectedError = $"Can't delete task id: {taskId}, name: '{externalReplicationFromDatabaseRecord.Name}', because it is a server-wide external replication task";
                Assert.Contains(expectedError, e.Message);

                var recordConnectionString = databaseRecord.RavenConnectionStrings.First().Value;
                e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(recordConnectionString)));
                Assert.Contains("connection string name can't start with prefix 'Server Wide Raven Connection String'", e.Message);
            }
        }

        [Fact]
        public async Task ToggleDisableServerWideExternalReplicationFails()
        {
            using (var store = GetDocumentStore())
            {
                var putConfiguration = new ServerWideExternalReplication
                {
                    Disabled = true,
                    TopologyDiscoveryUrls = new[] { store.Urls.First() }
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));

                var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var externalReplicationFromDatabaseRecord = databaseRecord.ExternalReplications.First();
                var taskId = externalReplicationFromDatabaseRecord.TaskId;
                var taskName = externalReplicationFromDatabaseRecord.Name;

                var e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.Replication, false)));
                Assert.Contains($"Can't enable task name '{taskName}', because it is a server-wide external replication task", e.Message);

                e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(taskId, OngoingTaskType.Replication, true)));
                Assert.Contains($"Can't disable task name '{taskName}', because it is a server-wide external replication task", e.Message);
            }
        }

        [Fact]
        public async Task CanCreateMoreThanOneServerWideExternalReplication()
        {
            using (var store = GetDocumentStore())
            {
                var putConfiguration = new ServerWideExternalReplication
                {
                    Disabled = true,
                    TopologyDiscoveryUrls = new[] { store.Urls.First() }
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));
                await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));
                await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));
                
                var externalReplications = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationsOperation());
                Assert.Equal(3, externalReplications.Length);

                var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(3, databaseRecord.ExternalReplications.Count);

                var toUpdate = externalReplications[1];
                toUpdate.ExcludedDatabases = new[] {"ExcludedDatabase"};
                await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(toUpdate));

                externalReplications = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationsOperation());
                Assert.Equal(3, externalReplications.Length);
                databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(3, databaseRecord.ExternalReplications.Count);

                // new database includes all server-wide external replications
                var newDbName = store.Database + "-testDatabase";
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDbName)));
                databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                Assert.Equal(3, databaseRecord.ExternalReplications.Count);
            }
        }

        [Fact]
        public async Task CanDeleteServerWideExternalReplication()
        {
            using (var store = GetDocumentStore())
            {
                var putConfiguration = new ServerWideExternalReplication
                {
                    Disabled = true,
                    TopologyDiscoveryUrls = new[] { store.Urls.First() }
                };

                var result1 = await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));
                var result2 = await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));

                var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(2, record1.ExternalReplications.Count);
                Assert.Equal(2, record1.RavenConnectionStrings.Count);
                var serverWideExternalReplications = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationsOperation());
                Assert.Equal(2, serverWideExternalReplications.Length);

                // the configuration is applied to new databases
                var newDbName = store.Database + "-testDatabase";
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDbName)));
                var record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                Assert.Equal(2, record2.ExternalReplications.Count);
                Assert.Equal(2, record2.RavenConnectionStrings.Count);

                await store.Maintenance.Server.SendAsync(new DeleteServerWideTaskOperation(result1.Name, OngoingTaskType.Replication));
                var serverWideExternalReplication = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationOperation(result1.Name));
                Assert.Null(serverWideExternalReplication);
                serverWideExternalReplications = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationsOperation());
                Assert.Equal(1, serverWideExternalReplications.Length);

                // verify that the server-wide external replication was deleted from all databases
                record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record1.ExternalReplications.Count);
                Assert.Equal(1, record1.RavenConnectionStrings.Count);
                Assert.Equal($"{ServerWideExternalReplication.NamePrefix}, {putConfiguration.GetDefaultTaskName()} #2", record1.ExternalReplications.First().Name);
                record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                Assert.Equal(1, record2.ExternalReplications.Count);
                Assert.Equal(1, record2.RavenConnectionStrings.Count);
                Assert.Equal($"{ServerWideExternalReplication.NamePrefix}, {putConfiguration.GetDefaultTaskName()} #2", record2.ExternalReplications.First().Name);

                await store.Maintenance.Server.SendAsync(new DeleteServerWideTaskOperation(result2.Name, OngoingTaskType.Replication));
                serverWideExternalReplication = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationOperation(result2.Name));
                Assert.Null(serverWideExternalReplication);
                serverWideExternalReplications = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationsOperation());
                Assert.Equal(0, serverWideExternalReplications.Length);

                record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(0, record1.ExternalReplications.Count);
                Assert.Equal(0, record1.RavenConnectionStrings.Count);
                record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                Assert.Equal(0, record2.ExternalReplications.Count);
                Assert.Equal(0, record2.RavenConnectionStrings.Count);
            }
        }

        [Fact]
        public async Task SkipExportingTheServerWideExternalReplication1()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore())
            {
                var putConfiguration1 = new ServerWideExternalReplication
                {
                    Name = "1",
                    Disabled = true,
                    TopologyDiscoveryUrls = new[] { store.Urls.First() }
                };

                var putConfiguration2 = new ServerWideExternalReplication
                {
                    Name = "2",
                    Disabled = true,
                    TopologyDiscoveryUrls = new[] { store.Urls.First() }
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration1));
                await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration2));

                var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(2, databaseRecord.ExternalReplications.Count);

                var serverWideBackupConfiguration = new ServerWideBackupConfiguration
                {
                    Disabled = false,
                    FullBackupFrequency = "0 2 * * 0",
                    IncrementalBackupFrequency = "0 2 * * 1",
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(serverWideBackupConfiguration));

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var backup = record.PeriodicBackups.First();
                var backupTaskId = backup.TaskId;

                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));

                string backupDirectory = null;
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(new GetPeriodicBackupStatusOperation(backupTaskId)).Status;
                    backupDirectory = status?.LocalBackup.BackupDirectory;
                    return status?.LastEtag;
                }, 0);

                Assert.Equal(0, value);

                var files = Directory.GetFiles(backupDirectory)
                    .Where(BackupUtils.IsBackupFile)
                    .OrderBackups()
                    .ToArray();

                var databaseName = GetDatabaseName() + "restore";
                var restoreConfig = new RestoreBackupConfiguration
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = files.OrderBackups().Last()
                };

                var restoreOperation = new RestoreBackupOperation(restoreConfig);
                store.Maintenance.Server.Send(restoreOperation)
                    .WaitForCompletion(TimeSpan.FromSeconds(30));

                // new server should have only 0 external replications
                var server = GetNewServer();

                using (EnsureDatabaseDeletion(databaseName, store))
                using (var store2 = GetDocumentStore(new Options
                {
                    CreateDatabase = false,
                    ModifyDatabaseName = s => databaseName,
                    Server = server
                }))
                {
                    store2.Maintenance.Server.Send(restoreOperation)
                        .WaitForCompletion(TimeSpan.FromSeconds(30));

                    var record2 = await store2.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    Assert.Equal(0, record2.ExternalReplications.Count);
                }
            }
        }

        [Fact]
        public async Task SkipExportingTheServerWideExternalReplication2()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore())
            {
                var putConfiguration1 = new ServerWideExternalReplication
                {
                    Name = "1",
                    Disabled = true,
                    TopologyDiscoveryUrls = new[] { store.Urls.First() }
                };

                var putConfiguration2 = new ServerWideExternalReplication
                {
                    Name = "2",
                    Disabled = true,
                    TopologyDiscoveryUrls = new[] { store.Urls.First() }
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration1));
                await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration2));

                var dbName = $"db/{Guid.NewGuid()}";
                var csName = $"cs/{Guid.NewGuid()}";

                var connectionString = new RavenConnectionString
                {
                    Name = csName,
                    Database = dbName,
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" }
                };

                var result = await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                Assert.NotNull(result.RaftCommandIndex);

                await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(new ExternalReplication(dbName, csName)
                {
                    Disabled = true
                }));

                var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(3, databaseRecord.ExternalReplications.Count);

                var serverWideBackupConfiguration = new ServerWideBackupConfiguration
                {
                    Disabled = false,
                    FullBackupFrequency = "0 2 * * 0",
                    IncrementalBackupFrequency = "0 2 * * 1",
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(serverWideBackupConfiguration));

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var backup = record.PeriodicBackups.First();
                var backupTaskId = backup.TaskId;

                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));

                string backupDirectory = null;
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(new GetPeriodicBackupStatusOperation(backupTaskId)).Status;
                    backupDirectory = status?.LocalBackup.BackupDirectory;
                    return status?.LastEtag;
                }, 0);

                Assert.Equal(0, value);

                var files = Directory.GetFiles(backupDirectory)
                    .Where(BackupUtils.IsBackupFile)
                    .OrderBackups()
                    .ToArray();

                var databaseName = GetDatabaseName() + "restore";
                var restoreConfig = new RestoreBackupConfiguration
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = files.OrderBackups().Last()
                };

                var restoreOperation = new RestoreBackupOperation(restoreConfig);
                store.Maintenance.Server.Send(restoreOperation)
                    .WaitForCompletion(TimeSpan.FromSeconds(30));

                // new server should have only 0 external replications
                var server = GetNewServer();

                using (EnsureDatabaseDeletion(databaseName, store))
                using (var store2 = GetDocumentStore(new Options
                {
                    CreateDatabase = false,
                    ModifyDatabaseName = s => databaseName,
                    Server = server
                }))
                {
                    store2.Maintenance.Server.Send(restoreOperation)
                        .WaitForCompletion(TimeSpan.FromSeconds(30));

                    var record2 = await store2.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    Assert.Equal(1, record2.ExternalReplications.Count);
                }
            }
        }

        [Fact]
        public async Task CanExcludeDatabase()
        {
            using (var store = GetDocumentStore())
            {
                var serverWideExternalReplication = new ServerWideExternalReplication
                {
                    Disabled = true,
                    TopologyDiscoveryUrls = new[] { store.Urls.First() }
                };

                var result = await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(serverWideExternalReplication));
                serverWideExternalReplication.Name = result.Name;

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record.ExternalReplications.Count);
                Assert.Equal(1, record.RavenConnectionStrings.Count);

                var dbName = $"db/{Guid.NewGuid()}";
                var csName = $"cs/{Guid.NewGuid()}";

                var connectionString = new RavenConnectionString
                {
                    Name = csName,
                    Database = dbName,
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" }
                };

                var putConnectionStringResult = await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                Assert.NotNull(putConnectionStringResult.RaftCommandIndex);

                var externalReplication = new ExternalReplication(dbName, csName)
                {
                    Name = "Regular Task",
                    Disabled = true
                };
                await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(externalReplication));

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(2, record.ExternalReplications.Count);
                Assert.Equal(2, record.RavenConnectionStrings.Count);

                serverWideExternalReplication.ExcludedDatabases = new[] { store.Database };
                await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(serverWideExternalReplication));

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record.ExternalReplications.Count);
                Assert.Equal(1, record.RavenConnectionStrings.Count);
                Assert.Equal(externalReplication.Name, record.ExternalReplications.First().Name);
            }
        }

        [Fact]
        public async Task CanExcludeForNewDatabase()
        {
            using (var store = GetDocumentStore())
            {
                var newDbName = store.Database + "-testDatabase";
                var serverWideExternalReplication = new ServerWideExternalReplication
                {
                    Disabled = true,
                    TopologyDiscoveryUrls = new[]
                    {
                        store.Urls.First()
                    },
                    ExcludedDatabases = new []
                    {
                        newDbName
                    }
                };

                var result = await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(serverWideExternalReplication));
                serverWideExternalReplication.Name = result.Name;
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDbName)));

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                Assert.Equal(0, record.ExternalReplications.Count);

                var dbName = $"db/{Guid.NewGuid()}";
                var csName = $"cs/{Guid.NewGuid()}";

                var connectionString = new RavenConnectionString
                {
                    Name = csName,
                    Database = dbName,
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:12345" }
                };

                var putConnectionStringResult = await store.Maintenance.ForDatabase(newDbName).SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                Assert.NotNull(putConnectionStringResult.RaftCommandIndex);

                var externalReplication = new ExternalReplication(dbName, csName)
                {
                    Name = "Regular Task",
                    Disabled = true
                };
                await store.Maintenance.ForDatabase(newDbName).SendAsync(new UpdateExternalReplicationOperation(externalReplication));

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record.ExternalReplications.Count);
                Assert.Equal(1, record.RavenConnectionStrings.Count);

                serverWideExternalReplication.ExcludedDatabases = new[] { store.Database };
                await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(serverWideExternalReplication));

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(0, record.ExternalReplications.Count);
                Assert.Equal(0, record.RavenConnectionStrings.Count);

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                Assert.Equal(2, record.ExternalReplications.Count);
                Assert.Equal(2, record.RavenConnectionStrings.Count);
                Assert.Equal(externalReplication.Name, record.ExternalReplications[0].Name);
                Assert.Equal(PutServerWideExternalReplicationCommand.GetTaskName(serverWideExternalReplication.Name), record.ExternalReplications[1].Name);

                serverWideExternalReplication.ExcludedDatabases = null;
                await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(serverWideExternalReplication));

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tasks = Server.ServerStore.Cluster.GetServerWideConfigurations(context, OngoingTaskType.Replication, serverWideExternalReplication.Name).ToList();
                    Assert.Equal(1, tasks.Count);

                    tasks[0].TryGet(nameof(ServerWideExternalReplication.ExcludedDatabases), out BlittableJsonReaderArray excludedDatabases);
                    Assert.NotNull(excludedDatabases);
                    Assert.Equal(0, excludedDatabases.Length);
                }

                var newDbName2 = store.Database + "-testDatabase2";
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDbName2)));

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                Assert.Equal(2, record.ExternalReplications.Count);
                Assert.Equal(2, record.RavenConnectionStrings.Count);

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName2));
                Assert.Equal(1, record.ExternalReplications.Count);
                Assert.Equal(1, record.RavenConnectionStrings.Count);
            }
        }

        [Fact]
        public async Task FailToAddNullOrEmptyDatabaseNames()
        {
            using (var store = GetDocumentStore())
            {
                var serverWideExternalReplication = new ServerWideExternalReplication
                {
                    Disabled = true,
                    TopologyDiscoveryUrls = new[]
                    {
                        store.Urls.First()
                    },
                    ExcludedDatabases = new[]
                    {
                        null,
                        "test"
                    }
                };

                await SaveAndAssertError();

                serverWideExternalReplication.ExcludedDatabases = new[]
                {
                    string.Empty
                };

                await SaveAndAssertError();

                serverWideExternalReplication.ExcludedDatabases = new[]
                {
                    " "
                };

                await SaveAndAssertError();

                async Task SaveAndAssertError()
                {
                    var error = await Assert.ThrowsAsync<RavenException>(async () => await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(serverWideExternalReplication)));
                    Assert.Contains($"{nameof(ServerWideExternalReplication.ExcludedDatabases)} cannot contain null or empty database names", error.Message);
                }
            }
        }

        private static void ValidateServerWideConfiguration(ServerWideExternalReplication serverWideConfiguration, ServerWideExternalReplication putConfiguration)
        {
            Assert.Equal(serverWideConfiguration.Name, putConfiguration.Name ?? putConfiguration.GetDefaultTaskName());
            Assert.Equal(putConfiguration.Disabled, serverWideConfiguration.Disabled);
            Assert.True(putConfiguration.TopologyDiscoveryUrls.SequenceEqual(serverWideConfiguration.TopologyDiscoveryUrls));
        }

        private static void ValidateConfiguration(ServerWideExternalReplication serverWideConfiguration, ExternalReplication externalReplication, string databaseName)
        {
            Assert.Equal(PutServerWideExternalReplicationCommand.GetTaskName(serverWideConfiguration.Name), externalReplication.Name);
            Assert.Equal(serverWideConfiguration.Disabled, externalReplication.Disabled);
            Assert.Equal(databaseName, externalReplication.Database);
            Assert.Equal(PutServerWideExternalReplicationCommand.GetRavenConnectionStringName(serverWideConfiguration.Name), externalReplication.ConnectionStringName);
        }
    }
}
