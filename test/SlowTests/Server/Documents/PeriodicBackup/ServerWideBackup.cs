using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Config;
using Raven.Server.ServerWide.Commands;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Platform;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class ServerWideBackup : RavenTestBase
    {
        public ServerWideBackup(ITestOutputHelper output) : base(output)
        {
            DoNotReuseServer();
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanAddServerWideBackupToNewDatabase()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
                {
                    FullBackupFrequency = "* * * * *",
                    Disabled = true
                }));

                var newDatabaseName = store.Database + "_new";
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDatabaseName)));

                var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDatabaseName));
                Assert.Equal(1, databaseRecord.PeriodicBackups.Count);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanStoreServerWideBackup()
        {
            using (var store = GetDocumentStore())
            {
                var putConfiguration = new ServerWideBackupConfiguration
                {
                    Disabled = true,
                    FullBackupFrequency = "0 2 * * 0",
                    IncrementalBackupFrequency = "0 2 * * 1",
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = "test/folder"
                    },
                    S3Settings = new S3Settings
                    {
                        BucketName = "ravendb-bucket",
                        RemoteFolderName = "grisha/backups"
                    },
                    AzureSettings = new AzureSettings
                    {
                        AccountKey = "Test",
                        AccountName = "Test",
                        RemoteFolderName = "grisha/backups"
                    },
                    FtpSettings = new FtpSettings
                    {
                        Url = "ftps://localhost/grisha/backups"
                    }
                };

                var result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
                var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result.Name));
                Assert.NotNull(serverWideConfiguration);

                ValidateServerWideConfiguration(serverWideConfiguration, putConfiguration);

                // the configuration is applied to existing databases
                var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var backups1 = record1.PeriodicBackups;
                Assert.Equal(1, backups1.Count);
                ValidateBackupConfiguration(serverWideConfiguration, backups1.First(), store.Database);

                // the configuration is applied to new databases
                var newDbName = store.Database + "-testDatabase";
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDbName)));
                var backups2 = record1.PeriodicBackups;
                Assert.Equal(1, backups2.Count);
                var record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                ValidateBackupConfiguration(serverWideConfiguration, record2.PeriodicBackups.First(), newDbName);

                // update the backup configuration
                putConfiguration.FullBackupFrequency = "3 2 * * 1";
                putConfiguration.LocalSettings.FolderPath += "/folder1";
                putConfiguration.S3Settings.RemoteFolderName += "/folder2";
                putConfiguration.AzureSettings.RemoteFolderName += "/folder3";
                putConfiguration.FtpSettings.Url += "/folder4";
                putConfiguration.Name = serverWideConfiguration.Name;

                result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
                serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result.Name));
                ValidateServerWideConfiguration(serverWideConfiguration, putConfiguration);

                record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record1.PeriodicBackups.Count);
                ValidateBackupConfiguration(serverWideConfiguration, record1.PeriodicBackups.First(), store.Database);

                record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                Assert.Equal(1, record2.PeriodicBackups.Count);
                ValidateBackupConfiguration(serverWideConfiguration, record2.PeriodicBackups.First(), newDbName);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task UpdateServerWideBackupThroughUpdatePeriodicBackupFails()
        {
            using (var store = GetDocumentStore())
            {
                var putConfiguration = new ServerWideBackupConfiguration
                {
                    Disabled = true,
                    FullBackupFrequency = "0 2 * * 0",
                    IncrementalBackupFrequency = "0 2 * * 1"
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));

                var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var currentBackupConfiguration = databaseRecord.PeriodicBackups.First();
                var serverWideBackupTaskId = currentBackupConfiguration.TaskId;

                var backupConfiguration = Backup.CreateBackupConfiguration(taskId: currentBackupConfiguration.TaskId, fullBackupFrequency: "0 2 * * 0", incrementalBackupFrequency: "0 2 * * 1" ,disabled: true);
                var taskName = PutServerWideBackupConfigurationCommand.GetTaskNameForDatabase(putConfiguration.GetDefaultTaskName());
                var e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfiguration)));
                var expectedError = $"Can't delete task id: {currentBackupConfiguration.TaskId}, name: '{taskName}', because it is a server-wide backup task";
                Assert.Contains(expectedError, e.Message);

                backupConfiguration.TaskId = 0;
                backupConfiguration.Name = currentBackupConfiguration.Name;
                e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfiguration)));
                expectedError = $"Can't create task: '{taskName}'. A regular (non server-wide) backup task name can't start with prefix '{ServerWideBackupConfiguration.NamePrefix}'";
                Assert.Contains(expectedError, e.Message);

                e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new DeleteOngoingTaskOperation(serverWideBackupTaskId, OngoingTaskType.Backup)));
                expectedError = $"Can't delete task id: {serverWideBackupTaskId}, name: '{taskName}', because it is a server-wide backup task";
                Assert.Contains(expectedError, e.Message);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ToggleDisableServerWideBackupFails()
        {
            using (var store = GetDocumentStore())
            {
                var putConfiguration = new ServerWideBackupConfiguration
                {
                    Disabled = true,
                    FullBackupFrequency = "0 2 * * 0",
                    IncrementalBackupFrequency = "0 2 * * 1"
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));

                var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var currentBackupConfiguration = databaseRecord.PeriodicBackups.First();
                var serverWideBackupTaskId = currentBackupConfiguration.TaskId;

                var e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(serverWideBackupTaskId, OngoingTaskType.Backup, false)));
                Assert.Contains("Can't enable task name 'Server Wide Backup, Backup w/o destinations', because it is a server-wide backup task", e.Message);

                e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(serverWideBackupTaskId, OngoingTaskType.Backup, true)));
                Assert.Contains("Can't disable task name 'Server Wide Backup, Backup w/o destinations', because it is a server-wide backup task", e.Message);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CreatePeriodicBackupFailsWhenUsingReservedName()
        {
            using (var store = GetDocumentStore())
            {
                var putConfiguration = new ServerWideBackupConfiguration
                {
                    Disabled = true,
                    FullBackupFrequency = "0 2 * * 0",
                    IncrementalBackupFrequency = "0 2 * * 1"
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));

                var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var currentBackupConfiguration = databaseRecord.PeriodicBackups.First();
                var serverWideBackupTaskId = currentBackupConfiguration.TaskId;
      
                var backupConfiguration = Backup.CreateBackupConfiguration(taskId: currentBackupConfiguration.TaskId, fullBackupFrequency: "0 2 * * 0", incrementalBackupFrequency: "0 2 * * 1", disabled: true);
                var taskName = PutServerWideBackupConfigurationCommand.GetTaskNameForDatabase(putConfiguration.GetDefaultTaskName());
                var e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfiguration)));
                var expectedError = $"Can't delete task id: {currentBackupConfiguration.TaskId}, name: '{taskName}', because it is a server-wide backup task";
                Assert.Contains(expectedError, e.Message);

                backupConfiguration.TaskId = 0;
                backupConfiguration.Name = currentBackupConfiguration.Name;
                e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfiguration)));
                expectedError = $"Can't create task: '{taskName}'. A regular (non server-wide) backup task name can't start with prefix '{ServerWideBackupConfiguration.NamePrefix}'";
                Assert.Contains(expectedError, e.Message);

                e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new DeleteOngoingTaskOperation(serverWideBackupTaskId, OngoingTaskType.Backup)));
                expectedError = $"Can't delete task id: {serverWideBackupTaskId}, name: '{taskName}', because it is a server-wide backup task";
                Assert.Contains(expectedError, e.Message);
            }
        }

        [Fact(Skip = "https://github.com/dotnet/corefx/issues/30691"), Trait("Category", "Smuggler")]
        public async Task CanCreateBackupUsingConfigurationFromBackupScript()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore())
            {
                var scriptPath = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
                var localSetting = new LocalSettings
                {
                    Disabled = false,
                    FolderPath = backupPath,
                };

                var localSettingsString = JsonConvert.SerializeObject(localSetting);

                string command;
                string script;

                if (PlatformDetails.RunningOnPosix)
                {
                    command = "bash";
                    script = $"#!/bin/bash\r\necho '{localSettingsString}'";
                    File.WriteAllText(scriptPath, script);
                    Process.Start("chmod", $"700 {scriptPath}");
                }
                else
                {
                    command = "powershell";
                    script = $"echo '{localSettingsString}'";
                    File.WriteAllText(scriptPath, script);
                }

                var putConfiguration = new ServerWideBackupConfiguration
                {
                    Disabled = false,
                    FullBackupFrequency = "0 2 * * 0",
                    IncrementalBackupFrequency = "0 2 * * 1",
                    LocalSettings = new LocalSettings
                    {
                        GetBackupConfigurationScript = new GetBackupConfigurationScript
                        {
                            Exec = command,
                            Arguments = scriptPath
                        }
                    }
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var backupTask = record.PeriodicBackups.First();
                Assert.Null(backupTask.LocalSettings.FolderPath);
                Assert.NotNull(backupTask.LocalSettings.GetBackupConfigurationScript);
                Assert.NotNull(backupTask.LocalSettings.GetBackupConfigurationScript.Exec);
                Assert.NotNull(backupTask.LocalSettings.GetBackupConfigurationScript.Arguments);

                var backupTaskId = backupTask.TaskId;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company { Name = "Hibernating Rhinos" }, "companies/1");
                    await session.SaveChangesAsync();
                }

                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanCreateMoreThanOneServerWideBackup()
        {
            using (var store = GetDocumentStore())
            {
                var putConfiguration = new ServerWideBackupConfiguration
                {
                    Disabled = true,
                    FullBackupFrequency = "0 2 * * 0",
                    IncrementalBackupFrequency = "0 2 * * 1"
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));

                putConfiguration.FtpSettings = new FtpSettings
                {
                    Disabled = true,
                    Url = "http://url:8080"
                };
                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));

                putConfiguration.AzureSettings = new AzureSettings
                {
                    Disabled = true,
                    AccountKey = "test"
                };
                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));

                var serverWideBackups = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationsOperation());
                Assert.Equal(3, serverWideBackups.Length);

                var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(3, databaseRecord.PeriodicBackups.Count);

                // update one of the tasks
                var toUpdate = serverWideBackups[1];
                toUpdate.BackupType = BackupType.Snapshot;
                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(toUpdate));

                serverWideBackups = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationsOperation());
                Assert.Equal(3, serverWideBackups.Length);

                databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(3, databaseRecord.PeriodicBackups.Count);

                // new database includes all server-wide backups
                var newDbName = store.Database + "-testDatabase";
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDbName)));
                databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                Assert.Equal(3, databaseRecord.PeriodicBackups.Count);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDeleteServerWideBackup()
        {
            using (var store = GetDocumentStore())
            {
                var putConfiguration = new ServerWideBackupConfiguration
                {
                    Disabled = true,
                    FullBackupFrequency = "0 2 * * 0",
                    IncrementalBackupFrequency = "0 2 * * 1"
                };

                var result1 = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
                var result2 = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));

                var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(2, record1.PeriodicBackups.Count);
                var serverWideBackups = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationsOperation());
                Assert.Equal(2, serverWideBackups.Length);

                // the configuration is applied to new databases
                var newDbName = store.Database + "-testDatabase";
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDbName)));
                var record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                Assert.Equal(2, record2.PeriodicBackups.Count);

                await store.Maintenance.Server.SendAsync(new DeleteServerWideBackupConfigurationOperation(result1.Name));
                var serverWideBackupConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result1.Name));
                Assert.Null(serverWideBackupConfiguration);
                serverWideBackups = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationsOperation());
                Assert.Equal(1, serverWideBackups.Length);

                // verify that the server-wide backup was deleted from all databases
                record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record1.PeriodicBackups.Count);
                Assert.Equal($"{ServerWideBackupConfiguration.NamePrefix}, {putConfiguration.GetDefaultTaskName()} #2", record1.PeriodicBackups.First().Name);
                record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                Assert.Equal(1, record2.PeriodicBackups.Count);
                Assert.Equal($"{ServerWideBackupConfiguration.NamePrefix}, {putConfiguration.GetDefaultTaskName()} #2", record2.PeriodicBackups.First().Name);

                await store.Maintenance.Server.SendAsync(new DeleteServerWideBackupConfigurationOperation(result2.Name));
                serverWideBackupConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result2.Name));
                Assert.Null(serverWideBackupConfiguration);
                serverWideBackups = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationsOperation());
                Assert.Equal(0, serverWideBackups.Length);

                record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(0, record1.PeriodicBackups.Count);
                record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                Assert.Equal(0, record2.PeriodicBackups.Count);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task SkipExportingTheServerWideBackup1()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore())
            {
                var serverWideBackupConfiguration1 = new ServerWideBackupConfiguration
                {
                    Disabled = false,
                    FullBackupFrequency = "0 2 * * 0",
                    IncrementalBackupFrequency = "0 2 * * 1",
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                };

                var serverWideBackupConfiguration2 = new ServerWideBackupConfiguration
                {
                    Disabled = true,
                    FullBackupFrequency = "0 2 * * 0",
                    IncrementalBackupFrequency = "0 2 * * 1"
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(serverWideBackupConfiguration1));
                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(serverWideBackupConfiguration2));

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

                // new server should have only 0 backups
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
                    Assert.Equal(0, record2.PeriodicBackups.Count);
                }
            }
        }

        [Theory, Trait("Category", "Smuggler")]
        [InlineData(EncryptionMode.None)]
        [InlineData(EncryptionMode.UseProvidedKey)]
        [InlineData(EncryptionMode.UseDatabaseKey)]
        public async Task ServerWideBackupShouldBeEncryptedForEncryptedDatabase(EncryptionMode encryptionMode)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var key = EncryptedServer(out var certificates, out string dbName);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record => record.Encrypted = true,
                Path = NewDataPath()
            }))
            {
                var serverWideBackupConfiguration = new ServerWideBackupConfiguration
                {
                    Disabled = false,
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "0 */6 * * *"
                };

                switch (encryptionMode)
                {
                    case EncryptionMode.None:
                        serverWideBackupConfiguration.BackupEncryptionSettings = new BackupEncryptionSettings
                        {
                            EncryptionMode = EncryptionMode.None,
                        };
                        break;
                    case EncryptionMode.UseDatabaseKey:
                        serverWideBackupConfiguration.BackupEncryptionSettings = new BackupEncryptionSettings
                        {
                            EncryptionMode = EncryptionMode.UseDatabaseKey
                        };
                        break;
                    case EncryptionMode.UseProvidedKey:
                        serverWideBackupConfiguration.BackupEncryptionSettings = new BackupEncryptionSettings
                        {
                            EncryptionMode = EncryptionMode.UseProvidedKey,
                            Key = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs="
                        };
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(encryptionMode), encryptionMode, null);
                }

                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(serverWideBackupConfiguration));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "grisha"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var backup = record.PeriodicBackups.First();
                var backupTaskId = backup.TaskId;

                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));

                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(new GetPeriodicBackupStatusOperation(backupTaskId)).Status;
                    return status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);

                var databaseName = $"restored_database-{Guid.NewGuid()}";

                var backupDirectory = $"{backupPath}/{store.Database}";
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupDirectory).First(),
                    DatabaseName = databaseName,
                    BackupEncryptionSettings = new BackupEncryptionSettings
                    {
                        EncryptionMode = EncryptionMode.UseProvidedKey,
                        Key = key
                    }
                }))
                {
                    using (var session = store.OpenSession(databaseName))
                    {
                        var users = session.Load<User>("users/1");
                        Assert.NotNull(users);
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task SkipExportingTheServerWideBackup2()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore())
            {
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

                // save another backup task in the database record
                var backupConfiguration = Backup.CreateBackupConfiguration(fullBackupFrequency: "0 2 * * 0", incrementalBackupFrequency: "0 2 * * 1", disabled: true);
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfiguration));

                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: true);
                string backupDirectory = status.LocalBackup.BackupDirectory;

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

                // old server should have 2: 1 server-wide and 1 regular backup
                using (var store2 = GetDocumentStore(new Options
                {
                    CreateDatabase = false,
                    ModifyDatabaseName = s => databaseName,
                }))
                {
                    var record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store2.Database));
                    Assert.Equal(2, record2.PeriodicBackups.Count);
                }

                // new server should have only one backup
                var server = GetNewServer();
                using (var store3 = GetDocumentStore(new Options
                {
                    CreateDatabase = false,
                    ModifyDatabaseName = s => databaseName,
                    Server = server
                }))
                {
                    store3.Maintenance.Server.Send(restoreOperation)
                        .WaitForCompletion(TimeSpan.FromSeconds(30));

                    var record3 = await store3.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    Assert.Equal(1, record3.PeriodicBackups.Count);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanCreateSnapshotBackupForNonEncryptedDatabase()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore())
            {
                var serverWideBackupConfiguration = new ServerWideBackupConfiguration
                {
                    Disabled = false,
                    FullBackupFrequency = "0 2 * * 0",
                    IncrementalBackupFrequency = "0 2 * * 1",
                    BackupType = BackupType.Snapshot,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    BackupEncryptionSettings = new BackupEncryptionSettings
                    {
                        Key = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                        EncryptionMode = EncryptionMode.UseProvidedKey
                    }
                };

                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(serverWideBackupConfiguration));

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var backup = record.PeriodicBackups.First();
                var backupTaskId = backup.TaskId;

                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));

                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(new GetPeriodicBackupStatusOperation(backupTaskId)).Status;
                    return status?.LastEtag;
                }, 0);

                Assert.Equal(0, value);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanStoreAndEditServerWideBackupForIdleDatabase()
        {
            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                    [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
                }
            });
            using (var store = GetDocumentStore(new Options { Server = server, RunInMemory = false }))
            {
                var fullFreq = "0 2 1 1 *";
                var incFreq = "0 2 * * 0";
                var putConfiguration = new ServerWideBackupConfiguration
                {
                    FullBackupFrequency = fullFreq,
                    IncrementalBackupFrequency = incFreq,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = "test/folder"
                    }
                };

                Assert.Equal(1, WaitForValue(() => server.ServerStore.IdleDatabases.Count, 1, timeout: 60000, interval: 1000));

                var result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
                var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result.Name));
                Assert.NotNull(serverWideConfiguration);
                Assert.Equal(fullFreq, serverWideConfiguration.FullBackupFrequency);
                Assert.Equal(incFreq, serverWideConfiguration.IncrementalBackupFrequency);
                Assert.Equal(1, server.ServerStore.IdleDatabases.Count);

                // update the backup configuration
                putConfiguration.Name = serverWideConfiguration.Name;
                putConfiguration.TaskId = serverWideConfiguration.TaskId;
                putConfiguration.FullBackupFrequency = "0 2 * * 0";

                var oldName = result.Name;
                result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));

                Exception ex = null;
                try
                {
                    await server.ServerStore.Cluster.WaitForIndexNotification(result.RaftCommandIndex, TimeSpan.FromMinutes(1));
                }
                catch (Exception e)
                {
                    ex = e;
                }
                finally
                {
                    Assert.Null(ex);
                }
                
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record.PeriodicBackups.Count);
                PeriodicBackupConfiguration periodicBackupConfiguration = record.PeriodicBackups.First();

                var newServerWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result.Name));

                // compare with periodic backup task 
                Assert.NotEqual(newServerWideConfiguration.TaskId, periodicBackupConfiguration.TaskId); // backup task id in db record doesn't change
                Assert.Equal(PutServerWideBackupConfigurationCommand.GetTaskNameForDatabase(oldName), periodicBackupConfiguration.Name);
                Assert.Equal(incFreq, periodicBackupConfiguration.FullBackupFrequency);
                Assert.Equal(incFreq, periodicBackupConfiguration.IncrementalBackupFrequency);
                Assert.NotEqual(serverWideConfiguration.FullBackupFrequency, periodicBackupConfiguration.FullBackupFrequency);

                // compare with previous server wide backup
                Assert.NotEqual(serverWideConfiguration.TaskId, newServerWideConfiguration.TaskId); // task id in server storage get increased with each change
                Assert.Equal(oldName, result.Name);
                Assert.Equal(incFreq, newServerWideConfiguration.FullBackupFrequency);
                Assert.Equal(incFreq, newServerWideConfiguration.IncrementalBackupFrequency);
                Assert.NotEqual(serverWideConfiguration.FullBackupFrequency, newServerWideConfiguration.FullBackupFrequency);
                Assert.Equal(1, server.ServerStore.IdleDatabases.Count);
            }
        }

        private static void ValidateServerWideConfiguration(ServerWideBackupConfiguration serverWideConfiguration, ServerWideBackupConfiguration putConfiguration)
        {
            Assert.Equal(serverWideConfiguration.Name, putConfiguration.Name ?? putConfiguration.GetDefaultTaskName());
            Assert.Equal(putConfiguration.Disabled, serverWideConfiguration.Disabled);
            Assert.Equal(putConfiguration.FullBackupFrequency, serverWideConfiguration.FullBackupFrequency);
            Assert.Equal(putConfiguration.IncrementalBackupFrequency, serverWideConfiguration.IncrementalBackupFrequency);

            Assert.Equal(putConfiguration.LocalSettings.FolderPath, serverWideConfiguration.LocalSettings.FolderPath);
            Assert.Equal(putConfiguration.S3Settings.BucketName, serverWideConfiguration.S3Settings.BucketName);
            Assert.Equal(putConfiguration.S3Settings.RemoteFolderName, serverWideConfiguration.S3Settings.RemoteFolderName);
            Assert.Equal(putConfiguration.AzureSettings.AccountKey, serverWideConfiguration.AzureSettings.AccountKey);
            Assert.Equal(putConfiguration.AzureSettings.AccountName, serverWideConfiguration.AzureSettings.AccountName);
            Assert.Equal(putConfiguration.AzureSettings.RemoteFolderName, serverWideConfiguration.AzureSettings.RemoteFolderName);
            Assert.Equal(putConfiguration.FtpSettings.Url, serverWideConfiguration.FtpSettings.Url);
        }

        private static void ValidateBackupConfiguration(ServerWideBackupConfiguration serverWideConfiguration, PeriodicBackupConfiguration backupConfiguration, string databaseName)
        {
            Assert.Equal(PutServerWideBackupConfigurationCommand.GetTaskNameForDatabase(serverWideConfiguration.Name), backupConfiguration.Name);
            Assert.Equal(serverWideConfiguration.Disabled, backupConfiguration.Disabled);
            Assert.Equal(serverWideConfiguration.FullBackupFrequency, backupConfiguration.FullBackupFrequency);
            Assert.Equal(serverWideConfiguration.IncrementalBackupFrequency, backupConfiguration.IncrementalBackupFrequency);

            Assert.Equal($"{serverWideConfiguration.LocalSettings.FolderPath}{Path.DirectorySeparatorChar}{databaseName}", backupConfiguration.LocalSettings.FolderPath);
            Assert.Equal(serverWideConfiguration.S3Settings.BucketName, backupConfiguration.S3Settings.BucketName);
            Assert.Equal($"{serverWideConfiguration.S3Settings.RemoteFolderName}/{databaseName}", backupConfiguration.S3Settings.RemoteFolderName);
            Assert.Equal(serverWideConfiguration.AzureSettings.AccountKey, backupConfiguration.AzureSettings.AccountKey);
            Assert.Equal(serverWideConfiguration.AzureSettings.AccountName, backupConfiguration.AzureSettings.AccountName);
            Assert.Equal($"{serverWideConfiguration.AzureSettings.RemoteFolderName}/{databaseName}", backupConfiguration.AzureSettings.RemoteFolderName);
            Assert.Equal($"{serverWideConfiguration.FtpSettings.Url}/{databaseName}", backupConfiguration.FtpSettings.Url);
        }
        
        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(10)]
        public async Task ServerWideBackup_WhenEditingName_ShouldNotCreateNewOne(int additionalBackupCount)
        {
            const string firstName = "FirstName";
            const string changedName = "changedName";
            
            using var store = GetDocumentStore();
            
            var result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
            {
                FullBackupFrequency = "0 2 * * 0",
                LocalSettings = new LocalSettings {FolderPath = "test/folder"},
                Name = firstName
            }));

            for (var i = 0; i < additionalBackupCount; i++)
            {
                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
                {
                    FullBackupFrequency = "0 2 * * 0",
                    LocalSettings = new LocalSettings {FolderPath = $"test/folder{i}"},
                    Name = "NameServerWide" + i
                }));
                
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings {FolderPath = $"test/folder{i}"},
                    IncrementalBackupFrequency = "0 2 * * 0",
                    Name = "Name" + i
                }));
            }
            
            var recordBeforeEditing = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

            var configuration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result.Name));
            configuration.Name = changedName;
            await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(configuration));

            var newBackup = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(firstName));
            Assert.Null(newBackup);
            var oldBackup = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(changedName));
            Assert.NotNull(oldBackup);
                
            var recordAfterEditing = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

            Assert.True(recordBeforeEditing.PeriodicBackups.Count == recordAfterEditing.PeriodicBackups.Count, GatherAdditionalInformationOnFailing());
            var beforeEditingConfig = recordBeforeEditing.PeriodicBackups.FirstOrDefault(b => b.Name.EndsWith(firstName));
            Assert.True(beforeEditingConfig != null, GatherAdditionalInformationOnFailing());
            var afterEditingConfig = recordAfterEditing.PeriodicBackups.First(b => b.Name.EndsWith(changedName));
            Assert.True(afterEditingConfig != null, GatherAdditionalInformationOnFailing());
            Assert.Equal(beforeEditingConfig.TaskId, afterEditingConfig.TaskId);

            string GatherAdditionalInformationOnFailing()
            {
                return $"Periodic backups before editing: {string.Join("; ", recordBeforeEditing.PeriodicBackups.Select(b => b.Name))}{Environment.NewLine}" +
                       $"Periodic backups after editing: {string.Join("; ", recordAfterEditing.PeriodicBackups.Select(b => b.Name))}";
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ServerWideBackup_WhenEditingNameWithOldTaskId_ShouldThrow()
        {
            const string firstName = "FirstName";
            const string changedName = "changedName";
            
            using var store = GetDocumentStore();
            
            var result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
            {
                FullBackupFrequency = "0 2 * * 0",
                LocalSettings = new LocalSettings {FolderPath = "test/folder"},
                Name = firstName
            }));
            
            var configuration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation(result.Name));
            configuration.FullBackupFrequency = "0 3 * * 0";
            await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(configuration));
            configuration.Name = changedName;

            await Assert.ThrowsAnyAsync<RavenException>(async () =>
            {
                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(configuration));
            });
        }
    }
}
