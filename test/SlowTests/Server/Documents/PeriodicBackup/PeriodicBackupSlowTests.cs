using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Server.Operations;
using Raven.Client.Server.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class PeriodicBackupTestsSlow : RavenTestBase
    {
        private readonly string _backupPath;

        public PeriodicBackupTestsSlow()
        {
            _backupPath = NewDataPath(suffix: "BackupFolder");
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_to_directory_multiple_backups_with_long_interval()
        {
            using (var store = GetDocumentStore())
            {
                var periodicBackupRunner = (await GetDocumentDatabaseInstanceFor(store)).PeriodicBackupRunner;

                // get by reflection the maxTimerTimeoutInMilliseconds field
                // this field is the maximum interval acceptable in .Net's threading timer
                // if the requested backup interval is bigger than this maximum interval, 
                // a timer with maximum interval will be used several times until the interval cumulatively
                // will be equal to requested interval
                typeof(PeriodicBackupRunner)
                    .GetField(nameof(PeriodicBackupRunner.MaxTimerTimeout), BindingFlags.Instance | BindingFlags.Public)
                    .SetValue(periodicBackupRunner, TimeSpan.FromMilliseconds(100));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");               
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = _backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var result = await store.Admin.Server.SendAsync(new UpdatePeriodicBackupOperation(config, store.Database));
                var periodicBackupTaskId = result.TaskId;

                var operation = new GetPeriodicBackupStatusOperation(store.Database, periodicBackupTaskId);
                SpinWait.SpinUntil(() =>
                {
                    var getPeriodicBackupResult = store.Admin.Server.Send(operation);
                    return getPeriodicBackupResult.Status?.LastEtag > 0;
                }, TimeSpan.FromSeconds(10));

                var etagForBackups = store.Admin.Server.Send(operation).Status.LastEtag;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.SaveChangesAsync();
                }

                SpinWait.SpinUntil(() =>
                {
                    var newLastEtag = store.Admin.Server.Send(operation).Status.LastEtag;
                    return newLastEtag != etagForBackups;
                }, TimeSpan.FromMinutes(2));
            }

            using (var store = GetDocumentStore(dbSuffixIdentifier: "2"))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerOptions(),
                    Directory.GetDirectories(_backupPath).First());
                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                    Assert.True(users.Any(x => x.Value.Name == "oren"));
                    Assert.True(users.Any(x => x.Value.Name == "ayende"));
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task periodic_backup_should_work_with_long_intervals()
        {
            using (var store = GetDocumentStore())
            {
                var periodicBackupRunner = (await GetDocumentDatabaseInstanceFor(store)).PeriodicBackupRunner;

                // get by reflection the maxTimerTimeoutInMilliseconds field
                // this field is the maximum interval acceptable in .Net's threading timer
                // if the requested backup interval is bigger than this maximum interval, 
                // a timer with maximum interval will be used several times until the interval cumulatively
                // will be equal to requested interval
                typeof(PeriodicBackupRunner)
                    .GetField(nameof(PeriodicBackupRunner.MaxTimerTimeout), BindingFlags.Instance | BindingFlags.Public)
                    .SetValue(periodicBackupRunner, TimeSpan.FromMilliseconds(100));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren 1" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = _backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var operation = new UpdatePeriodicBackupOperation(config, store.Database);
                var result = await store.Admin.Server.SendAsync(operation);
                var periodicBackupTaskId = result.TaskId;

                SpinWait.SpinUntil(() =>
                {
                    var getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(store.Database, periodicBackupTaskId);
                    var status = store.Admin.Server.Send(getPeriodicBackupStatus).Status;
                    return status?.LastFullBackup != null;
                }, 2000);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren 2" }, "users/2");
                    await session.SaveChangesAsync();
                }

                SpinWait.SpinUntil(() =>
                {
                    var getPeriodicBackupStatus = 
                        new GetPeriodicBackupStatusOperation(store.Database, periodicBackupTaskId);
                    var status = store.Admin.Server.Send(getPeriodicBackupStatus).Status;
                    return status?.LastFullBackup != null && status.LastIncrementalBackup != null;
                }, TimeSpan.FromMinutes(2));
            }

            using (var store = GetDocumentStore(dbSuffixIdentifier: "2"))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerOptions(),
                    Directory.GetDirectories(_backupPath).First());
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("oren 1", user.Name);

                    user = await session.LoadAsync<User>("users/2");
                    Assert.Equal("oren 2", user.Name);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_to_directory_multiple_backups()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = _backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var result = await store.Admin.Server.SendAsync(new UpdatePeriodicBackupOperation(config, store.Database));
                var periodicBackupTaskId = result.TaskId;

                var operation = new GetPeriodicBackupStatusOperation(store.Database, periodicBackupTaskId);
                SpinWait.SpinUntil(() =>
                {
                    var getPeriodicBackupResult = store.Admin.Server.Send(operation);
                    return getPeriodicBackupResult.Status?.LastEtag > 0;
                }, TimeSpan.FromSeconds(10));

                var etagForBackups = store.Admin.Server.Send(operation).Status.LastEtag;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.SaveChangesAsync();
                }

                SpinWait.SpinUntil(() =>
                {
                    var newLastEtag = store.Admin.Server.Send(operation).Status.LastEtag;
                    return newLastEtag != etagForBackups;
                }, TimeSpan.FromMinutes(2));
            }

            using (var store = GetDocumentStore(dbSuffixIdentifier: "2"))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerOptions(),
                    Directory.GetDirectories(_backupPath).First());
                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                    Assert.True(users.Any(x => x.Value.Name == "oren"));
                    Assert.True(users.Any(x => x.Value.Name == "ayende"));
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_restore_smuggler_correctly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = _backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var result = await store.Admin.Server.SendAsync(new UpdatePeriodicBackupOperation(config, store.Database));
                var periodicBackupTaskId = result.TaskId;

                var operation = new GetPeriodicBackupStatusOperation(store.Database, periodicBackupTaskId);
                SpinWait.SpinUntil(() =>
                {
                    var getPeriodicBackupResult = store.Admin.Server.Send(operation);
                    return getPeriodicBackupResult.Status?.LastEtag > 0;
                }, TimeSpan.FromSeconds(10));

                var etagForBackups = store.Admin.Server.Send(operation).Status.LastEtag;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.SaveChangesAsync();
                }

                SpinWait.SpinUntil(() =>
                {
                    var newLastEtag = store.Admin.Server.Send(operation).Status.LastEtag;
                    return newLastEtag != etagForBackups;
                }, TimeSpan.FromMinutes(2));
            }

            using (var store1 = GetDocumentStore(dbSuffixIdentifier: "2"))
            using (var store2 = GetDocumentStore(dbSuffixIdentifier: "2"))
            {
                var backupDirectory = Directory.GetDirectories(_backupPath).First();

                var backupToMovePath = $"{_backupPath}\\IncrementalBackupTemp";
                Directory.CreateDirectory(backupToMovePath);
                var incrementalBackupFile = Directory.GetFiles(backupDirectory).Last();
                var fileName = Path.GetFileName(incrementalBackupFile);
                File.Move(incrementalBackupFile, $"{backupToMovePath}\\{fileName}");

                await store1.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerOptions(), backupDirectory);
                using (var session = store1.OpenAsyncSession())
                {
                    var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                    var keyValuePair = users.First();
                    Assert.NotNull(keyValuePair.Value);
                    Assert.True(keyValuePair.Value.Name == "oren" && keyValuePair.Key == "users/1");
                    Assert.Null(users.Last().Value);
                }

                await store2.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerOptions(), backupToMovePath);
                using (var session = store2.OpenAsyncSession())
                {
                    var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                    Assert.Null(users.First().Value);
                    var keyValuePair = users.Last();
                    Assert.NotNull(keyValuePair.Value);
                    Assert.True(keyValuePair.Value.Name == "ayende" && keyValuePair.Key == "users/2");
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = _backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var result = await store.Admin.Server.SendAsync(new UpdatePeriodicBackupOperation(config, store.Database));
                var periodicBackupTaskId = result.TaskId;

                var operation = new GetPeriodicBackupStatusOperation(store.Database, periodicBackupTaskId);
                SpinWait.SpinUntil(() =>
                {
                    var getPeriodicBackupResult = store.Admin.Server.Send(operation);
                    return getPeriodicBackupResult.Status?.LastEtag > 0;
                }, TimeSpan.FromSeconds(10));

                var etagForBackups = store.Admin.Server.Send(operation).Status.LastEtag;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.SaveChangesAsync();
                }

                SpinWait.SpinUntil(() =>
                {
                    var newLastEtag = store.Admin.Server.Send(operation).Status.LastEtag;
                    return newLastEtag != etagForBackups;
                }, TimeSpan.FromMinutes(2));

                // restore the database with a different name
                const string databaseName = "restored_database";
                var restoreConfiguration = new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(_backupPath).First(),
                    DatabaseName = databaseName
                };
                var restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                var restoreResult = store.Admin.Server.Send(restoreBackupTask);
                var stateRequest = new GetOperationStateOperation(restoreResult.OperationId, isServerStoreOperation: true);

                SpinWait.SpinUntil(() =>
                {
                    var state = store.Admin.Server.Send(stateRequest);
                    return state.Status == OperationStatus.Completed;
                }, TimeSpan.FromMinutes(2));

                using (var session = store.OpenAsyncSession(databaseName))
                {
                    var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                    Assert.True(users.Any(x => x.Value.Name == "oren"));
                    Assert.True(users.Any(x => x.Value.Name == "ayende"));
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore_snapshot()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = _backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var result = await store.Admin.Server.SendAsync(new UpdatePeriodicBackupOperation(config, store.Database));
                var periodicBackupTaskId = result.TaskId;

                var operation = new GetPeriodicBackupStatusOperation(store.Database, periodicBackupTaskId);
                SpinWait.SpinUntil(() =>
                {
                    var getPeriodicBackupResult = store.Admin.Server.Send(operation);
                    return getPeriodicBackupResult.Status?.LastEtag > 0;
                }, TimeSpan.FromSeconds(10));
                
                var etagForBackups = store.Admin.Server.Send(operation).Status.LastEtag;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.StoreAsync(new User { Name = "ayende" }, "users/3");
                    await session.SaveChangesAsync();
                }

                SpinWait.SpinUntil(() =>
                {
                    var newLastEtag = store.Admin.Server.Send(operation).Status.LastEtag;
                    return newLastEtag != etagForBackups;
                }, TimeSpan.FromMinutes(2));

                // restore the database with a different name
                const string restoredDatabaseName = "restored_database_snapshot";
                var restoreConfiguration = new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(_backupPath).First(),
                    DatabaseName = restoredDatabaseName
                };
                var restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                var restoreResult = store.Admin.Server.Send(restoreBackupTask);
                var stateRequest = new GetOperationStateOperation(restoreResult.OperationId, isServerStoreOperation: true);
                store.Admin.Server.Send(stateRequest);

                SpinWait.SpinUntil(() =>
                {
                    var state = store.Admin.Server.Send(stateRequest);
                    return state.Status == OperationStatus.Completed;
                }, TimeSpan.FromMinutes(2));

                using (var session = store.OpenAsyncSession(restoredDatabaseName))
                {
                    var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                    Assert.True(users.Any(x => x.Value.Name == "oren"));
                    Assert.True(users.Any(x => x.Value.Name == "ayende"));
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task restore_settings_tests()
        {
            using (var store = GetDocumentStore(dbSuffixIdentifier: "2"))
            {
                var restoreConfiguration = new RestoreBackupConfiguration();

                var restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                var e = Assert.Throws<RavenException>(() => store.Admin.Server.Send(restoreBackupTask));
                Assert.Contains("Database name can't be null or empty", e.InnerException.Message);

                restoreConfiguration.DatabaseName = store.Database;
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Admin.Server.Send(restoreBackupTask));
                Assert.Contains("Cannot restore data to an existing database", e.InnerException.Message);

                restoreConfiguration.DatabaseName = "test";
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Admin.Server.Send(restoreBackupTask));
                Assert.Contains("Backup location can't be null or empty", e.InnerException.Message);

                restoreConfiguration.BackupLocation = "C:\\test";
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Admin.Server.Send(restoreBackupTask));
                Assert.Contains("Backup location doesn't exist", e.InnerException.Message);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = _backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var result = await store.Admin.Server.SendAsync(new UpdatePeriodicBackupOperation(config, store.Database));
                var periodicBackupTaskId = result.TaskId;

                var operation = new GetPeriodicBackupStatusOperation(store.Database, periodicBackupTaskId);
                SpinWait.SpinUntil(() =>
                {
                    var getPeriodicBackupResult = store.Admin.Server.Send(operation);
                    return getPeriodicBackupResult.Status?.LastEtag > 0;
                }, TimeSpan.FromSeconds(10));

                restoreConfiguration.BackupLocation = _backupPath;
                restoreConfiguration.DataDirectory = _backupPath;
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Admin.Server.Send(restoreBackupTask));
                Assert.Contains("New data directory must be empty of any files or folders", e.InnerException.Message);

                restoreConfiguration.BackupLocation = _backupPath;
                var emptyFolder = NewDataPath(suffix: "BackupFolderRestore");
                restoreConfiguration.DataDirectory = emptyFolder; ;
                restoreConfiguration.JournalsStoragePath = _backupPath;
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Admin.Server.Send(restoreBackupTask));
                Assert.Contains("Journals directory must be empty of any files or folders", e.InnerException.Message);

                restoreConfiguration.BackupLocation = _backupPath;
                restoreConfiguration.DataDirectory = emptyFolder; ;
                restoreConfiguration.JournalsStoragePath = emptyFolder;
                restoreConfiguration.IndexingStoragePath = _backupPath;
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Admin.Server.Send(restoreBackupTask));
                Assert.Contains("Indexes directory must be empty of any files or folders", e.InnerException.Message);
            }
        }
    }
}