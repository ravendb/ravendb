using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup.Restore
{
    public abstract class RestoreFromS3 : RavenTestBase
    {
        protected RestoreFromS3(ITestOutputHelper output) : base(output)
        {
        }

        protected readonly string _cloudPathPrefix = $"{nameof(RestoreFromS3)}-{Guid.NewGuid()}";

        protected async Task can_backup_and_restore_internal()
        {
            var defaultS3Settings = GetS3Settings();

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", 100);
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    S3Settings = defaultS3Settings,
                    IncrementalBackupFrequency = "0 0 1 1 *"
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                PeriodicBackupStatus status = null;
                var value = WaitForValue(() =>
                {
                    status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, expectedVal: 4, timeout: 30_000);
                Assert.True(4 == value, $"gotStatus? {status != null}, Status Error: {status?.Error?.Exception}," +
                                        $" S3 Error: {status?.UploadToS3?.Exception}, LocalBackup Exception: {status?.LocalBackup?.Exception}");
                Assert.True(status.LastOperationId != null, $"status.LastOperationId != null, Got status: {status != null}, exception: {status?.Error?.Exception}");

                OperationState backupOperation = null;
                var operationStatus = WaitForValue(() =>
                {
                    backupOperation = store.Maintenance.Send(new GetOperationStateOperation(status.LastOperationId.Value));
                    return backupOperation.Status;
                }, OperationStatus.Completed);
                Assert.Equal(OperationStatus.Completed, operationStatus);

                var backupResult = backupOperation.Result as BackupResult;
                Assert.NotNull(backupResult);
                Assert.True(backupResult.Counters.Processed, "backupResult.Counters.Processed");
                Assert.Equal(1, backupResult.Counters.ReadCount);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    session.CountersFor("users/2").Increment("downloads", 200);

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() =>
                {
                    status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, expectedVal: lastEtag, timeout: 30_000);
                Assert.True(lastEtag == value, $"gotStatus? {status != null}, Status Error: {status?.Error?.Exception}," +
                                               $" S3 Error: {status?.UploadToS3?.Exception}, LocalBackup Exception: {status?.LocalBackup?.Exception}");

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                var subfolderS3Settings = GetS3Settings(status.FolderName);

                using (RestoreDatabaseFromCloud(
                    store,
                    new RestoreFromS3Configuration { DatabaseName = databaseName, Settings = subfolderS3Settings, DisableOngoingTasks = true },
                    TimeSpan.FromSeconds(60)))
                {
                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                        Assert.True(users.Any(x => x.Value.Name == "oren"));
                        Assert.True(users.Any(x => x.Value.Name == "ayende"));

                        var val = await session.CountersFor("users/1").GetAsync("likes");
                        Assert.Equal(100, val);
                        val = await session.CountersFor("users/2").GetAsync("downloads");
                        Assert.Equal(200, val);
                    }

                    var originalDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var restoredDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                    using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(ctx);
                        Assert.Contains($"A:7-{originalDatabase.DbBase64Id}", databaseChangeVector);
                        Assert.Contains($"A:8-{restoredDatabase.DbBase64Id}", databaseChangeVector);
                    }
                }
            }
        }

        protected async Task can_backup_and_restore_snapshot_internal()
        {
            var defaultS3Settings = GetS3Settings();

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session
                        .Query<User>()
                        .Where(x => x.Name == "oren")
                        .ToListAsync(); // create an index to backup

                    await session
                        .Query<Order>()
                        .Where(x => x.Freight > 20)
                        .ToListAsync(); // create an index to backup

                    session.CountersFor("users/1").Increment("likes", 100); //create a counter to backup
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    S3Settings = defaultS3Settings,
                    IncrementalBackupFrequency = "0 0 1 1 *"
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                PeriodicBackupStatus backupStatus = null;
                var value = WaitForValue(() =>
                {
                    backupStatus = store.Maintenance.Send(operation).Status;
                    return backupStatus?.LastEtag;
                }, expectedVal: 4, timeout: 30_000);
                Assert.True(4 == value, $"gotStatus? {backupStatus != null}, Status Error: {backupStatus?.Error?.Exception}," +
                                        $" S3 Error: {backupStatus?.UploadToS3?.Exception}, LocalBackup Exception: {backupStatus?.LocalBackup?.Exception}");
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.StoreAsync(new User { Name = "ayende" }, "users/3");

                    session.CountersFor("users/2").Increment("downloads", 200);

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() =>
                {
                    backupStatus = store.Maintenance.Send(operation).Status;
                    return backupStatus?.LastEtag;
                }, expectedVal: lastEtag, timeout: 30_000);
                Assert.True(lastEtag == value, $"gotStatus? {backupStatus != null}, Status Error: {backupStatus?.Error?.Exception}," +
                                               $" S3 Error: {backupStatus?.UploadToS3?.Exception}, LocalBackup Exception: {backupStatus?.LocalBackup?.Exception}");

                // restore the database with a different name
                string restoredDatabaseName = $"restored_database_snapshot-{Guid.NewGuid()}";

                var subfolderS3Settings = GetS3Settings(backupStatus.FolderName);

                using (RestoreDatabaseFromCloud(store,
                    new RestoreFromS3Configuration
                    {
                        DatabaseName = restoredDatabaseName,
                        Settings = subfolderS3Settings
                    },
                    TimeSpan.FromSeconds(60)))
                {
                    using (var session = store.OpenAsyncSession(restoredDatabaseName))
                    {
                        var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                        Assert.NotNull(users["users/1"]);
                        Assert.NotNull(users["users/2"]);
                        Assert.True(users.Any(x => x.Value.Name == "oren"));
                        Assert.True(users.Any(x => x.Value.Name == "ayende"));

                        var val = await session.CountersFor("users/1").GetAsync("likes");
                        Assert.Equal(100, val);
                        val = await session.CountersFor("users/2").GetAsync("downloads");
                        Assert.Equal(200, val);
                    }

                    var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfIndexes);

                    var originalDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var restoredDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(restoredDatabaseName);
                    using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(ctx);
                        Assert.Contains($"A:8-{originalDatabase.DbBase64Id}", databaseChangeVector);
                        Assert.Contains($"A:10-{restoredDatabase.DbBase64Id}", databaseChangeVector);
                    }
                }
            }
        }

        protected async Task incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_database_key_internal()
        {
            var defaultS3Settings = GetS3Settings();

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
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "oren"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    S3Settings = defaultS3Settings,
                    IncrementalBackupFrequency = "0 */6 * * *",
                    BackupEncryptionSettings = new BackupEncryptionSettings
                    {
                        EncryptionMode = EncryptionMode.UseDatabaseKey
                    }
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);

                PeriodicBackupStatus backupStatus = null;
                var value = WaitForValue(() =>
                {
                    backupStatus = store.Maintenance.Send(operation).Status;
                    return backupStatus?.LastEtag;
                }, expectedVal: 1, timeout: 30_000);
                Assert.True(1 == value, $"gotStatus? {backupStatus != null}, Status Error: {backupStatus?.Error?.Exception}," +
                                        $" S3 Error: {backupStatus?.UploadToS3?.Exception}, LocalBackup Exception: {backupStatus?.LocalBackup?.Exception}");

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.StoreAsync(new User { Name = "ayende" }, "users/3");

                    session.CountersFor("users/2").Increment("downloads", 200);

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() =>
                {
                    backupStatus = store.Maintenance.Send(operation).Status;
                    return backupStatus?.LastEtag;
                }, expectedVal: lastEtag, timeout: 30_000);
                Assert.True(lastEtag == value, $"gotStatus? {backupStatus != null}, Status Error: {backupStatus?.Error?.Exception}," +
                                               $" S3 Error: {backupStatus?.UploadToS3?.Exception}, LocalBackup Exception: {backupStatus?.LocalBackup?.Exception}");

                var databaseName = $"restored_database-{Guid.NewGuid()}";

                var subfolderS3Settings = GetS3Settings(backupStatus.FolderName);

                using (RestoreDatabaseFromCloud(store, new RestoreFromS3Configuration
                {
                    Settings = subfolderS3Settings,
                    DatabaseName = databaseName,
                    BackupEncryptionSettings = new BackupEncryptionSettings
                    {
                        Key = key,
                        EncryptionMode = EncryptionMode.UseProvidedKey
                    }
                }))
                {
                    using (var session = store.OpenSession(databaseName))
                    {
                        var users = session.Load<User>("users/1");
                        Assert.NotNull(users);
                        users = session.Load<User>("users/2");
                        Assert.NotNull(users);
                    }
                }
            }
        }

        protected async Task incremental_and_full_check_last_file_for_backup_internal()
        {
            var defaultS3Settings = GetS3Settings();

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "user-1" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    S3Settings = defaultS3Settings,
                    IncrementalBackupFrequency = "0 */6 * * *",
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);

                PeriodicBackupStatus backupStatus = null;
                var value = WaitForValue(() =>
                {
                    backupStatus = store.Maintenance.Send(operation).Status;
                    return backupStatus?.LastEtag;
                }, expectedVal: 1, timeout: 30_000);
                Assert.True(1 == value, $"gotStatus? {backupStatus != null}, Status Error: {backupStatus?.Error?.Exception}," +
                                        $" S3 Error: {backupStatus?.UploadToS3?.Exception}, LocalBackup Exception: {backupStatus?.LocalBackup?.Exception}");

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "user-2" }, "users/2");

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() =>
                {
                    backupStatus = store.Maintenance.Send(operation).Status;
                    return backupStatus?.LastEtag;
                }, expectedVal: lastEtag, timeout: 30_000);
                Assert.True(lastEtag == value, $"gotStatus? {backupStatus != null}, Status Error: {backupStatus?.Error?.Exception}," +
                                               $" S3 Error: {backupStatus?.UploadToS3?.Exception}, LocalBackup Exception: {backupStatus?.LocalBackup?.Exception}");

                string lastFileToRestore;
                using (var client = new RavenAwsS3Client(defaultS3Settings))
                {
                    var fullBackupPath = $"{defaultS3Settings.RemoteFolderName}/{backupStatus.FolderName}";
                    lastFileToRestore = (await client.ListObjectsAsync(fullBackupPath, string.Empty, false)).FileInfoDetails.Last().FullPath;
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "user-3" }, "users/3");

                    await session.SaveChangesAsync();
                }

                lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() =>
                {
                    backupStatus = store.Maintenance.Send(operation).Status;
                    return backupStatus?.LastEtag;
                }, expectedVal: lastEtag, timeout: 30_000);
                Assert.True(lastEtag == value, $"gotStatus? {backupStatus != null}, Status Error: {backupStatus?.Error?.Exception}," +
                                               $" S3 Error: {backupStatus?.UploadToS3?.Exception}, LocalBackup Exception: {backupStatus?.LocalBackup?.Exception}");

                var databaseName = $"restored_database-{Guid.NewGuid()}";

                var subfolderS3Settings = GetS3Settings(backupStatus.FolderName);

                using (RestoreDatabaseFromCloud(store,
                    new RestoreFromS3Configuration
                    {
                        Settings = subfolderS3Settings,
                        DatabaseName = databaseName,
                        LastFileNameToRestore = lastFileToRestore
                    }))
                {
                    using (var session = store.OpenSession(databaseName))
                    {
                        var users = session.Load<User>("users/1");
                        Assert.NotNull(users);
                        users = session.Load<User>("users/2");
                        Assert.NotNull(users);
                        users = session.Load<User>("users/3");
                        Assert.Null(users);
                    }
                }
            }
        }

        protected async Task incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_provided_key_internal()
        {
            var defaultS3Settings = GetS3Settings();
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
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "oren"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    S3Settings = defaultS3Settings,
                    IncrementalBackupFrequency = "0 */6 * * *",
                    BackupEncryptionSettings = new BackupEncryptionSettings
                    {
                        Key = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                        EncryptionMode = EncryptionMode.UseProvidedKey
                    }
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);

                PeriodicBackupStatus backupStatus = null;
                var value = WaitForValue(() =>
                {
                    backupStatus = store.Maintenance.Send(operation).Status;
                    return backupStatus?.LastEtag;
                }, expectedVal: 1, timeout: 30_000);
                Assert.True(1 == value, $"gotStatus? {backupStatus != null}, Status Error: {backupStatus?.Error?.Exception}," +
                                   $" S3 Error: {backupStatus?.UploadToS3?.Exception}, LocalBackup Exception: {backupStatus?.LocalBackup?.Exception}");

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.StoreAsync(new User { Name = "ayende" }, "users/3");

                    session.CountersFor("users/2").Increment("downloads", 200);

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() =>
                {
                    backupStatus = store.Maintenance.Send(operation).Status;
                    return backupStatus?.LastEtag;
                }, expectedVal: lastEtag, timeout: 30_000);
                Assert.True(lastEtag == value, $"gotStatus? {backupStatus != null}, Status Error: {backupStatus?.Error?.Exception}," +
                                               $" S3 Error: {backupStatus?.UploadToS3?.Exception}, LocalBackup Exception: {backupStatus?.LocalBackup?.Exception}");
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                var subfolderS3Settings = GetS3Settings(backupStatus.FolderName);

                using (RestoreDatabaseFromCloud(store, new RestoreFromS3Configuration
                {
                    Settings = subfolderS3Settings,
                    DatabaseName = databaseName,
                    EncryptionKey = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                    BackupEncryptionSettings = new BackupEncryptionSettings
                    {
                        Key = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs=",
                        EncryptionMode = EncryptionMode.UseProvidedKey
                    }
                }))
                {
                    using (var session = store.OpenSession(databaseName))
                    {
                        var users = session.Load<User>("users/1");
                        Assert.NotNull(users);
                        users = session.Load<User>("users/2");
                        Assert.NotNull(users);
                    }
                }
            }
        }

        protected async Task snapshot_encrypted_db_and_restore_to_encrypted_DB_internal()
        {
            var key = EncryptedServer(out var certificates, out string dbName);

            var defaultS3Settings = GetS3Settings();
            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = certificates.ServerCertificate.Value,
                ClientCertificate = certificates.ServerCertificate.Value,
                ModifyDatabaseName = s => dbName,
                ModifyDatabaseRecord = record => record.Encrypted = true
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "oren"
                    }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    S3Settings = defaultS3Settings,
                    IncrementalBackupFrequency = "0 */6 * * *"
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);

                PeriodicBackupStatus backupStatus = null;
                var value = WaitForValue(() =>
                {
                    backupStatus = store.Maintenance.Send(operation).Status;
                    return backupStatus?.LastEtag;
                }, expectedVal: 1, timeout: 30_000);
                Assert.True(1 == value, $"gotStatus? {backupStatus != null}, Status Error: {backupStatus?.Error?.Exception}," +
                                        $" S3 Error: {backupStatus?.UploadToS3?.Exception}, LocalBackup Exception: {backupStatus?.LocalBackup?.Exception}");

                var databaseName = GetDatabaseName();
                var subfolderS3Settings = GetS3Settings(backupStatus.FolderName);

                using (RestoreDatabaseFromCloud(store, new RestoreFromS3Configuration
                {
                    Settings = subfolderS3Settings,
                    DatabaseName = databaseName,
                    EncryptionKey = key,
                    BackupEncryptionSettings = new BackupEncryptionSettings
                    {
                        EncryptionMode = EncryptionMode.UseDatabaseKey
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

        protected abstract S3Settings GetS3Settings(string subPath = null);

        public override void Dispose()
        {
            base.Dispose();

            var s3Settings = GetS3Settings();
            if (s3Settings == null)
                return;

            try
            {
                using (var s3Client = new RavenAwsS3Client(s3Settings))
                {
                    var cloudObjects = s3Client.ListObjectsAsync(s3Settings.RemoteFolderName, string.Empty, false).GetAwaiter().GetResult();
                    var pathsToDelete = cloudObjects.FileInfoDetails.Select(x => x.FullPath).ToList();

                    s3Client.DeleteMultipleObjects(pathsToDelete);
                }
            }
            catch (Exception)
            {
                // ignored
            }
        }
    }
}
