using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using FastTests.Server.Basic.Entities;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Raven.Client.ServerWide.Operations.Certificates;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class RestoreFromS3 : RavenTestBase
    {
        [Fact, Trait("Category", "Smuggler")]
        public void restore_s3_settings_tests()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_2"
            }))
            {
                var databaseName = $"restored_database-{Guid.NewGuid()}";
                var restoreConfiguration = new RestoreFromS3Configuration
                {
                    DatabaseName = databaseName
                };

                var restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);

                var e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("AWS access key cannot be null or empty", e.InnerException.Message);

                restoreConfiguration.Settings.AwsAccessKey = "test";
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("AWS secret key cannot be null or empty", e.InnerException.Message);

                restoreConfiguration.Settings.AwsSecretKey = "test";
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("AWS region cannot be null or empty", e.InnerException.Message);

                restoreConfiguration.Settings.AwsRegionName = "test";
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("AWS Bucket name cannot be null or empty", e.InnerException.Message);
            }
        }

        [S3Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore()
        {
            var localS3Settings = CopyLocalS3Settings();

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
                    S3Settings = localS3Settings,
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 4);
                Assert.Equal(4, value);

                var backupStatus = store.Maintenance.Send(operation);
                var backupOperationId = backupStatus.Status.LastOperationId;

                var backupOperation = store.Maintenance.Send(new GetOperationStateOperation(backupOperationId.Value));

                var backupResult = backupOperation.Result as BackupResult;
                Assert.True(backupResult.Counters.Processed);
                Assert.Equal(1, backupResult.Counters.ReadCount);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    session.CountersFor("users/2").Increment("downloads", 200);

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                localS3Settings.RemoteFolderName = $"{backupStatus.Status.FolderName}";
                var restoreFromS3Configuration = new RestoreFromS3Configuration
                {
                    DatabaseName = databaseName,
                    Settings = localS3Settings
                };
                var s3Operation = new RestoreBackupOperation(restoreFromS3Configuration);
                var restoreOperation = store.Maintenance.Server.Send(s3Operation);

                restoreOperation.WaitForCompletion(TimeSpan.FromSeconds(30));
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
                        Assert.Equal($"A:7-{originalDatabase.DbBase64Id}, A:8-{restoredDatabase.DbBase64Id}", databaseChangeVector);
                    }
                }
            }
        }

        [S3Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore_snapshot()
        {
            var localS3Settings = CopyLocalS3Settings();
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
                    S3Settings = localS3Settings,
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 4);
                Assert.Equal(4, value);
                var backupStatus = store.Maintenance.Send(operation);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.StoreAsync(new User { Name = "ayende" }, "users/3");

                    session.CountersFor("users/2").Increment("downloads", 200);

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);

                // restore the database with a different name
                string restoredDatabaseName = $"restored_database_snapshot-{Guid.NewGuid()}";
                localS3Settings.RemoteFolderName = $"{backupStatus.Status.FolderName}";

                using (RestoreDatabaseFromCloud(store, new RestoreFromS3Configuration
                {
                    DatabaseName = restoredDatabaseName,
                    Settings = localS3Settings

                }))
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
                        Assert.Equal($"A:8-{originalDatabase.DbBase64Id}, A:10-{restoredDatabase.DbBase64Id}", databaseChangeVector);
                    }
                }
            }
        }

        [S3Fact, Trait("Category", "Smuggler")]
        public async Task incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_database_key()
        {
            var localS3Settings = CopyLocalS3Settings();
            var key = EncryptedServer(out X509Certificate2 adminCert, out string dbName);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
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
                    S3Settings = localS3Settings,
                    IncrementalBackupFrequency = "0 */6 * * *",
                    BackupEncryptionSettings = new BackupEncryptionSettings
                    {
                        EncryptionMode = EncryptionMode.UseDatabaseKey
                    }
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);

                var value = WaitForValue(() =>
                {
                    var getPeriodicBackupResult = store.Maintenance.Send(operation);
                    return getPeriodicBackupResult.Status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.StoreAsync(new User { Name = "ayende" }, "users/3");

                    session.CountersFor("users/2").Increment("downloads", 200);

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);

                var backupStatus = store.Maintenance.Send(operation);
                var databaseName = $"restored_database-{Guid.NewGuid()}";
                localS3Settings.RemoteFolderName = $"{backupStatus.Status.FolderName}";

                using (RestoreDatabaseFromCloud(store, new RestoreFromS3Configuration
                {
                    Settings = localS3Settings,
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

        [S3Fact, Trait("Category", "Smuggler")]
        public async Task incremental_and_full_backup_encrypted_db_and_restore_to_encrypted_DB_with_provided_key()
        {
            var localS3Settings = CopyLocalS3Settings();
            var key = EncryptedServer(out X509Certificate2 adminCert, out string dbName);

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
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
                    S3Settings = localS3Settings,
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

                var value = WaitForValue(() =>
                {
                    var getPeriodicBackupResult = store.Maintenance.Send(operation);
                    return getPeriodicBackupResult.Status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.StoreAsync(new User { Name = "ayende" }, "users/3");

                    session.CountersFor("users/2").Increment("downloads", 200);

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);

                var backupStatus = store.Maintenance.Send(operation);
                var databaseName = $"restored_database-{Guid.NewGuid()}";
                localS3Settings.RemoteFolderName = $"{backupStatus.Status.FolderName}";

                using (RestoreDatabaseFromCloud(store, new RestoreFromS3Configuration
                {
                    Settings = localS3Settings,
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

        [S3Fact, Trait("Category", "Smuggler")]
        public async Task snapshot_encrypted_db_and_restore_to_encrypted_DB()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            var key = EncryptedServer(out X509Certificate2 adminCert, out string dbName);

            var localS3Settings = CopyLocalS3Settings();

            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = adminCert,
                ClientCertificate = adminCert,
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
                    BackupType = BackupType.Snapshot,
                    S3Settings = localS3Settings,
                    IncrementalBackupFrequency = "0 */6 * * *"
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);

                var value = WaitForValue(() =>
                {
                    var getPeriodicBackupResult = store.Maintenance.Send(operation);
                    return getPeriodicBackupResult.Status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);

                var backupStatus = store.Maintenance.Send(operation);
                var databaseName = $"restored_database-{Guid.NewGuid()}";
                localS3Settings.RemoteFolderName = $"{backupStatus.Status.FolderName}";

                using (RestoreDatabaseFromCloud(store, new RestoreFromS3Configuration
                {
                    Settings = localS3Settings,
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

        private string EncryptedServer(out X509Certificate2 adminCert, out string name)
        {
            var serverCertPath = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            adminCert = AskServerForClientCertificate(serverCertPath, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }

            var base64Key = Convert.ToBase64String(buffer);

            // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
            try
            {
                ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
            }
            catch (PlatformNotSupportedException)
            {
                // so we fall back to a file
                Server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();
            }

            Server.ServerStore.PutSecretKey(base64Key, dbName, true);
            name = dbName;
            return Convert.ToBase64String(buffer);
        }

        private static S3Settings CopyLocalS3Settings()
        {
            return new S3Settings
            {
                BucketName = S3Fact.S3Settings.BucketName,
                RemoteFolderName = S3Fact.S3Settings.RemoteFolderName,
                AwsAccessKey = S3Fact.S3Settings.AwsAccessKey,
                AwsRegionName = S3Fact.S3Settings.AwsRegionName,
                AwsSecretKey = S3Fact.S3Settings.AwsSecretKey,
                AwsSessionToken = S3Fact.S3Settings.AwsSessionToken,
            };
        }
    }
}
