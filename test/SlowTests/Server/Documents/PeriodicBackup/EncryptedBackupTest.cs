using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions;
using Raven.Server.Config;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class EncryptedBackupTest : RavenTestBase
    {
        [Fact]
        public async Task can_backup_and_restore_encrypted()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
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
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "0 */6 * * *",
                    EncryptionSettings = new EncryptionSettings
                    {
                        Key = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs="
                    }
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 3);
                Assert.Equal(3, value);

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

                using (RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DatabaseName = databaseName,
                    EncryptionSettings = new EncryptionSettings
                    {
                        Key = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs="
                    }
                }))
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
                }
            }
        }

        [Fact]
        public async Task can_backup_and_restore_sample_data_encrypted()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.PerformanceHints.MaxNumberOfResults)] = "1"
            }))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session.Query<Employee>().ToList(); // this will generate performance hint
                }

                var database = await GetDatabase(store.Database);
                database.NotificationCenter.Paging.UpdatePaging(null);

                int beforeBackupAlertCount;
                using (database.NotificationCenter.GetStored(out var actions))
                    beforeBackupAlertCount = actions.Count();

                Assert.True(beforeBackupAlertCount > 0);

                var beforeBackupStats = store.Maintenance.Send(new GetStatisticsOperation());

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "0 */6 * * *",
                    EncryptionSettings = new EncryptionSettings
                    {
                        Key = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs="
                    }
                };

                var backupTaskId = (store.Maintenance.Send(new UpdatePeriodicBackupOperation(config))).TaskId;
                store.Maintenance.Send(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                SpinWait.SpinUntil(() =>
                {
                    var getPeriodicBackupResult = store.Maintenance.Send(operation);
                    return getPeriodicBackupResult.Status?.LastEtag > 0;
                }, TimeSpan.FromSeconds(15));

                // restore the database with a different name
                var restoredDatabaseName = GetDatabaseName();

                using (RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DatabaseName = restoredDatabaseName, 
                    EncryptionSettings = new EncryptionSettings
                    {
                        Key = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs="
                    }
                }))
                {
                    var afterRestoreStats = store.Maintenance.ForDatabase(restoredDatabaseName).Send(new GetStatisticsOperation());

                    var restoredDatabase = await GetDatabase(restoredDatabaseName);

                    int afterRestoreAlertCount;
                    using (restoredDatabase.NotificationCenter.GetStored(out var actions))
                        afterRestoreAlertCount = actions.Count();

                    Assert.True(afterRestoreAlertCount > 0);

                    var indexesPath = restoredDatabase.Configuration.Indexing.StoragePath;
                    var indexesDirectory = new DirectoryInfo(indexesPath.FullPath);
                    Assert.True(indexesDirectory.Exists);
                    Assert.Equal(afterRestoreStats.CountOfIndexes, indexesDirectory.GetDirectories().Length);

                    Assert.NotEqual(beforeBackupStats.DatabaseId, afterRestoreStats.DatabaseId);
                    Assert.Equal(beforeBackupStats.CountOfAttachments, afterRestoreStats.CountOfAttachments);
                    Assert.Equal(beforeBackupStats.CountOfConflicts, afterRestoreStats.CountOfConflicts);
                    Assert.Equal(beforeBackupStats.CountOfDocuments, afterRestoreStats.CountOfDocuments);
                    Assert.Equal(beforeBackupStats.CountOfDocumentsConflicts, afterRestoreStats.CountOfDocumentsConflicts);
                    Assert.Equal(beforeBackupStats.CountOfIndexes, afterRestoreStats.CountOfIndexes);
                    Assert.Equal(beforeBackupStats.CountOfRevisionDocuments, afterRestoreStats.CountOfRevisionDocuments);
                    Assert.Equal(beforeBackupStats.CountOfTombstones, afterRestoreStats.CountOfTombstones);
                    Assert.Equal(beforeBackupStats.CountOfUniqueAttachments, afterRestoreStats.CountOfUniqueAttachments);
                }
            }
        }

        [Fact]
        public unsafe void failed_to_restore_wrong_key()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                     session.Store(new User { Name = "oren" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", 100);
                    session.SaveChanges();
                }

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "0 */6 * * *",
                    EncryptionSettings = new EncryptionSettings
                    {
                        Key = "OI7Vll7DroXdUORtc6Uo64wdAk1W0Db9ExXXgcg5IUs="
                    }
                };

                var backupTaskId = (store.Maintenance.Send(new UpdatePeriodicBackupOperation(config))).TaskId;
                store.Maintenance.Send(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 3);
                Assert.Equal(3, value);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                var key = new byte[(int)Sodium.crypto_secretstream_xchacha20poly1305_keybytes()];
                fixed (byte* pKey = key)
                {
                    Sodium.crypto_secretstream_xchacha20poly1305_keygen(pKey);
                }

                var e = Assert.Throws<RavenException>(() =>
                {
                    using (RestoreDatabase(store, new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(backupPath).First(),
                        DatabaseName = databaseName,
                        EncryptionSettings = new EncryptionSettings
                        {
                            Key = Convert.ToBase64String(key)
                        }
                    }))
                    {
                    }
                });
                Assert.IsType<CryptographicException>(e.InnerException);
            }
        }
    }
}
