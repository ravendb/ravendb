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
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreFromGoogleCloud : RavenTestBase
    {
        public RestoreFromGoogleCloud(ITestOutputHelper output) : base(output)
        {
        }

        private readonly string _cloudPathPrefix = $"{nameof(RestoreFromGoogleCloud)}-{Guid.NewGuid()}";
        
        [Fact]
        public void restore_google_cloud_settings_tests()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_2"
            }))
            {
                var databaseName = $"restored_database-{Guid.NewGuid()}";
                var restoreConfiguration = new RestoreFromGoogleCloudConfiguration
                {
                    DatabaseName = databaseName
                };

                var restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);

                var e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("Google Cloud Bucket name cannot be null or empty", e.InnerException.Message);

                restoreConfiguration.Settings.BucketName = "test";
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("Google Credentials JSON cannot be null or empty", e.InnerException.Message);

                restoreConfiguration.Settings.GoogleCredentialsJson = "test";
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("Wrong format for Google Credentials.", e.InnerException.Message);
            }
        }

        [GoogleCloudFact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    session.CountersFor("users/1").Increment("likes", 100);
                    await session.SaveChangesAsync();
                }
                
                var googleCloudSettings = GetGoogleCloudSettings();

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    GoogleCloudSettings = googleCloudSettings,
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
                }, 4);
                Assert.True(4 == value, $"4 == value, Got status: {status != null}, exception: {status?.Error?.Exception}");
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

                var subfolderGoogleCloudSettings = GetGoogleCloudSettings(status.FolderName);
                
                var restoreFromGoogleCloudConfiguration = new RestoreFromGoogleCloudConfiguration
                {
                    DatabaseName = databaseName,
                    Settings = subfolderGoogleCloudSettings,
                    DisableOngoingTasks = true
                };

                using (RestoreDatabaseFromCloud(store, restoreFromGoogleCloudConfiguration, TimeSpan.FromSeconds(30)))
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

        [GoogleCloudFact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore_snapshot()
        {
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

                var googleCloudSettings = GetGoogleCloudSettings();

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    GoogleCloudSettings = googleCloudSettings,
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

                var operationStatus = WaitForValue(() =>
                {
                    var backupOperation = store.Maintenance.Send(new GetOperationStateOperation(backupStatus.Status.LastOperationId.Value));
                    return backupOperation.Status;
                }, OperationStatus.Completed);
                Assert.Equal(OperationStatus.Completed, operationStatus);

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
                string databaseName = $"restored_database_snapshot-{Guid.NewGuid()}";

                var subfolderGoogleCloudSettings = GetGoogleCloudSettings(backupStatus.Status.FolderName);
                
                var restoreFromGoogleCloudConfiguration = new RestoreFromGoogleCloudConfiguration
                {
                    DatabaseName = databaseName,
                    Settings = subfolderGoogleCloudSettings
                };

                using (RestoreDatabaseFromCloud(store, restoreFromGoogleCloudConfiguration, TimeSpan.FromSeconds(300)))
                {
                    using (var session = store.OpenAsyncSession(databaseName))
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
                    var restoredDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
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

        private GoogleCloudSettings GetGoogleCloudSettings(string subPath = null)
        {
            var testSettings = GoogleCloudFactAttribute.GoogleCloudSettings;

            if (testSettings == null)
                return null;

            var remoteFolderName = string.IsNullOrEmpty(subPath)
                ? _cloudPathPrefix
                : $"{_cloudPathPrefix}/{subPath}";
            
            return new GoogleCloudSettings
            {
                BucketName = testSettings.BucketName,
                GoogleCredentialsJson = testSettings.GoogleCredentialsJson,
                RemoteFolderName = remoteFolderName
            };
        }
        
        public override void Dispose()
        {
            base.Dispose();
            
            var settings = GetGoogleCloudSettings();
            if (settings == null)
                return;

            try
            {
                using (var client = new RavenGoogleCloudClient(settings))
                {
                    var cloudObjects = client.ListObjectsAsync(settings.RemoteFolderName).GetAwaiter().GetResult();

                    foreach (var cloudObject in cloudObjects)
                    {
                        try
                        {
                            client.DeleteObjectAsync(cloudObject.Name).GetAwaiter().GetResult();
                        }
                        catch (Exception)
                        {
                            // ignored
                        }
                    }
                }
            }
            catch (Exception)
            {
                // ignored
            }
        }
    }
}
