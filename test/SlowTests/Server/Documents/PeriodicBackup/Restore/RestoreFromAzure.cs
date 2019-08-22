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
using System.Security.Cryptography.X509Certificates;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Azure;

namespace SlowTests.Server.Documents.PeriodicBackup.Restore
{
    public class RestoreFromAzure : RavenTestBase
    {
        private const string AzureAccountName = "devstoreaccount1";
        private const string AzureAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
        private AzureSettings _azureSettings = GenerateAzureSettings();

        [Fact, Trait("Category", "Smuggler")]
        public void restore_azure_cloud_settings_tests()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_2"
            }))
            {
                var databaseName = $"restored_database-{Guid.NewGuid()}";
                var restoreConfiguration = new RestoreFromAzureConfiguration
                {
                    DatabaseName = databaseName
                };

                var restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);

                var e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("Account key cannot be null or empty", e.InnerException.Message);

                restoreConfiguration.Settings.AccountKey = "test";
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("Account name cannot be null or empty", e.InnerException.Message);

                restoreConfiguration.Settings.AccountName = "test";
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("Storage container cannot be null or empty", e.InnerException.Message);
            }
        }

        [AzureStorageEmulatorFact]
        public void can_backup_and_restore()
        {

            InitContainer();
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
                    AzureSettings = _azureSettings,
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (store.Maintenance.Send(new UpdatePeriodicBackupOperation(config))).TaskId;
                store.Maintenance.Send(new StartBackupOperation(true, backupTaskId));
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

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "ayende" }, "users/2");
                    session.CountersFor("users/2").Increment("downloads", 200);

                     session.SaveChanges();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                 store.Maintenance.Send(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                _azureSettings.RemoteFolderName = $"{backupStatus.Status.FolderName}";
                var restoreFromGoogleCloudConfiguration = new RestoreFromAzureConfiguration()
                {
                    DatabaseName = databaseName,
                    Settings = _azureSettings
                };
                var googleCloudOperation = new RestoreBackupOperation(restoreFromGoogleCloudConfiguration);
                var restoreOperation = store.Maintenance.Server.Send(googleCloudOperation);

                restoreOperation.WaitForCompletion(TimeSpan.FromSeconds(30));
                {
                    using (var session = store.OpenSession(databaseName))
                    {
                        var users =  session.Load<User>(new[] { "users/1", "users/2" });
                        Assert.True(users.Any(x => x.Value.Name == "oren"));
                        Assert.True(users.Any(x => x.Value.Name == "ayende"));

                        var val =  session.CountersFor("users/1").Get("likes");
                        Assert.Equal(100, val);
                        val = session.CountersFor("users/2").Get("downloads");
                        Assert.Equal(200, val);
                    }

                    var originalDatabase = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).Result;
                    var restoredDatabase = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).Result;
                    using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(ctx);
                        Assert.Equal($"A:7-{originalDatabase.DbBase64Id}, A:8-{restoredDatabase.DbBase64Id}", databaseChangeVector);
                    }
                }
            }
        }

        private void InitContainer()
        {
            using (var client = new RavenAzureClient(_azureSettings))
            {
                client.DeleteContainer();
                client.PutContainer();
            }
        }

        public static AzureSettings GenerateAzureSettings(string containerName = "mycontainer")
        {
            return new AzureSettings
            {
                AccountName = AzureAccountName,
                AccountKey = AzureAccountKey,
                StorageContainer = containerName,
                RemoteFolderName = ""
            };
        }
    }
}
