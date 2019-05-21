using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class ServerWideBackup : RavenTestBase
    {
        [Fact]
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

                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
                var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation());
                ValidateServerWideConfiguration(serverWideConfiguration, putConfiguration);

                // the configuration is applied to existing databases
                var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                ValidateBackupConfiguration(serverWideConfiguration, record1.PeriodicBackups.First(), store.Database);

                // the configuration is applied to new databases
                var newDbName = store.Database + "-testDatabase";
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(newDbName)));
                var record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                ValidateBackupConfiguration(serverWideConfiguration, record2.PeriodicBackups.First(), newDbName);

                // update the backup configuration
                putConfiguration.FullBackupFrequency = "3 2 * * 1";
                putConfiguration.LocalSettings.FolderPath += "/folder1";
                putConfiguration.S3Settings.RemoteFolderName += "/folder2";
                putConfiguration.AzureSettings.RemoteFolderName += "/folder3";
                putConfiguration.FtpSettings.Url += "/folder4";

                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(putConfiguration));
                serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideBackupConfigurationOperation());
                ValidateServerWideConfiguration(serverWideConfiguration, putConfiguration);

                record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                ValidateBackupConfiguration(serverWideConfiguration, record1.PeriodicBackups.First(), store.Database);
                record2 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                ValidateBackupConfiguration(serverWideConfiguration, record2.PeriodicBackups.First(), newDbName);
            }
        }

        [Fact]
        public async Task UpdateOfServerWideBackupThroughUpdatePeriodicBackupFails()
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
                var backupConfiguration = new PeriodicBackupConfiguration
                {
                    Disabled = true,
                    TaskId = currentBackupConfiguration.TaskId,
                    FullBackupFrequency = "0 2 * * 0",
                    IncrementalBackupFrequency = "0 2 * * 1"
                };

                var e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfiguration)));
                var expectedError = $"Can't update task id: {currentBackupConfiguration.TaskId}, name: 'Server Wide Backup Configuration', because it is a server wide backup task";
                Assert.Contains(expectedError, e.Message);

                backupConfiguration.TaskId = 0;
                backupConfiguration.Name = currentBackupConfiguration.Name;
                e = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfiguration)));
                Assert.Contains("Can't update task name 'Server Wide Backup Configuration', because it is a server wide backup task", e.Message);
            }
        }

        private static void ValidateServerWideConfiguration(ServerWideBackupConfiguration serverWideConfiguration, ServerWideBackupConfiguration putConfiguration)
        {
            Assert.Equal(ServerWideBackupConfiguration.ConfigurationName, serverWideConfiguration.Name);
            Assert.Equal(putConfiguration.Name, serverWideConfiguration.Name);
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

        private static void ValidateBackupConfiguration(ServerWideBackupConfiguration serverWideConfiguration, 
            PeriodicBackupConfiguration backupConfiguration, string databaseName)
        {
            Assert.Equal(serverWideConfiguration.Name, backupConfiguration.Name);
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
    }
}
