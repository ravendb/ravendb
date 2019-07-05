using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class Retention : RavenTestBase
    {
        [Theory]
        [InlineData(20, 5)]
        [InlineData(20, 20)]
        [InlineData(25, 10)]
        [InlineData(30, 3)]
        [InlineData(40, 20)]
        [InlineData(45, 1)]
        [InlineData(50, 50)]
        [InlineData(70, 13)]
        public async Task can_delete_backups_by_date(int backupAgeInSeconds, int numberOfBackupsToCreate)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            await CanDeleteBackupsByDate(backupAgeInSeconds, numberOfBackupsToCreate,
                (configuration, _) =>
                {
                    configuration.LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    };
                },
                _ =>
                {
                    var directories = Directory.GetDirectories(backupPath)
                        .Where(x => Directory.GetFiles(x).Any(BackupUtils.IsFullBackupOrSnapshot));

                    return Task.FromResult(directories.Count());
                }, timeout: 15000);
        }

        [Theory]
        //[Theory(Skip = "Requires Amazon AWS Credentials")]
        [InlineData(20, 5)]
        [InlineData(20, 20)]
        [InlineData(25, 10)]
        [InlineData(30, 3)]
        [InlineData(40, 20)]
        [InlineData(45, 1)]
        [InlineData(50, 50)]
        [InlineData(70, 13)]
        public async Task can_delete_backups_by_date_s3(int backupAgeInSeconds, int numberOfBackupsToCreate)
        {
            await CanDeleteBackupsByDate(backupAgeInSeconds, numberOfBackupsToCreate,
                (configuration, databaseName) =>
                {
                    configuration.S3Settings = GetS3Settings(databaseName);
                },
                async databaseName =>
                {
                    using (var client = new RavenAwsS3Client(GetS3Settings(databaseName)))
                    {
                        var folders = await client.ListObjects($"{client.RemoteFolderName}/", "/", listFolders: true);
                        return folders.FileInfoDetails.Count;
                    }
                }, timeout: 120000);
        }

        private async Task CanDeleteBackupsByDate(
            int backupAgeInSeconds,
            int numberOfBackupsToCreate,
            Action<PeriodicBackupConfiguration, string> modifyConfiguration, 
            Func<string, Task<int>> getDirectoriesCount, 
            int timeout)
        {
            var minimumBackupAgeToKeep = TimeSpan.FromSeconds(backupAgeInSeconds);

            using (var store = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration
                {
                    IncrementalBackupFrequency = "30 3 L * ?",
                    RetentionPolicy = new RetentionPolicy
                    {
                        MinimumBackupAgeToKeep = minimumBackupAgeToKeep
                    }
                };

                modifyConfiguration(config, store.Database);

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;

                var lastEtag = 0L;
                for (var i = 0; i < numberOfBackupsToCreate; i++)
                {
                    string userId;

                    using (var session = store.OpenAsyncSession())
                    {
                        var user = new User { Name = "Grisha" };
                        await session.StoreAsync(user);
                        userId = user.Id;
                        await session.SaveChangesAsync();
                    }

                    // create full backup
                    lastEtag = await CreateBackup(store, true, backupTaskId, lastEtag, timeout);

                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>(userId);
                        user.Age = 33;
                        await session.SaveChangesAsync();
                    }

                    // create incremental backup
                    lastEtag = await CreateBackup(store, false, backupTaskId, lastEtag, timeout);
                }

                await Task.Delay(minimumBackupAgeToKeep + TimeSpan.FromSeconds(5));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Grisha" });
                    await session.SaveChangesAsync();
                }
                await CreateBackup(store, true, backupTaskId, lastEtag, timeout);

                var directoriesCount = await getDirectoriesCount(store.Database);
                var expectedNumberOfDirectories = 1;
                Assert.Equal(expectedNumberOfDirectories, directoriesCount);
            }
        }

        private async Task<long> CreateBackup(DocumentStore store, bool isFullBackup, long backupTaskId, long lastEtag, int timeout)
        {
            await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup, backupTaskId));
            var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
            var value = WaitForValue(() =>
            {
                var status = store.Maintenance.Send(operation).Status;
                if (status == null)
                    return false;

                if (status.LastEtag <= lastEtag)
                    return false;

                lastEtag = status.LastEtag.Value;
                return true;
            }, true, timeout: timeout);
            Assert.True(value);

            return lastEtag;
        }

        private static S3Settings GetS3Settings(string databaseName, [CallerMemberName] string remoteFolderName = null)
        {
            return new S3Settings
            {
                AwsAccessKey = null,
                AwsSecretKey = null,
                AwsRegionName = null,
                BucketName = "ravendb-test",
                RemoteFolderName = $"{remoteFolderName}/{databaseName}"
            };
        }
    }
}
