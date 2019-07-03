using System;
using System.Collections.Generic;
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
        [InlineData(10)]
        [InlineData(2)]
        [InlineData(1)]
        [InlineData(13)]
        [InlineData(5)]
        [InlineData(20)]
        public async Task can_delete_backups_in_correct_order(int minimumBackupsToKeep)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder", forceCreateDir: true);

            await CanDeleteBackupsInCorrectOrder(minimumBackupsToKeep,
                (configuration, _) =>
                {
                    configuration.LocalSettings = new LocalSettings {FolderPath = backupPath};
                },
                _ =>
                {
                    var directories = Directory.GetDirectories(backupPath)
                        .Where(x => Directory.GetFiles(x).Any(BackupUtils.IsFullBackupOrSnapshot))
                        .Select(x => new DirectoryDetails
                        {
                            Path = x,
                            LastWriteTime = Directory.GetLastWriteTime(x)
                        })
                        .ToList();

                    return Task.FromResult(directories);
                }, timeout: 15000);
        }

        [Theory]
        [InlineData(15, null)]
        [InlineData(25, null)]
        [InlineData(30, null)]
        [InlineData(40, null)]
        [InlineData(15, 10)]
        [InlineData(25, 2)]
        [InlineData(30, 13)]
        [InlineData(40, 5)]
        public async Task can_delete_backups_by_date(int seconds, int? minimumBackupsToKeep)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            await CanDeleteBackupsByDate(seconds, minimumBackupsToKeep,
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
        [InlineData(10)]
        [InlineData(2)]
        [InlineData(1)]
        [InlineData(13)]
        [InlineData(5)]
        [InlineData(20)]
        public async Task can_delete_backups_in_correct_order_s3(int minimumBackupsToKeep)
        {
            await CanDeleteBackupsInCorrectOrder(minimumBackupsToKeep, (configuration, databaseName) =>
                {
                    configuration.S3Settings = GetS3Settings(databaseName);
                },
                async databaseName =>
                {
                    using (var client = new RavenAwsS3Client(GetS3Settings(databaseName)))
                    {
                        var folders = await client.ListObjects($"{client.RemoteFolderName}/", "/", listFolders: true);
                        return folders
                            .Select(x => new DirectoryDetails {Path = x.FullPath, LastWriteTime = x.LastModified})
                            .ToList();
                    }
                }, timeout: 60000);
        }

        [Theory]
        //[Theory(Skip = "Requires Amazon AWS Credentials")]
        [InlineData(15, null)]
        [InlineData(25, null)]
        [InlineData(30, null)]
        [InlineData(40, null)]
        [InlineData(15, 10)]
        [InlineData(25, 2)]
        [InlineData(30, 13)]
        [InlineData(40, 5)]
        public async Task can_delete_backups_by_date_s3(int seconds, int? minimumBackupsToKeep)
        {
            await CanDeleteBackupsByDate(seconds, minimumBackupsToKeep,
                (configuration, databaseName) =>
                {
                    configuration.S3Settings = GetS3Settings(databaseName);
                },
                async databaseName =>
                {
                    using (var client = new RavenAwsS3Client(GetS3Settings(databaseName)))
                    {
                        var folders = await client.ListObjects($"{client.RemoteFolderName}/", "/", listFolders: true);
                        return folders.Count();
                    }
                }, timeout: 60000);
        }

        private async Task CanDeleteBackupsInCorrectOrder(int minimumBackupsToKeep, 
            Action<PeriodicBackupConfiguration, string> modifyConfiguration, Func<string, Task<List<DirectoryDetails>>> getDirectories, int timeout)
        {
            using (var store = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration
                {
                    IncrementalBackupFrequency = "30 3 L * ?",
                    RetentionPolicy = new RetentionPolicy
                    {
                        MinimumBackupsToKeep = minimumBackupsToKeep
                    },
                };

                modifyConfiguration(config, store.Database);

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;

                var lastEtag = 0L;
                for (var i = 0; i < minimumBackupsToKeep + 3; i++)
                {
                    string userId;

                    using (var session = store.OpenAsyncSession())
                    {
                        var user = new User { Name = "Grisha" };
                        await session.StoreAsync(user);
                        userId = user.Id;
                        await session.SaveChangesAsync();
                    }

                    var directoryToBeDeleted = await GetDirectoryToBeDeleted();

                    // create full backup
                    lastEtag = await CreateBackup(store, true, backupTaskId, lastEtag, timeout);

                    await AssertDirectoriesCount();
                    await AssertDirectoryDeleted(directoryToBeDeleted);

                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>(userId);
                        user.Age = 33;
                        await session.SaveChangesAsync();
                    }

                    // create incremental backup
                    lastEtag = await CreateBackup(store, false, backupTaskId, lastEtag, timeout);

                    await AssertDirectoriesCount();
                }

                await AssertDirectoriesCount();

                async Task AssertDirectoriesCount()
                {
                    var directories = await getDirectories(store.Database);
                    if (directories.Count <= minimumBackupsToKeep)
                        return;

                    Assert.Equal(minimumBackupsToKeep, directories.Count);
                }

                async Task<string> GetDirectoryToBeDeleted()
                {
                    var directories = await getDirectories(store.Database);
                    if (directories.Count == 1 ||
                        directories.Count < minimumBackupsToKeep)
                        return null;

                    return directories.OrderBy(x => x.Path).ThenBy(x => x.LastWriteTime).Select(x => x.Path).First();
                }

                async Task AssertDirectoryDeleted(string directoryToBeDeleted)
                {
                    if (directoryToBeDeleted == null)
                        return;

                    var directories = (await getDirectories(store.Database)).Select(x => x.Path);
                    Assert.True(directories.All(x => x.Equals(directoryToBeDeleted) == false));
                }
            }
        }

        private async Task CanDeleteBackupsByDate(int seconds, int? minimumBackupsToKeep, 
            Action<PeriodicBackupConfiguration, string> modifyConfiguration, Func<string, Task<int>> getDirectoriesCount, int timeout)
        {
            var minimumBackupAgeToKeep = TimeSpan.FromSeconds(seconds);

            using (var store = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration
                {
                    IncrementalBackupFrequency = "30 3 L * ?",
                    RetentionPolicy = new RetentionPolicy
                    {
                        MinimumBackupsToKeep = minimumBackupsToKeep,
                        MinimumBackupAgeToKeep = minimumBackupAgeToKeep
                    }
                };

                modifyConfiguration(config, store.Database);

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;

                var lastEtag = 0L;
                var runs = minimumBackupsToKeep + 10 ?? 10;
                for (var i = 0; i < runs; i++)
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
                var expectedNumberOfDirectories = minimumBackupsToKeep ?? 1;
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

        private class DirectoryDetails
        {
            public string Path { get; set; }

            public DateTime LastWriteTime { get; set; }
        }
    }
}
