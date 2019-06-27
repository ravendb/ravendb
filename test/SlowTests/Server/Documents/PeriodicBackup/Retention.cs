using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
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

            using (var store = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    RetentionPolicy = new RetentionPolicy
                    {
                        MinimumBackupsToKeep = minimumBackupsToKeep
                    },
                    IncrementalBackupFrequency = "30 3 L * ?"
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;

                var lastEtag = 0L;
                for (var i = 0; i < minimumBackupsToKeep + 3; i++)
                {
                    string userId;

                    using (var session = store.OpenAsyncSession())
                    {
                        var user = new User { Name = "Grisha"};
                        await session.StoreAsync(user);
                        userId = user.Id;
                        await session.SaveChangesAsync();
                    }

                    var directoryToBeDeleted = GetDirectoryToBeDeleted();

                    // create full backup
                    lastEtag = await CreateBackup(store, true, backupTaskId, lastEtag);

                    AssertDirectoriesCount();
                    AssertDirectoryDeleted(directoryToBeDeleted);

                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>(userId);
                        user.Age = 33;
                        await session.SaveChangesAsync();
                    }

                    // create incremental backup
                    lastEtag = await CreateBackup(store, false, backupTaskId, lastEtag);

                    AssertDirectoriesCount();
                }

                AssertDirectoriesCount();

                void AssertDirectoriesCount()
                {
                    var directories = Directory.GetDirectories(backupPath);
                    if (directories.Length <= minimumBackupsToKeep)
                        return;

                    Assert.Equal(minimumBackupsToKeep, directories.Length);
                }

                string GetDirectoryToBeDeleted()
                {
                    var directories = Directory.GetDirectories(backupPath);
                    if (directories.Length == 1 ||
                        directories.Length < minimumBackupsToKeep)
                        return null;

                    return directories.OrderBy(Directory.GetLastWriteTime).First();
                }

                void AssertDirectoryDeleted(string directoryToBeDeleted)
                {
                    if (directoryToBeDeleted == null)
                        return;

                    var directories = Directory.GetDirectories(backupPath);
                    Assert.True(directories.Any(x => x.Equals(directoryToBeDeleted) == false));
                }
            }
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
            var minimumBackupAgeToKeep = TimeSpan.FromSeconds(seconds);
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    RetentionPolicy = new RetentionPolicy
                    {
                        MinimumBackupsToKeep = minimumBackupsToKeep,
                        MinimumBackupAgeToKeep = minimumBackupAgeToKeep
                    },
                    IncrementalBackupFrequency = "30 3 L * ?"
                };

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
                    lastEtag = await CreateBackup(store, true, backupTaskId, lastEtag);

                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>(userId);
                        user.Age = 33;
                        await session.SaveChangesAsync();
                    }

                    // create incremental backup
                    lastEtag = await CreateBackup(store, false, backupTaskId, lastEtag);
                }

                await Task.Delay(minimumBackupAgeToKeep + TimeSpan.FromSeconds(5));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Grisha" });
                    await session.SaveChangesAsync();
                }
                await CreateBackup(store, true, backupTaskId, lastEtag);

                var directories = Directory.GetDirectories(backupPath);
                var expectedNumberOfDirectories = minimumBackupsToKeep ?? 1;
                Assert.Equal(expectedNumberOfDirectories, directories.Length);
            }
        }

        private async Task<long> CreateBackup(DocumentStore store, bool isFullBackup, long backupTaskId, long lastEtag)
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
            }, true);
            Assert.True(value);

            return lastEtag;
        }
    }
}
