using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Smuggler;
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
        public async Task CanBackupToDirectory_MultipleBackups_with_long_interval()
        {
            using (var store = GetDocumentStore())
            {
                var periodicBackupRunner = (await GetDocumentDatabaseInstanceFor(store)).BundleLoader.PeriodicBackupRunner;

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
        public async Task PeriodicBackup_should_work_with_long_intervals()
        {
            using (var store = GetDocumentStore())
            {
                var periodicBackupRunner = (await GetDocumentDatabaseInstanceFor(store)).BundleLoader.PeriodicBackupRunner;

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
        public async Task CanBackupToDirectory_MultipleExports()
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

    }
}