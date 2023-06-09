using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;
using static Raven.Server.Utils.BackupUtils;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class PeriodicBackupTestsSlow : ClusterTestBase
    {
        private readonly ITestOutputHelper _output;

        public PeriodicBackupTestsSlow(ITestOutputHelper output) : base(output)
        {
            _output = output;
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_to_directory()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "* * * * *");
                var operation = new UpdatePeriodicBackupOperation(config);
                var result = await store.Maintenance.SendAsync(operation);
                var periodicBackupTaskId = result.TaskId;

                var getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(periodicBackupTaskId);
                var done = SpinWait.SpinUntil(() => store.Maintenance.Send(getPeriodicBackupStatus).Status?.LastFullBackup != null, TimeSpan.FromSeconds(180));
                Assert.True(done, "Failed to complete the backup in time");
            }

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_2"
            }))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(),
                    Directory.GetDirectories(backupPath).First());

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal("oren", user.Name);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_to_directory_multiple_backups_with_long_interval()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore())
            {
                var periodicBackupRunner = (await Databases.GetDocumentDatabaseInstanceFor(store)).PeriodicBackupRunner;

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

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);
            }

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_2"
            }))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(),
                    Directory.GetDirectories(backupPath).First());
                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                    Assert.True(users.Any(x => x.Value.Name == "oren"));
                    Assert.True(users.Any(x => x.Value.Name == "ayende"));
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task periodic_backup_should_work_with_long_intervals()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var periodicBackupRunner = (await Databases.GetDocumentDatabaseInstanceFor(store)).PeriodicBackupRunner;

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

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren 2" }, "users/2");
                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);
            }

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_2"
            }))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(),
                    Directory.GetDirectories(backupPath).First());
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
        public async Task can_backup_to_directory_multiple_backups()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);
            }

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_2"
            }))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(),
                    Directory.GetDirectories(backupPath).First());
                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                    Assert.True(users.Any(x => x.Value.Name == "oren"));
                    Assert.True(users.Any(x => x.Value.Name == "ayende"));
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanImportTombstonesFromIncrementalBackup()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "fitzchak" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete("users/1");
                    await session.SaveChangesAsync();
                }

                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: 2);
            }

            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_2"
            }))
            {
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(),
                    Directory.GetDirectories(backupPath).First());
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Null(user);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_restore_smuggler_correctly()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);
            }

            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_2"
            }))
            using (var store2 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_2"
            }))
            {
                var backupDirectory = Directory.GetDirectories(backupPath).First();

                var backupToMovePath = $"{backupPath}{Path.DirectorySeparatorChar}IncrementalBackupTemp";
                Directory.CreateDirectory(backupToMovePath);
                var incrementalBackupFile = Directory.GetFiles(backupDirectory).OrderBackups().Last();
                var fileName = Path.GetFileName(incrementalBackupFile);
                File.Move(incrementalBackupFile, $"{backupToMovePath}{Path.DirectorySeparatorChar}{fileName}");

                await store1.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), backupDirectory);
                using (var session = store1.OpenAsyncSession())
                {
                    var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                    var keyValuePair = users.First();
                    Assert.NotNull(keyValuePair.Value);
                    Assert.True(keyValuePair.Value.Name == "oren" && keyValuePair.Key == "users/1");
                    Assert.Null(users.Last().Value);
                }

                await store2.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), backupToMovePath);
                using (var session = store2.OpenAsyncSession())
                {
                    var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                    Assert.Null(users.First().Value);
                    var keyValuePair = users.Last();
                    Assert.NotNull(keyValuePair.Value);
                    Assert.True(keyValuePair.Value.Name == "ayende" && keyValuePair.Key == "users/2");
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore()
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

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);

                var backupStatus = store.Maintenance.Send(operation);
                var backupOperationId = backupStatus.Status.LastOperationId;

                var backupOperation = store.Maintenance.Send(new GetOperationStateOperation(backupOperationId.Value));
                Assert.Equal(OperationStatus.Completed, backupOperation.Status);

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
                await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                var backupLocation = Directory.GetDirectories(backupPath).First();

                using (ReadOnly(backupLocation))
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = backupLocation,
                    DatabaseName = databaseName
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

        [Theory, Trait("Category", "Smuggler")]
        [InlineData("* * * * *", null)]
        [InlineData(null, "* * * * *")]
        [InlineData("0 0 1 * *", null)]
        [InlineData(null, "0 0 1 * *")]
        public async Task next_full_backup_time_calculated_correctly(string fullBackupFrequency, string incrementalBackupFrequency)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: fullBackupFrequency, incrementalBackupFrequency: incrementalBackupFrequency);

                var backup = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                var documentDatabase = (await Databases.GetDocumentDatabaseInstanceFor(store));
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var now = DateTime.UtcNow;
                var nextBackupDetails = documentDatabase.PeriodicBackupRunner.GetNextBackupDetails(record, record.PeriodicBackups.First(), new PeriodicBackupStatus
                {
                    LastFullBackupInternal = now.AddDays(-360)
                }, Server.ServerStore.NodeTag);

                Assert.Equal(backup.TaskId, nextBackupDetails.TaskId);
                Assert.Equal(TimeSpan.Zero, nextBackupDetails.TimeSpan);
                Assert.Equal(true, nextBackupDetails.IsFull);
                Assert.True(nextBackupDetails.DateTime >= now);
            }
        }

        [Theory, Trait("Category", "Smuggler")]
        [InlineData(CompressionLevel.Optimal)]
        [InlineData(CompressionLevel.Fastest)]
        [InlineData(CompressionLevel.NoCompression)]
        public async Task can_backup_and_restore_snapshot(CompressionLevel compressionLevel)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
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

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                config.SnapshotSettings = new SnapshotSettings { CompressionLevel = compressionLevel };
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.StoreAsync(new User { Name = "ayende" }, "users/3");

                    session.CountersFor("users/2").Increment("downloads", 200);

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);

                // restore the database with a different name
                string restoredDatabaseName = $"restored_database_snapshot-{Guid.NewGuid()}";
                var backupLocation = Directory.GetDirectories(backupPath).First();
                using (ReadOnly(backupLocation))
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = backupLocation,
                    DatabaseName = restoredDatabaseName
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
                        Assert.Contains($"A:8-{originalDatabase.DbBase64Id}", databaseChangeVector);
                        Assert.Contains($"A:11-{restoredDatabase.DbBase64Id}", databaseChangeVector);
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanBackupAndRestoreSnapshotExcludingIndexes()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            string restoredDatabaseName = $"restored_database_snapshot-{Guid.NewGuid()}";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Lev1" }, "users/1");
                    await session.StoreAsync(new User { Name = "Lev2" }, "users/2");
                    await session.StoreAsync(new User { Name = "Lev3" }, "users/3");
                    await session.SaveChangesAsync();
                }

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(0, stats.CountOfIndexes);

                using (var session = store.OpenAsyncSession())
                {
                    await session
                        .Query<User>()
                        .Where(x => x.Name == "Lev")
                        .ToListAsync(); // create an index to backup

                    await session
                        .Query<Order>()
                        .Where(x => x.Freight > 5)
                        .ToListAsync(); // create an index to backup

                    await session.SaveChangesAsync();
                }

                stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfIndexes);

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                config.SnapshotSettings = new SnapshotSettings { CompressionLevel = CompressionLevel.Fastest, ExcludeIndexes = false };
                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                // check that backup file consist Indexes folder
                var backupLocation = Directory.GetDirectories(backupPath).First();
                using (ReadOnly(backupLocation))
                {
                    var backupFile = Directory.GetFiles(backupLocation).First();
                    using (ZipArchive archive = ZipFile.OpenRead(backupFile))
                        Assert.True(archive.Entries.Any(entry => entry.FullName.Contains("Indexes")));
                }

                Directory.Delete(backupLocation, true);
                Assert.False(Directory.Exists(backupLocation));

                config.SnapshotSettings.ExcludeIndexes = true;
                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                backupLocation = Directory.GetDirectories(backupPath).First();
                using (ReadOnly(backupLocation))
                {
                    var backupFile = Directory.GetFiles(backupLocation).First();

                    using (ZipArchive archive = ZipFile.OpenRead(backupFile))
                        Assert.False(archive.Entries.Any(entry => entry.FullName.Contains("Indexes")));

                    using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupLocation, DatabaseName = restoredDatabaseName }))
                    using (var session = store.OpenAsyncSession(restoredDatabaseName))
                    {
                        var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                        Assert.NotNull(users["users/1"]);
                        Assert.NotNull(users["users/2"]);
                        Assert.True(users.Any(x => x.Value.Name == "Lev1"));
                        Assert.True(users.Any(x => x.Value.Name == "Lev2"));
                    }
                }
            }
        }


        [Theory, Trait("Category", "Smuggler")]
        [InlineData(BackupType.Snapshot)]
        [InlineData(BackupType.Backup)]
        public async Task can_backup_and_restore_snapshot_with_compare_exchange(BackupType backupType)
        {
            var ids = Enumerable.Range(0, 2 * 1024) // DatabaseDestination.DatabaseCompareExchangeActions.BatchSize
                .Select(i => "users/" + i).ToArray();

            var backupPath = NewDataPath(suffix: "BackupFolder");
            using var store = GetDocumentStore();
            await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                foreach (var id in ids)
                {
                    await session.StoreAsync(new User(), id);
                }
                await session.SaveChangesAsync();
            }

            var sourceStats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
            Assert.Equal(ids.Length, sourceStats.CountOfCompareExchange);

            var config = Backup.CreateBackupConfiguration(backupPath, backupType: backupType);
            config.SnapshotSettings = new SnapshotSettings { CompressionLevel = CompressionLevel.NoCompression };
            var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

            var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDatabaseEtag;
            await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);

            // restore the database with a different name
            string restoredDatabaseName = GetDatabaseName();
            var backupLocation = Directory.GetDirectories(backupPath).First();

            using (ReadOnly(backupLocation))
            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
            {
                BackupLocation = backupLocation,
                DatabaseName = restoredDatabaseName
            }))
            {
                using var destination = new DocumentStore { Urls = store.Urls, Database = restoredDatabaseName }.Initialize();

                using (var session = destination.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var users = await session.LoadAsync<User>(ids);
                    Assert.All(users.Values, Assert.NotNull);
                }

                var restoreStats = await destination.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
                Assert.Equal(sourceStats.CountOfCompareExchange, restoreStats.CountOfCompareExchange);

                using (var session = destination.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var user = await session.LoadAsync<User>(ids[0]);

                    await session.StoreAsync(user);

                    await session.SaveChangesAsync();
                }

                using (var session = destination.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    await session.StoreAsync(new User(), ids[0]);
                    await Assert.ThrowsAnyAsync<ConcurrencyException>(async () => await session.SaveChangesAsync());
                }
            }
        }

        class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
        }

        [Fact]
        public async Task ClusterWideTransaction_WhenImportWithoutCompareExchange_ShouldNotFailOnAfterImportModification()
        {
            const string id = "test/1";

            var file = GetTempFileName();
            var (nodes, leader) = await CreateRaftCluster(3);
            using var source = GetDocumentStore(new Options { Server = leader, ReplicationFactor = nodes.Count });
            using var destination = GetDocumentStore(new Options { Server = leader, ReplicationFactor = nodes.Count });
            using (var session = source.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = new TestObj();
                await session.StoreAsync(entity, id);
                await session.SaveChangesAsync();
            }
            var result = await source.Operations.SendAsync(new GetCompareExchangeValuesOperation<TestObj>(""));
            Assert.Single(result);
            Assert.EndsWith(id, result.Single().Key, StringComparison.OrdinalIgnoreCase);

            //Export without `CompareExchange`s
            var exportOperation = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions { OperateOnTypes = DatabaseItemType.Documents }, file);
            await exportOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            var importOperation = await destination.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
            await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            using (var session = destination.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var loadedEntity = await session.LoadAsync<TestObj>(id);
                loadedEntity.Prop = "Toli";
                await session.StoreAsync(loadedEntity, id);
                await session.SaveChangesAsync();
            }
            using (var session = destination.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var loadedEntity = await session.LoadAsync<TestObj>(id);
                Assert.Equal("Toli", loadedEntity.Prop);
            }

        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore_snapshot_with_compression()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore(new Options
            {
                RunInMemory = false,
                ModifyDatabaseRecord = record =>
                {
                    record.DocumentsCompression = new DocumentsCompressionConfiguration
                    {
                        Collections = new[] { "Orders" },
                        CompressRevisions = true
                    };
                }
            }))
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                var sourceStats = await store.Maintenance.SendAsync(new GetStatisticsOperation());

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                var databasePath = database.Configuration.Core.DataDirectory.FullPath;
                var compressionRecovery = Directory.GetFiles(databasePath, TableValueCompressor.CompressionRecoveryExtensionGlob);
                Assert.Equal(2, compressionRecovery.Length);

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                config.SnapshotSettings = new SnapshotSettings { CompressionLevel = CompressionLevel.NoCompression };
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);

                // restore the database with a different name
                string restoredDatabaseName = $"restored_database_snapshot-{Guid.NewGuid()}";
                var backupLocation = Directory.GetDirectories(backupPath).First();
                using (ReadOnly(backupLocation))
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = backupLocation,
                    DatabaseName = restoredDatabaseName
                }))
                {
                    // exception was throw during restore that compression recovery files were already existing

                    var restoreStats = await store.Maintenance.SendAsync(new GetStatisticsOperation());

                    Assert.Equal(sourceStats.CountOfDocuments, restoreStats.CountOfDocuments);

                    database = await Databases.GetDocumentDatabaseInstanceFor(store, restoredDatabaseName);
                    databasePath = database.Configuration.Core.DataDirectory.FullPath;
                    compressionRecovery = Directory.GetFiles(databasePath, TableValueCompressor.CompressionRecoveryExtensionGlob);
                    Assert.Equal(2, compressionRecovery.Length);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore_compression_config()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                record.DocumentsCompression = new DocumentsCompressionConfiguration(true, "Users");
                store.Maintenance.Server.Send(new UpdateDatabaseOperation(record, record.Etag));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "oren"
                    }, "users/1");
                    await session.StoreAsync(new User
                    {
                        Name = "ayende"
                    }, "users/2");

                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DatabaseName = databaseName
                }))
                {
                    var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    var compression = databaseRecord.DocumentsCompression;
                    Assert.NotNull(compression);
                    Assert.Contains("Users", compression.Collections);

                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                        Assert.True(users.Any(x => x.Value.Name == "oren"));
                        Assert.True(users.Any(x => x.Value.Name == "ayende"));
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore_with_timeseries()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                await store.TimeSeries.SetRawPolicyAsync("users", TimeValue.FromYears(1));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "oren"
                    }, "users/1");

                    for (int i = 0; i < 360; i++)
                    {
                        session.TimeSeriesFor("users/1", "Heartrate")
                            .Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/1");
                    }

                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);

                var backupStatus = store.Maintenance.Send(operation);
                var backupOperationId = backupStatus.Status.LastOperationId;

                var backupOperation = store.Maintenance.Send(new GetOperationStateOperation(backupOperationId.Value));
                Assert.Equal(OperationStatus.Completed, backupOperation.Status);

                var backupResult = backupOperation.Result as BackupResult;
                Assert.True(backupResult.TimeSeries.Processed);
                Assert.Equal(360, backupResult.TimeSeries.ReadCount);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "ayende"
                    }, "users/2");

                    for (int i = 0; i < 180; i++)
                    {
                        session.TimeSeriesFor("users/2", "Heartrate")
                            .Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/2");
                    }

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DatabaseName = databaseName
                }))
                {
                    var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    var tsConfig = databaseRecord.TimeSeries;
                    Assert.NotNull(tsConfig);
                    Assert.Equal(TimeValue.FromYears(1), tsConfig.Collections["Users"].RawPolicy.RetentionTime);

                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                        Assert.True(users.Any(x => x.Value.Name == "oren"));
                        Assert.True(users.Any(x => x.Value.Name == "ayende"));

                        var values = (await session.TimeSeriesFor("users/1", "Heartrate").GetAsync(DateTime.MinValue, DateTime.MaxValue)).ToList();
                        Assert.Equal(360, values.Count);

                        for (int i = 0; i < values.Count; i++)
                        {
                            Assert.Equal(baseline.AddSeconds(i * 10), values[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(i % 60, values[i].Values[0]);
                            Assert.Equal("watches/1", values[i].Tag);
                        }

                        values = (await session.TimeSeriesFor("users/2", "Heartrate").GetAsync(DateTime.MinValue, DateTime.MaxValue)).ToList();
                        Assert.Equal(180, values.Count);

                        for (int i = 0; i < values.Count; i++)
                        {
                            Assert.Equal(baseline.AddSeconds(i * 10), values[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(i % 60, values[i].Values[0]);
                            Assert.Equal("watches/2", values[i].Tag);
                        }
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore_snapshot_with_timeseries()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    //create time series segment to backup
                    for (int i = 0; i < 360; i++)
                    {
                        session.TimeSeriesFor("users/1", "Heartrate").Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/1");
                    }

                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    for (int i = 0; i < 360; i++)
                    {
                        session.TimeSeriesFor("users/2", "Heartrate").Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/2");
                    }

                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);

                // restore the database with a different name
                string restoredDatabaseName = $"restored_database_snapshot-{Guid.NewGuid()}";
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DatabaseName = restoredDatabaseName
                }))
                {
                    using (var session = store.OpenAsyncSession(restoredDatabaseName))
                    {
                        var users = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                        Assert.NotNull(users["users/1"]);
                        Assert.NotNull(users["users/2"]);
                        Assert.True(users.Any(x => x.Value.Name == "oren"));
                        Assert.True(users.Any(x => x.Value.Name == "ayende"));

                        var values = (await session.TimeSeriesFor("users/1", "Heartrate").GetAsync(DateTime.MinValue, DateTime.MaxValue)).ToList();
                        Assert.Equal(360, values.Count);

                        for (int i = 0; i < values.Count; i++)
                        {
                            Assert.Equal(baseline.AddSeconds(i * 10), values[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(i % 60, values[i].Values[0]);
                            Assert.Equal("watches/1", values[i].Tag);
                        }

                        values = (await session.TimeSeriesFor("users/2", "Heartrate").GetAsync(DateTime.MinValue, DateTime.MaxValue)).ToList();
                        Assert.Equal(360, values.Count);

                        for (int i = 0; i < values.Count; i++)
                        {
                            Assert.Equal(baseline.AddSeconds(i * 10), values[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(i % 60, values[i].Values[0]);
                            Assert.Equal("watches/2", values[i].Tag);
                        }
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task RestoreSnapshotWithTimeSeriesCollectionConfiguration_WhenConfigurationInFirstSnapshot()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var timeSeriesConfiguration = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromHours(96)),
                            Policies = new List<TimeSeriesPolicy> { new TimeSeriesPolicy("BySecond", TimeValue.FromSeconds(1)) }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(timeSeriesConfiguration));

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);

                await RestoreAndCheckTimeSeriesConfiguration(store, backupPath, timeSeriesConfiguration);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task RestoreSnapshotWithTimeSeriesCollectionConfiguration_WhenConfigurationInIncrementalSnapshot()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var entity = new User();
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(entity);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);
                var statusLastEtag = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(backupTaskId))).Status.LastEtag;

                var timeSeriesConfiguration = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromHours(96)),
                            Policies = new List<TimeSeriesPolicy> { new TimeSeriesPolicy("BySecond", TimeValue.FromSeconds(1)) }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(timeSeriesConfiguration));
                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Patch<User, string>(entity.Id, u => u.Name, "Patched");
                    await session.SaveChangesAsync();
                }
                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false);
                Assert.True(status.LastEtag > statusLastEtag, "status.LastEtag > statusLastEtag");

                await RestoreAndCheckTimeSeriesConfiguration(store, backupPath, timeSeriesConfiguration);
            }
        }

        private async Task RestoreAndCheckTimeSeriesConfiguration(IDocumentStore store, string backupPath, TimeSeriesConfiguration timeSeriesConfiguration)
        {
            string restoredDatabaseName = $"{store.Database}-restored";
            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = Directory.GetDirectories(backupPath).First(), DatabaseName = restoredDatabaseName }))
            {
                var db = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(restoredDatabaseName));
                var actual = db.TimeSeries;

                Assert.NotNull(actual);
                Assert.Equal(timeSeriesConfiguration.Collections.Count, actual.Collections.Count);
                Assert.Equal(timeSeriesConfiguration.PolicyCheckFrequency, actual.PolicyCheckFrequency);
                foreach (var (key, expectedCollection) in timeSeriesConfiguration.Collections)
                {
                    Assert.True(actual.Collections.TryGetValue(key, out var actualCollection));
                    Assert.Equal(expectedCollection.Policies, actualCollection.Policies);
                    Assert.Equal(expectedCollection.RawPolicy, actualCollection.RawPolicy);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task restore_settings_tests()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"{s}_2"
            }))
            {
                var restoreConfiguration = new RestoreBackupConfiguration();

                var restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                var e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("Name cannot be null or empty.", e.InnerException.Message);

                restoreConfiguration.DatabaseName = "abc*^&.";
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("The name 'abc*^&.' is not permitted. Only letters, digits and characters ('_', '-', '.') are allowed.", e.InnerException.Message);

                restoreConfiguration.DatabaseName = store.Database;
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("Cannot restore data to an existing database", e.InnerException.Message);

                restoreConfiguration.DatabaseName = "test-" + Guid.NewGuid();
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("Backup location can't be null or empty", e.InnerException.Message);

                restoreConfiguration.BackupLocation = "this-path-doesn't-exist\\";
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("Backup location doesn't exist", e.InnerException.Message);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);

                restoreConfiguration.BackupLocation = backupPath;
                restoreConfiguration.DataDirectory = backupPath;
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("New data directory must be empty of any files or folders", e.InnerException.Message);

                // perform restore with a valid db name
                var emptyFolder = NewDataPath(suffix: "BackupFolderRestore123");
                var validDbName = "日本語-שלום-cześć_Привет.123";

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DataDirectory = emptyFolder,
                    DatabaseName = validDbName
                }))
                {
                    using (var session = store.OpenAsyncSession(validDbName))
                    {
                        var user = await session.LoadAsync<User>("users/1");
                        Assert.NotNull(user);
                    }
                };
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task periodic_backup_should_export_starting_from_last_etag()
        {
            //https://issues.hibernatingrhinos.com/issue/RavenDB-11395

            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.StoreAsync(new User { Name = "aviv" }, "users/2");

                    session.CountersFor("users/1").Increment("likes", 100);
                    session.CountersFor("users/2").Increment("downloads", 200);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);

                var exportPath = GetBackupPath(store, backupTaskId, incremental: false);

                using (var store2 = GetDocumentStore())
                {
                    var op = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportPath);
                    await op.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(2, stats.CountOfDocuments);
                    Assert.Equal(2, stats.CountOfCounterEntries);

                    using (var session = store2.OpenAsyncSession())
                    {
                        var user1 = await session.LoadAsync<User>("users/1");
                        var user2 = await session.LoadAsync<User>("users/2");

                        Assert.Equal("oren", user1.Name);
                        Assert.Equal("aviv", user2.Name);

                        var dic = await session.CountersFor(user1).GetAllAsync();
                        Assert.Equal(1, dic.Count);
                        Assert.Equal(100, dic["likes"]);

                        dic = await session.CountersFor(user2).GetAllAsync();
                        Assert.Equal(1, dic.Count);
                        Assert.Equal(200, dic["downloads"]);
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/3");
                    session.CountersFor("users/3").Increment("votes", 300);
                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);

                exportPath = GetBackupPath(store, backupTaskId);

                using (var store3 = GetDocumentStore())
                {
                    // importing to a new database, in order to verify that
                    // periodic backup imports only the changed documents (and counters)

                    var op = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportPath);
                    await op.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store3.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);

                    Assert.Equal(1, stats.CountOfCounterEntries);

                    using (var session = store3.OpenAsyncSession())
                    {
                        var user3 = await session.LoadAsync<User>("users/3");

                        Assert.Equal("ayende", user3.Name);

                        var dic = await session.CountersFor(user3).GetAllAsync();
                        Assert.Equal(1, dic.Count);
                        Assert.Equal(300, dic["votes"]);
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task periodic_backup_with_timeseries_should_export_starting_from_last_etag()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    //create time series segment to backup
                    for (int i = 0; i < 360; i++)
                    {
                        session.TimeSeriesFor("users/1", "Heartrate").Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/1");
                    }

                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);

                var exportPath = GetBackupPath(store, backupTaskId, incremental: false);

                using (var store2 = GetDocumentStore())
                {
                    var op = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportPath);
                    await op.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfTimeSeriesSegments);

                    using (var session = store2.OpenAsyncSession())
                    {
                        var user1 = await session.LoadAsync<User>("users/1");
                        Assert.Equal("oren", user1.Name);

                        var values = (await session.TimeSeriesFor("users/1", "Heartrate")
                            .GetAsync(DateTime.MinValue, DateTime.MaxValue))
                            .ToList();

                        Assert.Equal(360, values.Count);

                        for (int i = 0; i < values.Count; i++)
                        {
                            Assert.Equal(baseline.AddSeconds(i * 10), values[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(i % 60, values[i].Values[0]);
                            Assert.Equal("watches/1", values[i].Tag);
                        }
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    for (int i = 0; i < 180; i++)
                    {
                        session.TimeSeriesFor("users/2", "Heartrate")
                            .Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/1");
                    }
                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);

                exportPath = GetBackupPath(store, backupTaskId);

                using (var store3 = GetDocumentStore())
                {
                    // importing to a new database, in order to verify that
                    // periodic backup imports only the changed documents (and timeseries)

                    var op = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportPath);
                    await op.WaitForCompletionAsync(TimeSpan.FromMinutes(15));

                    var stats = await store3.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);

                    Assert.Equal(1, stats.CountOfTimeSeriesSegments);

                    using (var session = store3.OpenAsyncSession())
                    {
                        var user2 = await session.LoadAsync<User>("users/2");

                        Assert.Equal("ayende", user2.Name);

                        var values = (await session.TimeSeriesFor(user2, "Heartrate")
                                .GetAsync(DateTime.MinValue, DateTime.MaxValue))
                            .ToList();

                        Assert.Equal(180, values.Count);

                        for (int i = 0; i < values.Count; i++)
                        {
                            Assert.Equal(baseline.AddSeconds(i * 10), values[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(i % 60, values[i].Values[0]);
                            Assert.Equal("watches/1", values[i].Tag);
                        }
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task periodic_backup_with_incremental_timeseries_should_export_starting_from_last_etag()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 360; i++)
                    {
                        session.IncrementalTimeSeriesFor("users/1", "INC:Heartrate").Increment(baseline.AddSeconds(i * 10), new[] { i % 60d });
                    }

                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                var exportPath = GetBackupPath(store, backupTaskId, incremental: false);

                using (var store2 = GetDocumentStore())
                {
                    var op = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportPath);
                    await op.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var stats = await store2.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfTimeSeriesSegments);

                    using (var session = store2.OpenAsyncSession())
                    {
                        var user1 = await session.LoadAsync<User>("users/1");
                        Assert.Equal("oren", user1.Name);

                        var values = (await session.IncrementalTimeSeriesFor("users/1", "INC:Heartrate")
                            .GetAsync())
                            .ToList();

                        Assert.Equal(360, values.Count);

                        for (int i = 0; i < values.Count; i++)
                        {
                            Assert.Equal(baseline.AddSeconds(i * 10), values[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(i % 60, values[i].Values[0]);
                        }
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    for (int i = 0; i < 180; i++)
                    {
                        session.IncrementalTimeSeriesFor("users/2", "INC:Heartrate")
                            .Increment(baseline.AddSeconds(i * 10), new[] { i % 60d });
                    }
                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);

                exportPath = GetBackupPath(store, backupTaskId);

                using (var store3 = GetDocumentStore())
                {
                    // importing to a new database, in order to verify that
                    // periodic backup imports only the changed documents (and timeseries)

                    var op = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportPath);
                    await op.WaitForCompletionAsync(TimeSpan.FromMinutes(15));

                    var stats = await store3.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);

                    Assert.Equal(1, stats.CountOfTimeSeriesSegments);

                    using (var session = store3.OpenAsyncSession())
                    {
                        var user2 = await session.LoadAsync<User>("users/2");

                        Assert.Equal("ayende", user2.Name);

                        var values = (await session.IncrementalTimeSeriesFor(user2, "INC:Heartrate")
                                .GetAsync())
                            .ToList();

                        Assert.Equal(180, values.Count);

                        for (int i = 0; i < values.Count; i++)
                        {
                            Assert.Equal(baseline.AddSeconds(i * 10), values[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(i % 60, values[i].Values[0]);
                        }
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task IncrementTimeSeriesBackup()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var config = Backup.CreateBackupConfiguration(backupPath);

            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.IncrementalTimeSeriesFor("users/1", "INC:Heartrate").Increment(baseline.AddSeconds(i * 10), 1);
                    }

                    await session.SaveChangesAsync();
                }

                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 10; i < 20; i++)
                    {
                        session.IncrementalTimeSeriesFor("users/1", "INC:Heartrate").Increment(baseline.AddSeconds(i * 10), 1);
                    }

                    await session.SaveChangesAsync();
                }

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

                using (var restored = RestoreAndGetStore(store, backupPath, out var releaseDatabase))
                using (releaseDatabase)
                {
                    var stats = await restored.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfTimeSeriesSegments);

                    using (var session = restored.OpenAsyncSession())
                    {
                        var user1 = await session.LoadAsync<User>("users/1");
                        Assert.Equal("oren", user1.Name);

                        var values = (await session.IncrementalTimeSeriesFor("users/1", "INC:Heartrate")
                                .GetAsync())
                            .ToList();

                        Assert.Equal(20, values.Count);

                        for (int i = 0; i < values.Count; i++)
                        {
                            Assert.Equal(baseline.AddSeconds(i * 10), values[i].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                            Assert.Equal(1, values[i].Values[0]);
                        }
                    }
                }
            }
        }


        [Fact, Trait("Category", "Smuggler")]
        public async Task BackupTaskShouldStayOnTheOriginalNode()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var cluster = await CreateRaftCluster(5);

            using (var store = GetDocumentStore(new Options
            {
                ReplicationFactor = 5,
                Server = cluster.Leader
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                    Assert.True(await WaitForDocumentInClusterAsync<User>(session.Advanced.RequestExecutor.TopologyNodes, "users/1", u => u.Name == "oren",
                        TimeSpan.FromSeconds(15)));
                }

                var operation = new UpdatePeriodicBackupOperation(Backup.CreateBackupConfiguration(backupPath));
                var result = await store.Maintenance.SendAsync(operation);
                var periodicBackupTaskId = result.TaskId;

                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(periodicBackupTaskId, TimeSpan.FromSeconds(15));

                await Backup.RunBackupInClusterAsync(store, result.TaskId, isFullBackup: true);
                await ActionWithLeader(async x => await Cluster.WaitForRaftCommandToBeAppliedInClusterAsync(x, nameof(UpdatePeriodicBackupStatusCommand)), cluster.Nodes);

                var backupInfo = new GetOngoingTaskInfoOperation(result.TaskId, OngoingTaskType.Backup);
                var backupInfoResult = await store.Maintenance.SendAsync(backupInfo);
                var originalNode = backupInfoResult.ResponsibleNode.NodeTag;

                var toDelete = cluster.Nodes.First(n => n.ServerStore.NodeTag != originalNode);
                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: true, fromNode: toDelete.ServerStore.NodeTag, timeToWaitForConfirmation: TimeSpan.FromSeconds(30)));

                var nodesCount = await WaitForValueAsync(async () =>
                {
                    var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    if (res == null)
                    {
                        return -1;
                    }

                    return res.Topology.Count;
                }, 4);

                Assert.Equal(4, nodesCount);

                await Backup.RunBackupInClusterAsync(store, backupInfoResult.TaskId, isFullBackup: true);
                await ActionWithLeader(async x => await Cluster.WaitForRaftCommandToBeAppliedInClusterAsync(x, nameof(UpdatePeriodicBackupStatusCommand)), cluster.Nodes);

                backupInfoResult = await store.Maintenance.SendAsync(backupInfo);
                Assert.Equal(originalNode, backupInfoResult.ResponsibleNode.NodeTag);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CreateFullBackupWithSeveralCompareExchange()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var user = new User
                {
                    Name = "💩"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/poo", user, 0));

                var user2 = new User
                {
                    Name = "💩🤡"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/pooclown", user2, 0));

                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */6 * * *");
                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                var files = Directory.GetFiles(backupDirectory)
                    .Where(BackupUtils.IsBackupFile)
                    .OrderBackups()
                    .ToArray();

                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = files.Last()
                };

                var restoreOperation = new RestoreBackupOperation(restoreConfig);
                store.Maintenance.Server.Send(restoreOperation)
                    .WaitForCompletion(TimeSpan.FromSeconds(30));

                using (var store2 = GetDocumentStore(new Options()
                {
                    CreateDatabase = false,
                    ModifyDatabaseName = s => databaseName
                }))
                {
                    using (var session = store2.OpenAsyncSession(new SessionOptions
                    {
                        TransactionMode = TransactionMode.ClusterWide
                    }))
                    {
                        var user1 = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("emojis/poo"));
                        var user3 = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("emojis/pooclown"));
                        Assert.Equal(user.Name, user1.Value.Name);
                        Assert.Equal(user2.Name, user3.Value.Name);
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task incremental_and_full_check_last_file_for_backup()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "users/1" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "users/2" }, "users/2");
                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);

                string backupFolder = Directory.GetDirectories(backupPath).OrderBy(Directory.GetCreationTime).Last();
                var lastBackupToRestore = Directory.GetFiles(backupFolder).Where(BackupUtils.IsBackupFile)
                    .OrderBackups()
                    .ToArray();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "users/3" }, "users/3");
                    await session.SaveChangesAsync();
                }

                lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);

                var databaseName = GetDatabaseName() + "restore";

                using (Backup.RestoreDatabase(
                    store,
                    new RestoreBackupConfiguration()
                    {
                        BackupLocation = backupFolder,
                        DatabaseName = databaseName,
                        LastFileNameToRestore = lastBackupToRestore.Last()
                    },
                    TimeSpan.FromSeconds(60)))
                {
                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var bestUser = await session.LoadAsync<User>("users/1");
                        var mediocreUser1 = await session.LoadAsync<User>("users/2");
                        var mediocreUser2 = await session.LoadAsync<User>("users/3");
                        Assert.NotNull(bestUser);
                        Assert.NotNull(mediocreUser1);
                        Assert.Null(mediocreUser2);
                    }
                }
            }
        }

        private static async Task<long> RunBackupOperationAndAssertCompleted(DocumentStore store, bool isFullBackup, long taskId)
        {
            var op = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup, taskId));
            await op.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

            return op.Id;
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_run_incremental_with_no_changes()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "users/1" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false);
                var value = status.LocalBackup.IncrementalBackupDurationInMs;
                Assert.Equal(0, value);

                string backupFolder = Directory.GetDirectories(backupPath).OrderBy(Directory.GetCreationTime).Last();
                var backupsToRestore = Directory.GetFiles(backupFolder).Where(BackupUtils.IsBackupFile)
                    .OrderBackups()
                    .ToArray();

                Assert.Equal(1, backupsToRestore.Length);

                var databaseName = GetDatabaseName() + "restore";

                using (Backup.RestoreDatabase(
                    store,
                    new RestoreBackupConfiguration()
                    {
                        BackupLocation = backupFolder,
                        DatabaseName = databaseName,
                        LastFileNameToRestore = backupsToRestore.Last()
                    },
                    TimeSpan.FromSeconds(60)))
                {
                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var user = await session.LoadAsync<User>("users/1");
                        Assert.NotNull(user);
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_create_local_snapshot_and_restore_using_restore_point()
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
                var localSettings = new LocalSettings()
                {
                    FolderPath = backupPath
                };

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                var client = store.GetRequestExecutor().HttpClient;

                var data = new StringContent(JsonConvert.SerializeObject(localSettings), Encoding.UTF8, "application/json");
                var response = await client.PostAsync(store.Urls.First() + "/admin/restore/points?type=Local ", data);
                string result = response.Content.ReadAsStringAsync().Result;
                var restorePoints = JsonConvert.DeserializeObject<RestorePoints>(result);
                Assert.Equal(1, restorePoints.List.Count);
                var point = restorePoints.List.First();
                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = $"restored_database-{Guid.NewGuid()}";
                var restoreOperation = await store.Maintenance.Server.SendAsync(new RestoreBackupOperation(new RestoreBackupConfiguration()
                {
                    DatabaseName = databaseName,
                    BackupLocation = backupDirectory,
                    DisableOngoingTasks = true,
                    LastFileNameToRestore = point.FileName,
                }));

                await restoreOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
                using (var store2 = GetDocumentStore(new Options() { CreateDatabase = false, ModifyDatabaseName = s => databaseName }))
                {
                    using (var session = store2.OpenSession(databaseName))
                    {
                        var users = session.Load<User>(new[] { "users/1", "users/2" });
                        Assert.True(users.Any(x => x.Value.Name == "oren"));

                        var val = session.CountersFor("users/1").Get("likes");
                        Assert.Equal(100, val);
                    }

                    var originalDatabase = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).Result;
                    var restoredDatabase = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).Result;
                    using (restoredDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(ctx);
                        var expected1 = $"A:5-{restoredDatabase.DbBase64Id}, A:4-{originalDatabase.DbBase64Id}";
                        var expected2 = $"A:4-{originalDatabase.DbBase64Id}, A:5-{restoredDatabase.DbBase64Id}";
                        Assert.True(databaseChangeVector == expected1 || databaseChangeVector == expected2, $"Expected:\t\"{databaseChangeVector}\"\nActual:\t\"{expected1}\" or \"{expected2}\"\n");
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task SuccessfulFullBackupAfterAnErrorOneShouldClearTheErrorStatesFromBackupStatusAndLocalBackup()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "egr"
                    }, "users/1");

                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "0 0 1 1 *", backupEncryptionSettings: new BackupEncryptionSettings()
                {
                    EncryptionMode = EncryptionMode.UseDatabaseKey
                });
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store, opStatus: OperationStatus.Faulted);
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                PeriodicBackupStatus status = store.Maintenance.Send(operation).Status;
                Assert.NotNull(status.Error);
                Assert.NotNull(status.LocalBackup);
                Assert.NotNull(status.LocalBackup.Exception);

                // status.LastFullBackup is only saved if the backup ran successfully
                Assert.Null(status.LastFullBackup);
                Assert.NotNull(status.LastFullBackupInternal);
                var oldLastFullBackupInternal = status.LastFullBackupInternal;
                Assert.True(status.IsFull, "status.IsFull");
                Assert.Null(status.LastEtag);
                Assert.Null(status.FolderName);
                Assert.Null(status.LastIncrementalBackup);
                Assert.Null(status.LastIncrementalBackupInternal);
                // update LastOperationId even on the task error
                Assert.NotNull(status.LastOperationId);
                var oldOpId = status.LastOperationId;

                Assert.NotNull(status.LastRaftIndex);
                Assert.Null(status.LastRaftIndex.LastEtag);
                Assert.NotNull(status.LocalBackup.LastFullBackup);
                var oldLastFullBackup = status.LastFullBackup;

                Assert.Null(status.LocalBackup.LastIncrementalBackup);
                Assert.NotNull(status.NodeTag);
                Assert.True(status.DurationInMs >= 0, "status.DurationInMs >= 0");
                // update backup task
                config.TaskId = backupTaskId;
                config.BackupEncryptionSettings = null;
                var id = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                Assert.Equal(backupTaskId, id);

                status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: true, expectedEtag: 1);

                Assert.Null(status.Error);
                Assert.NotNull(status.LocalBackup);
                Assert.Null(status.LocalBackup.Exception);

                // status.LastFullBackup is only saved if the backup ran successfully
                Assert.NotNull(status.LastFullBackup);
                Assert.NotNull(status.LastFullBackupInternal);
                Assert.NotEqual(oldLastFullBackupInternal, status.LastFullBackupInternal);

                Assert.True(status.IsFull, "status.IsFull");
                Assert.Equal(1, status.LastEtag);
                Assert.NotNull(status.FolderName);
                Assert.Null(status.LastIncrementalBackup);
                Assert.Null(status.LastIncrementalBackupInternal);
                Assert.NotNull(status.LastOperationId);
                Assert.NotEqual(oldOpId, status.LastOperationId);
                Assert.NotNull(status.LastRaftIndex);
                Assert.NotNull(status.LastRaftIndex.LastEtag);
                Assert.NotNull(status.LocalBackup.LastFullBackup);
                Assert.NotEqual(oldLastFullBackup, status.LocalBackup.LastFullBackup);
                Assert.Null(status.LocalBackup.LastIncrementalBackup);
                Assert.NotNull(status.NodeTag);
                Assert.True(status.DurationInMs > 0, "status.DurationInMs > 0");
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task FullBackupShouldSkipDeadSegments()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "oren"
                    }, "users/1");

                    for (int i = 0; i < 360; i++)
                    {
                        session.TimeSeriesFor("users/1", "Heartrate")
                            .Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/1");
                    }

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor("users/1", "Heartrate").Delete();
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DatabaseName = databaseName
                }))
                {
                    var stats = await store.Maintenance.ForDatabase(databaseName).SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(0, stats.CountOfTimeSeriesSegments);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task IncrementalBackupShouldIncludeDeadSegments()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "oren"
                    }, "users/1");

                    for (int i = 0; i < 360; i++)
                    {
                        session.TimeSeriesFor("users/1", "Heartrate")
                            .Append(baseline.AddSeconds(i * 10), new[] { i % 60d }, "watches/1");
                    }

                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);
                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor("users/1", "Heartrate").Delete();
                    await session.SaveChangesAsync();
                }

                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DatabaseName = databaseName
                }))
                {
                    var stats = await store.Maintenance.ForDatabase(databaseName).SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfTimeSeriesSegments);

                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var user = await session.LoadAsync<User>("users/1");
                        Assert.NotNull(user);
                        Assert.Null(await session.TimeSeriesFor("users/1", "Heartrate").GetAsync());
                    }
                }
            }
        }

        [Theory, Trait("Category", "Smuggler")]
        [InlineData(BackupType.Snapshot)]
        [InlineData(BackupType.Backup)]
        public async Task CanCreateOneTimeBackupAndRestore(BackupType backupType)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var name = "EGR";

            using (var store = GetDocumentStore(new Options { DeleteDatabaseOnDispose = true, Path = NewDataPath() }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = name }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = new BackupConfiguration
                {
                    BackupType = backupType,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                };

                var operation = await store.Maintenance.SendAsync(new BackupOperation(config));
                var backupResult = (BackupResult)await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15));
                Assert.True(backupResult.Documents.Processed);
                Assert.True(backupResult.CompareExchange.Processed);
                Assert.True(backupResult.CompareExchangeTombstones.Processed);
                Assert.True(backupResult.Conflicts.Processed);
                Assert.True(backupResult.Counters.Processed);
                Assert.True(backupResult.DatabaseRecord.Processed);
                Assert.True(backupResult.Identities.Processed);
                Assert.True(backupResult.Indexes.Processed);
                Assert.Null(backupResult.LegacyLastAttachmentEtag);
                Assert.Null(backupResult.LegacyLastDocumentEtag);
                Assert.True(backupResult.RevisionDocuments.Processed);
                Assert.True(backupResult.TimeSeries.Processed);
                Assert.True(backupResult.Tombstones.Processed);
                Assert.True(backupResult.Subscriptions.Processed);
                Assert.Equal(1, backupResult.Documents.ReadCount);
                Assert.NotEmpty(backupResult.Messages);

                // check the backup status of one time backup
                var client = store.GetRequestExecutor().HttpClient;
                // one time backup always save the status under task id 0
                var response = await client.GetAsync(store.Urls.First() + $"/periodic-backup/status?name={store.Database}&taskId=0");
                string result = response.Content.ReadAsStringAsync().Result;
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    using var bjro = ctx.Sync.ReadForMemory(result, "test");
                    bjro.TryGet("Status", out BlittableJsonReaderObject statusBjro);
                    var status = JsonDeserializationClient.PeriodicBackupStatus(statusBjro);
                    Assert.NotNull(status.LocalBackup);
                    Assert.False(status.LocalBackup.TempFolderUsed);
                    Assert.True(status.IsFull);
                    Assert.False(status.IsEncrypted);
                    Assert.NotNull(status.UploadToAzure);
                    Assert.True(status.UploadToAzure.Skipped);
                    Assert.NotNull(status.UploadToFtp);
                    Assert.True(status.UploadToFtp.Skipped);
                    Assert.NotNull(status.UploadToGlacier);
                    Assert.True(status.UploadToGlacier.Skipped);
                    Assert.NotNull(status.UploadToGoogleCloud);
                    Assert.True(status.UploadToGoogleCloud.Skipped);
                    Assert.NotNull(status.UploadToS3);
                    Assert.True(status.UploadToS3.Skipped);

                    Assert.Equal("A", status.NodeTag);
                    Assert.True(status.DurationInMs > 0, "status.DurationInMs > 0");

                    if (backupType == BackupType.Backup)
                    {
                        Assert.False(backupResult.SnapshotBackup.Processed);
                        Assert.True(backupResult.Documents.LastEtag > 0, "backupResult.Documents.LastEtag > 0");
                        Assert.Equal(BackupType.Backup, status.BackupType);
                    }
                    else
                    {
                        Assert.True(backupResult.SnapshotBackup.Processed);
                        Assert.Equal(BackupType.Snapshot, status.BackupType);
                    }
                }

                var databaseName = $"restored_database-{Guid.NewGuid()}";
                var backupLocation = Directory.GetDirectories(backupPath).First();

                using (ReadOnly(backupLocation))
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupLocation, DatabaseName = databaseName }))
                {
                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var usr = await session.LoadAsync<User>("users/1");
                        Assert.Equal(name, usr.Name);
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task PeriodicBackup_WhenEnabledAndDefinesNoDestinations_ShouldThrows()
        {
            using var store = GetDocumentStore();

            var config = Backup.CreateBackupConfiguration();
            var operation = new UpdatePeriodicBackupOperation(config);

            Assert.False(config.ValidateDestinations(out var message));
            var exception = await Assert.ThrowsAnyAsync<Exception>(async () => await store.Maintenance.SendAsync(operation));
            Assert.Contains(message, exception.Message);
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ManualBackup_WhenDefinesNoDestinations_ShouldThrowsOnServerAsWell()
        {
            using var store = GetDocumentStore();

            var config = new BackupConfiguration { BackupType = BackupType.Backup };

            using (var requestExecutor = store.GetRequestExecutor())
            using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var command = new BackupOperation.BackupCommand(config);
                var request = command.CreateRequest(context, new ServerNode { Url = store.Urls.First(), Database = store.Database }, out var url);
                request.RequestUri = new Uri(url);
                var client = store.GetRequestExecutor(store.Database).HttpClient;
                var response = await client.SendAsync(request);

                Assert.False(config.ValidateDestinations(out var message));
                var exception = await Assert.ThrowsAnyAsync<Exception>(async () => await ExceptionDispatcher.Throw(context, response));
                Assert.Contains(message, exception.Message);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task OneTimeBackupWithInvalidConfigurationShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGR" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = new BackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = null
                };

                await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new BackupOperation(config)));
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanGetOneTimeBackupStatusFromDatabasesInfo()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var name = "EGR";

            using (var store = GetDocumentStore(new Options { DeleteDatabaseOnDispose = true, Path = NewDataPath() }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = name }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = new BackupConfiguration { BackupType = BackupType.Backup, LocalSettings = new LocalSettings { FolderPath = backupPath } };

                var operation = await store.Maintenance.SendAsync(new BackupOperation(config));
                var backupResult = (BackupResult)await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

                var client = store.GetRequestExecutor().HttpClient;
                var response = await client.GetAsync(store.Urls.First() + $"/databases?name={store.Database}");
                string result = response.Content.ReadAsStringAsync().Result;
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    using var bjro = ctx.Sync.ReadForMemory(result, "test");
                    var databaseInfo = JsonDeserializationServer.DatabaseInfo(bjro);
                    Assert.NotNull(databaseInfo);
                    Assert.Equal(BackupTaskType.OneTime, databaseInfo.BackupInfo.BackupTaskType);
                    Assert.Equal(1, databaseInfo.BackupInfo.Destinations.Count);
                    Assert.Equal(nameof(BackupConfiguration.BackupDestination.Local), databaseInfo.BackupInfo.Destinations.First());
                    Assert.NotNull(databaseInfo.BackupInfo.LastBackup);
                    Assert.Equal(0, databaseInfo.BackupInfo.IntervalUntilNextBackupInSec);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task IncrementalBackupWithNoChangesShouldSet_BackupStatus_IsFull_ToFalse()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "egr"
                    }, "users/1");

                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "0 0 1 1 *");
                var backupTaskId = Backup.UpdateConfigAndRunBackup(Server, config, store);
                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false);

                Assert.False(status.IsFull);
                Assert.NotNull(status.LocalBackup);
                Assert.Equal(0, status.LocalBackup.IncrementalBackupDurationInMs);
                Assert.Equal(BackupType.Backup, status.BackupType);
                Assert.True(status.DurationInMs >= 0, "status.DurationInMs >= 0");
                Assert.Null(status.Error);
                Assert.False(status.IsEncrypted);
                Assert.Equal(1, status.LastEtag);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_move_database_with_backup()
        {
            DoNotReuseServer();

            var cluster = await CreateRaftCluster(2);
            var databaseName = GetDatabaseName();
            await CreateDatabaseInCluster(databaseName, 2, cluster.Nodes[0].WebUrl);

            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (DocumentStore store = new DocumentStore
            {
                Urls = new[]
                {
                    cluster.Nodes[0].WebUrl,
                    cluster.Nodes[1].WebUrl
                },
                Database = databaseName
            })
            {
                store.Initialize();
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Grisha" }, "users/1");
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "* * * * *");
                var backupTaskId = await Backup.CreateAndRunBackupInClusterAsync(config, store);
                var responsibleNode = await Backup.GetBackupResponsibleNode(cluster.Leader, backupTaskId, databaseName, keepTaskOnOriginalMemberNode: true);

                store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: responsibleNode, timeToWaitForConfirmation: TimeSpan.FromSeconds(30)));
                await WaitForDatabaseToBeDeleted(store, TimeSpan.FromSeconds(30));

                var server = cluster.Nodes.FirstOrDefault(x => x.ServerStore.NodeTag == responsibleNode == false);
                server.ServerStore.LicenseManager.LicenseStatus.Attributes["highlyAvailableTasks"] = false;

                var newResponsibleNode = await Backup.GetBackupResponsibleNode(server, backupTaskId, databaseName, keepTaskOnOriginalMemberNode: true);

                Assert.Equal(server.ServerStore.NodeTag, newResponsibleNode);
                Assert.NotEqual(responsibleNode, newResponsibleNode);
            }

            async Task<bool> WaitForDatabaseToBeDeleted(IDocumentStore store, TimeSpan timeout)
            {
                var pollingInterval = timeout.TotalSeconds < 1 ? timeout : TimeSpan.FromSeconds(1);
                var sw = Stopwatch.StartNew();
                while (true)
                {
                    var delayTask = Task.Delay(pollingInterval);
                    var dbTask = store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                    var doneTask = await Task.WhenAny(dbTask, delayTask);
                    if (doneTask == delayTask)
                    {
                        if (sw.Elapsed > timeout)
                        {
                            return false;
                        }
                        continue;
                    }
                    var dbRecord = dbTask.Result;
                    if (dbRecord == null || dbRecord.DeletionInProgress == null || dbRecord.DeletionInProgress.Count == 0)
                    {
                        return true;
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task Backup_WhenContainRevisionWithoutConfiguration_ShouldBackupRevisions()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            var userForFullBackup = new User();
            var userForIncrementalBackup = new User();
            using (var src = GetDocumentStore())
            {
                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(userForFullBackup);
                    await session.StoreAsync(userForIncrementalBackup);
                    await session.SaveChangesAsync();

                    session.Advanced.Revisions.ForceRevisionCreationFor(userForFullBackup.Id);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "* * * * *");
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, src);

                using (var session = src.OpenAsyncSession())
                {
                    session.Advanced.Revisions.ForceRevisionCreationFor(userForIncrementalBackup.Id);
                    await session.SaveChangesAsync();
                }
                await Backup.RunBackupAsync(Server, backupTaskId, src, isFullBackup: false);
            }

            using (var dest = GetDocumentStore())
            {
                string fromDirectory = Directory.GetDirectories(backupPath).First();
                await dest.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), fromDirectory);
                using (var session = dest.OpenAsyncSession())
                {
                    await AssertRevisions(userForFullBackup.Id);
                    await AssertRevisions(userForIncrementalBackup.Id);

                    async Task AssertRevisions(string id)
                    {
                        var revision = await session.Advanced.Revisions.GetForAsync<User>(id);
                        Assert.NotNull(revision);
                        Assert.NotEmpty(revision);
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task Should_throw_on_document_with_changed_collection_when_no_tombstones_processed()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                const string documentId = "Users/1";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Grisha" }, documentId);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                var fullBackupOpId = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(backupTaskId))).Status.LastOperationId;
                Assert.NotNull(fullBackupOpId);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(documentId);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Person { Name = "Grisha" }, documentId);
                    await session.SaveChangesAsync();
                }

                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false);
                Assert.NotNull(status.LastOperationId);
                Assert.NotEqual(fullBackupOpId, status.LastOperationId);

                // to have a different count of docs in databases
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Person { Name = "EGOR" }, "Users/2");
                    await session.SaveChangesAsync();
                }

                string backupFolder = Directory.GetDirectories(backupPath).OrderBy(Directory.GetCreationTime).Last();
                var backupFilesToRestore = Directory.GetFiles(backupFolder).Where(BackupUtils.IsBackupFile)
                    .OrderBackups()
                    .ToArray();

                var databaseName = GetDatabaseName() + "_restore";
                using (Databases.EnsureDatabaseDeletion(databaseName, store))
                {
                    var restoreOperation = new RestoreBackupOperation(new RestoreBackupConfiguration
                    {
                        BackupLocation = backupFolder,
                        DatabaseName = databaseName,
                        LastFileNameToRestore = backupFilesToRestore.First()
                    });

                    var operation = await store.Maintenance.Server.SendAsync(restoreOperation);
                    var res = (RestoreResult)await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
                    Assert.Equal(1, res.Documents.ReadCount);

                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var bestUser = await session.LoadAsync<User>("users/1");
                        Assert.NotNull(bestUser);
                        Assert.Equal("Grisha", bestUser.Name);

                        var metadata = session.Advanced.GetMetadataFor(bestUser);
                        var expectedCollection = store.Conventions.GetCollectionName(typeof(User));
                        Assert.True(metadata.TryGetValue(Constants.Documents.Metadata.Collection, out string collection));
                        Assert.Equal(expectedCollection, collection);
                        var stats = store.Maintenance.ForDatabase(databaseName).Send(new GetStatisticsOperation());
                        Assert.Equal(1, stats.CountOfDocuments);
                    }

                    var options = new DatabaseSmugglerImportOptions();
                    options.OperateOnTypes &= ~DatabaseItemType.Tombstones;
                    var opRes = await store.Smuggler.ForDatabase(databaseName).ImportAsync(options, backupFilesToRestore.Last());
                    await Assert.ThrowsAsync<DocumentCollectionMismatchException>(async () => await opRes.WaitForCompletionAsync(TimeSpan.FromSeconds(60)));
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task Can_restore_backup_when_document_changed_collection_between_backups()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                const string documentId = "Users/1";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Grisha" }, documentId);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                var fullBackupOpId = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(backupTaskId))).Status.LastOperationId;
                Assert.NotNull(fullBackupOpId);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(documentId);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Person { Name = "Grisha" }, documentId);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(documentId);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new PersonWithAddress { Name = "Grisha" }, documentId);
                    await session.SaveChangesAsync();
                }

                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false);
                Assert.NotNull(status.LastOperationId);
                Assert.NotEqual(fullBackupOpId, status.LastOperationId);

                // to have a different count of docs in databases
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Person { Name = "EGOR" }, "Users/2");
                    await session.SaveChangesAsync();
                }

                string backupFolder = Directory.GetDirectories(backupPath).OrderBy(Directory.GetCreationTime).Last();
                var lastBackupToRestore = Directory.GetFiles(backupFolder).Where(BackupUtils.IsBackupFile)
                    .OrderBackups()
                    .ToArray();

                var databaseName = GetDatabaseName() + "_restore";
                using (Databases.EnsureDatabaseDeletion(databaseName, store))
                {
                    var restoreOperation = new RestoreBackupOperation(new RestoreBackupConfiguration
                    {
                        BackupLocation = backupFolder,
                        DatabaseName = databaseName,
                        LastFileNameToRestore = lastBackupToRestore.Last()
                    });

                    var operation = await store.Maintenance.Server.SendAsync(restoreOperation);
                    var res = (RestoreResult)await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
                    Assert.Equal(2, res.Documents.ReadCount);
                    Assert.Equal(2, res.Tombstones.ReadCount);

                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var bestUser = await session.LoadAsync<PersonWithAddress>("users/1");
                        Assert.NotNull(bestUser);
                        Assert.Equal("Grisha", bestUser.Name);

                        var metadata = session.Advanced.GetMetadataFor(bestUser);
                        var expectedCollection = store.Conventions.GetCollectionName(typeof(PersonWithAddress));
                        Assert.True(metadata.TryGetValue(Constants.Documents.Metadata.Collection, out string collection));
                        Assert.Equal(expectedCollection, collection);
                        var stats = store.Maintenance.ForDatabase(databaseName).Send(new GetStatisticsOperation());
                        Assert.Equal(1, stats.CountOfDocuments);
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task Can_restore_snapshot_when_document_changed_collection_between_backups()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                const string documentId = "Users/1";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Grisha" }, documentId);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                var fullBackupOpId = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(backupTaskId))).Status.LastOperationId;
                Assert.NotNull(fullBackupOpId);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(documentId);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Person { Name = "Grisha" }, documentId);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(documentId);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new PersonWithAddress { Name = "Grisha" }, documentId);
                    await session.SaveChangesAsync();
                }

                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false);
                Assert.NotNull(status.LastOperationId);
                Assert.NotEqual(fullBackupOpId, status.LastOperationId);

                // to have a different count of docs in databases
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Person { Name = "EGOR" }, "Users/2");
                    await session.SaveChangesAsync();
                }

                string backupFolder = Directory.GetDirectories(backupPath).OrderBy(Directory.GetCreationTime).Last();
                var lastBackupToRestore = Directory.GetFiles(backupFolder).Where(BackupUtils.IsBackupFile)
                    .OrderBackups()
                    .ToArray();

                var databaseName = GetDatabaseName() + "_restore";
                using (Databases.EnsureDatabaseDeletion(databaseName, store))
                {
                    var restoreOperation = new RestoreBackupOperation(new RestoreBackupConfiguration
                    {
                        BackupLocation = backupFolder,
                        DatabaseName = databaseName,
                        LastFileNameToRestore = lastBackupToRestore.Last()
                    });

                    var operation = await store.Maintenance.Server.SendAsync(restoreOperation);
                    var res = (RestoreResult)await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
                    Assert.Equal(2, res.Documents.ReadCount);
                    Assert.Equal(2, res.Tombstones.ReadCount);

                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var bestUser = await session.LoadAsync<PersonWithAddress>("users/1");
                        Assert.NotNull(bestUser);
                        Assert.Equal("Grisha", bestUser.Name);

                        var metadata = session.Advanced.GetMetadataFor(bestUser);
                        var expectedCollection = store.Conventions.GetCollectionName(typeof(PersonWithAddress));
                        Assert.True(metadata.TryGetValue(Constants.Documents.Metadata.Collection, out string collection));
                        Assert.Equal(expectedCollection, collection);
                        var stats = store.Maintenance.ForDatabase(databaseName).Send(new GetStatisticsOperation());
                        Assert.Equal(1, stats.CountOfDocuments);
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task Can_restore_backup_when_document_with_attachment_changed_collection_between_backups()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                const string documentId = "Users/1";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Grisha" }, documentId);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                var fullBackupOpId = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(backupTaskId))).Status.LastOperationId;
                Assert.NotNull(fullBackupOpId);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(documentId);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Person { Name = "Grisha" }, documentId);
                    await session.SaveChangesAsync();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    var result = store.Operations.Send(new PutAttachmentOperation(documentId, "test_attachment", profileStream, "image/png"));
                    Assert.Equal("test_attachment", result.Name);
                }

                var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false);
                Assert.NotNull(status.LastOperationId);
                Assert.NotEqual(fullBackupOpId, status.LastOperationId);

                // to have a different count of docs in databases
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Person { Name = "EGOR" }, "Users/2");
                    await session.SaveChangesAsync();
                }

                string backupFolder = Directory.GetDirectories(backupPath).OrderBy(Directory.GetCreationTime).Last();
                var lastBackupToRestore = Directory.GetFiles(backupFolder).Where(BackupUtils.IsBackupFile)
                    .OrderBackups()
                    .ToArray();

                var databaseName = GetDatabaseName() + "_restore";
                using (Databases.EnsureDatabaseDeletion(databaseName, store))
                {
                    var restoreOperation = new RestoreBackupOperation(new RestoreBackupConfiguration
                    {
                        BackupLocation = backupFolder,
                        DatabaseName = databaseName,
                        LastFileNameToRestore = lastBackupToRestore.Last()
                    });

                    var operation = await store.Maintenance.Server.SendAsync(restoreOperation);
                    var res = (RestoreResult)await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
                    Assert.Equal(2, res.Documents.ReadCount);
                    Assert.Equal(1, res.Tombstones.ReadCount);
                    WaitForUserToContinueTheTest(store);
                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var bestUser = await session.LoadAsync<Person>("users/1");
                        Assert.NotNull(bestUser);
                        Assert.Equal("Grisha", bestUser.Name);

                        var metadata = session.Advanced.GetMetadataFor(bestUser);
                        var expectedCollection = store.Conventions.GetCollectionName(typeof(Person));
                        Assert.True(metadata.TryGetValue(Constants.Documents.Metadata.Collection, out string collection));
                        Assert.Equal(expectedCollection, collection);
                        var stats = store.Maintenance.ForDatabase(databaseName).Send(new GetStatisticsOperation());
                        Assert.Equal(1, stats.CountOfDocuments);
                        Assert.Equal(1, stats.CountOfAttachments);
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ShouldKeepTheBackupRunningIfItGotActiveByOtherNodeWhileRunning()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options
            {
                Server = server
            }))
            {
                const string documentId = "Users/1";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Grisha" }, documentId);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);


                var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                Assert.NotNull(documentDatabase);
                var tcs = new TaskCompletionSource<object>();
                documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = tcs;
                try
                {
                    var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store, opStatus: OperationStatus.InProgress);
                    var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    var backups1 = record1.PeriodicBackups;
                    Assert.Equal(1, backups1.Count);

                    var taskId = backups1.First().TaskId;
                    var responsibleDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                    Assert.NotNull(responsibleDatabase);
                    var tag = responsibleDatabase.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                    Assert.Equal(server.ServerStore.NodeTag, tag);

                    responsibleDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateActiveByOtherNodeStatus_UpdateConfigurations = true;
                    responsibleDatabase.PeriodicBackupRunner.UpdateConfigurations(record1);
                    tcs.SetResult(null);

                    responsibleDatabase.PeriodicBackupRunner._forTestingPurposes = null;
                    var getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(taskId);
                    PeriodicBackupStatus status = null;
                    var val = WaitForValue(() =>
                    {
                        status = store.Maintenance.Send(getPeriodicBackupStatus).Status;
                        return status?.LastFullBackup != null;
                    }, true, timeout: 66666, interval: 444);
                    Assert.NotNull(status);
                    Assert.Null(status.Error);
                    Assert.True(val, "Failed to complete the backup in time");
                }
                finally
                {
                    try
                    {
                        tcs.TrySetResult(null);
                    }
                    catch
                    {
                        // ignored
                    }

                    documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = null;
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ShouldCancelTheBackupRunningIfItGotDisabledWhileRunning()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options
            {
                Server = server
            }))
            {
                const string documentId = "Users/1";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Grisha" }, documentId);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);

                var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                Assert.NotNull(documentDatabase);
                var tcs = new TaskCompletionSource<object>();
                documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = tcs;
                try
                {
                    var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store, opStatus: OperationStatus.InProgress);
                    var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    var backups1 = record1.PeriodicBackups;
                    Assert.Equal(1, backups1.Count);

                    var taskId = backups1.First().TaskId;
                    var responsibleDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                    Assert.NotNull(responsibleDatabase);
                    var tag = responsibleDatabase.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                    Assert.Equal(server.ServerStore.NodeTag, tag);

                    responsibleDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateDisableNodeStatus_UpdateConfigurations = true;
                    responsibleDatabase.PeriodicBackupRunner.UpdateConfigurations(record1);
                    tcs.SetResult(null);

                    responsibleDatabase.PeriodicBackupRunner._forTestingPurposes = null;
                    var val = WaitForValue(() =>
                    {
                        var ongoingTaskBackup = store.Maintenance.Send(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;
                        if (ongoingTaskBackup == null)
                            return false;

                        if (ongoingTaskBackup.BackupDestinations.Count != 1 &&
                            ongoingTaskBackup.BackupDestinations[0] != nameof(BackupConfiguration.BackupDestination.Local))
                            return false;

                        return ongoingTaskBackup.OnGoingBackup == null;
                    }, true, timeout: 66666, interval: 444);
                    Assert.True(val, "Failed to complete the backup in time");
                }
                finally
                {
                    try
                    {
                        tcs.TrySetResult(null);
                    }
                    catch
                    {
                        // ignored
                    }
                    documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = null;
                }
            }
        }

                        // Performing backup Delay to the time:
        [InlineData(1)] // until the next scheduled backup time.
        [InlineData(5)] // after the next scheduled backup.
        [Theory, Trait("Category", "Smuggler")]
        public async Task ShouldProperlyPlaceOriginalBackupTimePropertyWithDelay(int delayDurationInMinutes)
        {
            const string fullBackupFrequency = "*/2 * * * *";
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options { Server = server }))
            {
                using (var session = store.OpenAsyncSession())
                    await Backup.FillDatabaseWithRandomDataAsync(databaseSizeInMb: 1, session);

                var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                Assert.NotNull(database);
                database.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = new TaskCompletionSource<object>();

                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: fullBackupFrequency);
                var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store, opStatus: OperationStatus.InProgress);
                
                // Let's delay the backup task
                var taskBackupInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;
                Assert.NotNull(taskBackupInfo);
                Assert.NotNull(taskBackupInfo.OnGoingBackup);
                Assert.NotNull(taskBackupInfo.OnGoingBackup.StartTime);

                var delayDuration = TimeSpan.FromMinutes(delayDurationInMinutes);
                var delayUntil = DateTime.Now + delayDuration;
                await store.Maintenance.SendAsync(new DelayBackupOperation(taskBackupInfo.OnGoingBackup.RunningBackupTaskId, delayDuration));

                // There should be no OnGoingBackup operation in the OngoingTaskBackup
                await WaitForValueAsync(async () =>
                {
                    var afterDelayTaskBackupInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;
                    return afterDelayTaskBackupInfo is { OnGoingBackup: null };
                }, true);

                var backupStatus = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                Assert.NotNull(backupStatus);
                Assert.NotNull(backupStatus.DelayUntil);
                Assert.NotNull(backupStatus.OriginalBackupTime);

                var nextFullBackup = GetNextBackupOccurrence(new NextBackupOccurrenceParameters
                {
                    BackupFrequency = fullBackupFrequency,
                    Configuration = config,
                    LastBackupUtc = taskBackupInfo.OnGoingBackup.StartTime.Value
                });
                Assert.NotNull(nextFullBackup);

                Assert.Equal(backupStatus.OriginalBackupTime,
                    delayUntil < nextFullBackup 
                        ? taskBackupInfo.OnGoingBackup.StartTime    // until the next scheduled backup time.
                        : nextFullBackup.Value.ToUniversalTime());  // after the next scheduled backup.
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDelayBackupTask()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options { Server = server }))
            {
                using (var session = store.OpenAsyncSession())
                    await Backup.FillDatabaseWithRandomDataAsync(databaseSizeInMb: 10, session);

                var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                Assert.NotNull(database);
                database.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = new TaskCompletionSource<object>();

                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *");
                var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store, opStatus: OperationStatus.InProgress);

                // The backup task is running, and the next backup should be scheduled for the next minute (based on the backup configuration)
                var taskBackupInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;
                Assert.NotNull(taskBackupInfo);
                Assert.NotNull(taskBackupInfo.OnGoingBackup);
                var expectedNextBackupDateTime = DateTime.UtcNow.Date
                    .AddHours(DateTime.UtcNow.Hour)
                    .AddMinutes(DateTime.UtcNow.Minute + 1);
                Assert.Equal(expectedNextBackupDateTime, taskBackupInfo.NextBackup.DateTime);

                // Let's delay the backup task to 1 hour
                var delayDuration = TimeSpan.FromHours(1);
                var sw = Stopwatch.StartNew();
                await store.Maintenance.SendAsync(new DelayBackupOperation(taskBackupInfo.OnGoingBackup.RunningBackupTaskId, delayDuration));

                // There should be no OnGoingBackup operation in the OngoingTaskBackup
                // The next backup should be scheduled in almost 1 hour
                OngoingTaskBackup afterDelayTaskBackupInfo = null;
                await WaitForValueAsync(async () =>
                {
                    afterDelayTaskBackupInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;
                    return afterDelayTaskBackupInfo is { OnGoingBackup: null };
                }, true);
                Assert.Null(afterDelayTaskBackupInfo.LastFullBackup);
                Assert.True(afterDelayTaskBackupInfo.NextBackup.TimeSpan > delayDuration.Subtract(TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds + 1_000)) &&
                            afterDelayTaskBackupInfo.NextBackup.TimeSpan <= delayDuration, 
                    $"NextBackup in: `{afterDelayTaskBackupInfo.NextBackup.TimeSpan}`, delayDuration with tolerance: `{delayDuration.Subtract(TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds + 1_000))}`, " +
                    $"delayDuration: `{delayDuration}`");

                // DelayUntil value in backup status and the time of scheduled next backup should be equal
                var backupStatus = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                Assert.NotNull(backupStatus);
                Assert.NotNull(backupStatus.DelayUntil);
                Assert.Equal(backupStatus.DelayUntil, afterDelayTaskBackupInfo.NextBackup.DateTime);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ShouldDelayBackupOnNotResponsibleNode()
        {
            const int clusterSize = 3;

            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();

            var (nodes, leaderServer) = await CreateRaftCluster(clusterSize);
            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leaderServer.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
            })
            {
                leaderStore.Initialize();
                await Backup.FillClusterDatabaseWithRandomDataAsync(databaseSizeInMb: 10, leaderStore, clusterSize);

                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", mentorNode: leaderServer.ServerStore.NodeTag);
                var taskId = await Backup.UpdateConfigAndRunBackupAsync(leaderServer, config, leaderStore);

                var responsibleDatabase = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(leaderStore.Database).ConfigureAwait(false);
                Assert.NotNull(responsibleDatabase);
                responsibleDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = new TaskCompletionSource<object>();

                var now = DateTime.UtcNow;
                var expectedNextBackupDateTime = now.Date
                    .AddHours(now.Hour)
                    .AddMinutes(now.Minute + 1);

                await Backup.RunBackupInClusterAsync(leaderStore, taskId, opStatus: OperationStatus.InProgress);

                // Just to be sure, the DelayUntil value was not set before the task delaying
                var backupStatus = (await leaderStore.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                Assert.NotNull(backupStatus);
                Assert.Null(backupStatus.DelayUntil);

                // The backup task is running on the mentor node, and the next backup should be scheduled for the next minute (based on the backup configuration) without any errors
                OngoingTaskBackup onGoingTaskInfo = null;
                await WaitForValueAsync(async () =>
                {
                    onGoingTaskInfo = await leaderStore.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;

                    return onGoingTaskInfo != null &&
                           leaderServer.ServerStore.NodeTag == onGoingTaskInfo.MentorNode &&
                           onGoingTaskInfo.Error == null &&
                           expectedNextBackupDateTime == onGoingTaskInfo.NextBackup.DateTime &&
                           onGoingTaskInfo.OnGoingBackup != null &&
                           onGoingTaskInfo.TaskConnectionStatus == OngoingTaskConnectionStatus.Active;
                }, expectedVal: true,
                    timeout: (int)TimeSpan.FromMinutes(5).TotalMilliseconds,
                    interval: (int)TimeSpan.FromSeconds(1).TotalMilliseconds);

                Assert.NotNull(onGoingTaskInfo);
                Assert.True(leaderServer.ServerStore.NodeTag == onGoingTaskInfo.MentorNode, userMessage: $"The value of 'leaderServer.ServerStore.NodeTag': {leaderServer.ServerStore.NodeTag} is not equal to 'onGoingTaskInfo.MentorNode': {onGoingTaskInfo.MentorNode}.");
                Assert.True(onGoingTaskInfo.Error == null, userMessage: $"The onGoingTaskInfo.Error is not null: {onGoingTaskInfo.Error}.");
                Assert.True(expectedNextBackupDateTime == onGoingTaskInfo.NextBackup.DateTime, userMessage: $"The 'expectedNextBackupDateTime': {expectedNextBackupDateTime} is not equal to 'onGoingTaskInfo.NextBackup.DateTime': {onGoingTaskInfo.NextBackup.DateTime}.");
                Assert.NotNull(onGoingTaskInfo.OnGoingBackup);
                Assert.True(onGoingTaskInfo.TaskConnectionStatus == OngoingTaskConnectionStatus.Active, userMessage: $"The onGoingTaskInfo.TaskConnectionStatus is {onGoingTaskInfo.TaskConnectionStatus}, which is not {nameof(OngoingTaskConnectionStatus.Active)} as expected.");

                // Let's delay the backup task to 1 hour
                var delayDuration = TimeSpan.FromHours(1);
                var sw = Stopwatch.StartNew();
                var runningBackupTaskId = onGoingTaskInfo.OnGoingBackup.RunningBackupTaskId;
                await leaderStore.Maintenance.SendAsync(new DelayBackupOperation(runningBackupTaskId, delayDuration));

                // The next backup should be scheduled in almost 1 hour on the current periodic backup task
                Raven.Server.Documents.PeriodicBackup.PeriodicBackup periodicBackup = null;
                TimeSpan nextBackup = default;
                WaitForValue(() =>
                {
                    periodicBackup = responsibleDatabase.PeriodicBackupRunner.PeriodicBackups.Single(x => x.BackupStatus.TaskId == taskId);
                    nextBackup = periodicBackup.GetNextBackup().TimeSpan;

                    return periodicBackup is { RunningTask: null, RunningBackupStatus: null } &&
                           nextBackup > delayDuration.Subtract(TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds + 1_000)) && nextBackup <= delayDuration;
                }, expectedVal: true,
                    timeout: (int)TimeSpan.FromMinutes(5).TotalMilliseconds,
                    interval: (int)TimeSpan.FromSeconds(1).TotalMilliseconds);

                Assert.NotNull(periodicBackup);
                Assert.Null(periodicBackup.RunningTask);
                Assert.Null(periodicBackup.RunningBackupStatus);
                Assert.True(nextBackup > delayDuration.Subtract(TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds + 1_000)) && nextBackup <= delayDuration,
                    $"The NextBackup is set for: {nextBackup}, the delay duration with tolerance is: {delayDuration.Subtract(TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds + 1_000))}, and the actual delay duration is: {delayDuration}.");

                await WaitForValueAsync(async () =>
                {
                    onGoingTaskInfo = await leaderStore.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;

                    return onGoingTaskInfo != null &&
                           onGoingTaskInfo.ResponsibleNode.NodeTag == leaderServer.ServerStore.NodeTag;
                }, expectedVal: true,
                    timeout: (int)TimeSpan.FromMinutes(5).TotalMilliseconds,
                    interval: (int)TimeSpan.FromSeconds(1).TotalMilliseconds);

                Assert.NotNull(onGoingTaskInfo);
                Assert.Equal(onGoingTaskInfo.ResponsibleNode.NodeTag, leaderServer.ServerStore.NodeTag);

                // We'll check another (not leader) nodes in cluster
                foreach (var server in nodes.Where(node => node != leaderServer))
                {
                    await AssertNextBackupSchedule(server, delayDuration, databaseName, taskId, sw);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ShouldScheduleNextBackupAfterServerRestartCorrectly()
        {
            const int clusterSize = 3;

            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();

            var (nodes, leaderServer) = await CreateRaftCluster(clusterSize);
            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);
            var notLeaderServer = nodes.First(x => x != leaderServer);

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leaderServer.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
            })
            {
                leaderStore.Initialize();
                await Backup.FillClusterDatabaseWithRandomDataAsync(databaseSizeInMb: 10, leaderStore, clusterSize);

                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", mentorNode: leaderServer.ServerStore.NodeTag);
                var taskId = await Backup.UpdateConfigAndRunBackupAsync(leaderServer, config, leaderStore);

                var database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(leaderStore.Database).ConfigureAwait(false);
                Assert.NotNull(database);
                database.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = new TaskCompletionSource<object>();

                await Backup.RunBackupInClusterAsync(leaderStore, taskId, opStatus: OperationStatus.InProgress);
                
                // Let's delay the backup task to 1 hour
                var delayDuration = TimeSpan.FromHours(1);
                var sw = Stopwatch.StartNew();
                var onGoingTaskInfo = await leaderStore.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;
                Assert.NotNull(onGoingTaskInfo);
                var runningBackupTaskId = onGoingTaskInfo.OnGoingBackup.RunningBackupTaskId;
                await leaderStore.Maintenance.SendAsync(new DelayBackupOperation(runningBackupTaskId, delayDuration));

                var disposingResult = await DisposeServerAndWaitForFinishOfDisposalAsync(notLeaderServer);
                using var newServer = GetNewServer(new ServerCreationOptions
                {
                    DeletePrevious = false,
                    RunInMemory = false,
                    DataDirectory = disposingResult.DataDirectory,
                    CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = disposingResult.Url }
                });
                Assert.NotNull(newServer);

                await AssertNextBackupSchedule(serverToObserve: newServer, delayDuration, databaseName, taskId, sw);
            }
        }

        private static async Task AssertNextBackupSchedule(RavenServer serverToObserve, TimeSpan delayDuration, string databaseName, long taskId, Stopwatch sw)
        {
            using (var store = new DocumentStore
            {
                Urls = new[] { serverToObserve.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
            })
            {
                store.Initialize();

                OngoingTaskBackup onGoingTaskBackup = null;
                PeriodicBackupStatus backupStatus = null;
                await WaitForValueAsync(async () =>
                {
                    onGoingTaskBackup = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;

                    if (onGoingTaskBackup?.NextBackup == null ||
                        onGoingTaskBackup.OnGoingBackup != null ||
                        (onGoingTaskBackup.NextBackup.TimeSpan > delayDuration.Subtract(TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds + 1_000)) &&
                        onGoingTaskBackup.NextBackup.TimeSpan <= delayDuration) == false)
                        return false;

                    backupStatus = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(onGoingTaskBackup.TaskId))).Status;
                    return backupStatus is { DelayUntil: { } } &&
                           backupStatus.DelayUntil == onGoingTaskBackup.NextBackup.DateTime;
                }, true,
                    timeout: (int)TimeSpan.FromMinutes(5).TotalMilliseconds,
                    interval: (int)TimeSpan.FromSeconds(1).TotalMilliseconds);

                Assert.NotNull(onGoingTaskBackup.NextBackup);
                Assert.Null(onGoingTaskBackup.OnGoingBackup);
                Assert.True(onGoingTaskBackup.ResponsibleNode.NodeTag != serverToObserve.ServerStore.NodeTag, userMessage: $"The strings 'onGoingTaskBackup.ResponsibleNode.NodeTag' and 'serverToObserve.ServerStore.NodeTag' are equal, with the value '{onGoingTaskBackup.ResponsibleNode.NodeTag}', but they should be different.");
                Assert.True(onGoingTaskBackup.NextBackup.TimeSpan > delayDuration.Subtract(TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds + 1_000)) && onGoingTaskBackup.NextBackup.TimeSpan <= delayDuration,
                    $"The NextBackup is set for: {onGoingTaskBackup.NextBackup.TimeSpan}, the delay duration with tolerance is: {delayDuration.Subtract(TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds + 1_000))}, and the actual delay duration is: {delayDuration}.");

                // DelayUntil value in backup status and the time of scheduled next backup should be equal
                Assert.NotNull(backupStatus);
                Assert.NotNull(backupStatus.DelayUntil);
                Assert.True(backupStatus.DelayUntil == onGoingTaskBackup.NextBackup.DateTime, userMessage: $"The value of 'backupStatus.DelayUntil' is {backupStatus.DelayUntil}, whereas the value of 'onGoingTaskBackup.NextBackup.DateTime' is {onGoingTaskBackup.NextBackup.DateTime}. They are not equal, but they should be.");
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task EveryNodeHasDelayInMemory()
        {
            const int clusterSize = 3;

            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();

            var (nodes, leaderServer) = await CreateRaftCluster(clusterSize);
            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);
            var notLeaderServer = nodes.First(x => x != leaderServer);

            using (var leaderStore = new DocumentStore
                   {
                       Urls = new[] { leaderServer.WebUrl },
                       Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                       Database = databaseName
                   })
            {
                leaderStore.Initialize();

                await Backup.FillClusterDatabaseWithRandomDataAsync(databaseSizeInMb: 10, leaderStore, clusterSize);

                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", mentorNode: leaderServer.ServerStore.NodeTag);
                var taskId = await Backup.UpdateConfigAndRunBackupAsync(leaderServer, config, leaderStore);

                var responsibleDatabase = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(leaderStore.Database).ConfigureAwait(false);
                Assert.NotNull(responsibleDatabase);
                responsibleDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = new TaskCompletionSource<object>();

                await Backup.RunBackupInClusterAsync(leaderStore, taskId, opStatus: OperationStatus.InProgress);

                // Let's delay the backup task to 1 hour
                var delayDuration = TimeSpan.FromHours(1);
                var sw = Stopwatch.StartNew();
                var onGoingTaskInfo = await leaderStore.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;
                Assert.NotNull(onGoingTaskInfo);
                var runningBackupTaskId = onGoingTaskInfo.OnGoingBackup.RunningBackupTaskId;
                await leaderStore.Maintenance.SendAsync(new DelayBackupOperation(runningBackupTaskId, delayDuration));
                
                // We'll check another (not leader) nodes in cluster
                foreach (var server in nodes.Where(node => node != leaderServer))
                {
                    using (var store = new DocumentStore
                           {
                               Urls = new[] { server.WebUrl },
                               Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                               Database = databaseName
                           })
                    {
                        store.Initialize();

                        var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                        documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().BackupStatusFromMemoryOnly = true;
                        
                        PeriodicBackupStatus inMemoryStatus = null;
                        WaitForValue(() =>
                        {
                            inMemoryStatus = documentDatabase.PeriodicBackupRunner.GetBackupStatus(taskId);
                            return inMemoryStatus != null;
                        }, true);

                        var nextBackupTimeSpan = inMemoryStatus.DelayUntil - DateTime.UtcNow;
                        Assert.True(nextBackupTimeSpan > delayDuration.Subtract(TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds + 1_000)) &&
                                    nextBackupTimeSpan <= delayDuration,
                            $"NextBackup in: `{nextBackupTimeSpan}`, delayDuration with tolerance: `{delayDuration.Subtract(TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds + 1_000))}`, " +
                            $"delayDuration: `{delayDuration}`");
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ShouldDelayOnCurrentNodeIfClusterDown()
        {
            const int clusterSize = 3;

            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();

            var (nodes, leaderServer) = await CreateRaftCluster(clusterSize);
            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);
            
            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leaderServer.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
            })
            {
                leaderStore.Initialize();
                await Backup.FillClusterDatabaseWithRandomDataAsync(databaseSizeInMb: 10, leaderStore, clusterSize);
                
                var responsibleDatabase = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(leaderStore.Database).ConfigureAwait(false);
                Assert.NotNull(responsibleDatabase);
                responsibleDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = new TaskCompletionSource<object>();
                
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", mentorNode: leaderServer.ServerStore.NodeTag);
                var taskId = await Backup.UpdateConfigAndRunBackupAsync(leaderServer, config, leaderStore, opStatus: OperationStatus.InProgress);
                
                // Simulate Cluster Down state
                foreach (var node in nodes.Where(x => x != leaderServer))
                {
                   await DisposeAndRemoveServer(node);
                }
                
                // Let's delay the backup task to 1 hour
                var onGoingTaskInfo = await leaderStore.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;
                Assert.NotNull(onGoingTaskInfo);
                var delayDuration = TimeSpan.FromHours(1);
                var sw = Stopwatch.StartNew();
                var runningBackupTaskId = onGoingTaskInfo.OnGoingBackup.RunningBackupTaskId;
                await leaderStore.Maintenance.SendAsync(new DelayBackupOperation(runningBackupTaskId, delayDuration));

                // Check that there is no ongoing backup and new task scheduled properly
                onGoingTaskInfo = null;
                await WaitForValueAsync( async () =>
                {
                    onGoingTaskInfo = await leaderStore.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;
                    return onGoingTaskInfo is { OnGoingBackup: null };
                }, true);
                Assert.Null(onGoingTaskInfo.LastFullBackup);
                Assert.True(onGoingTaskInfo.NextBackup.TimeSpan > delayDuration.Subtract(TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds + 1_000)) &&
                            onGoingTaskInfo.NextBackup.TimeSpan <= delayDuration,
                    $"NextBackup in: `{onGoingTaskInfo.NextBackup.TimeSpan}`, delayDuration with tolerance: `{delayDuration.Subtract(TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds + 1_000))}`, " +
                    $"delayDuration: `{delayDuration}`");
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task NumberOfCurrentlyRunningBackupsShouldBeCorrectAfterBackupTaskDelay()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                    await Backup.FillDatabaseWithRandomDataAsync(databaseSizeInMb: 1, session);

                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                Assert.NotNull(database);
                database.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = new TaskCompletionSource<object>();

                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *");
                var taskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store, opStatus: OperationStatus.InProgress);

                // The backup task is running, and the next backup should be scheduled for the next minute (based on the backup configuration)
                var taskBackupInfo = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;
                Assert.NotNull(taskBackupInfo);
                Assert.NotNull(taskBackupInfo.OnGoingBackup);

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    AssertNumberOfConcurrentBackups(expectedNumber: 1);
                    
                    // Let's delay the backup task to 1 hour
                    var delayDuration = TimeSpan.FromHours(1);
                    await store.Maintenance.SendAsync(new DelayBackupOperation(taskBackupInfo.OnGoingBackup.RunningBackupTaskId, delayDuration));

                    AssertNumberOfConcurrentBackups(expectedNumber: 0);

                    void AssertNumberOfConcurrentBackups(int expectedNumber)
                    {
                        int concurrentBackups = WaitForValue( () => Server.ServerStore.ConcurrentBackupsCounter.CurrentNumberOfRunningBackups,
                            expectedVal: expectedNumber,
                            timeout: Convert.ToInt32(TimeSpan.FromMinutes(1).TotalMilliseconds),
                            interval: Convert.ToInt32(TimeSpan.FromSeconds(1).TotalMilliseconds));

                        Assert.Equal(expectedNumber, concurrentBackups);
                    }
                }
            }
        }
        
        [Fact, Trait("Category", "Smuggler")]
        public async Task ShouldRearrangeTheBackupTimer_IfItGot_ActiveByOtherNode_Then_ActiveByCurrentNode_WhileRunning()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options
            {
                Server = server
            }))
            {
                const string documentId = "Users/1";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Grisha" }, documentId);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);

                var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                Assert.NotNull(documentDatabase);
                var tcs = new TaskCompletionSource<object>();
                documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = tcs;
                try
                {
                    var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store, opStatus: OperationStatus.InProgress);
                    var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    var backups1 = record1.PeriodicBackups;
                    Assert.Equal(1, backups1.Count);

                    var taskId = backups1.First().TaskId;
                    var responsibleDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                    Assert.NotNull(responsibleDatabase);
                    var tag = responsibleDatabase.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                    Assert.Equal(server.ServerStore.NodeTag, tag);

                    responsibleDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateActiveByOtherNodeStatus_UpdateConfigurations = true;
                    responsibleDatabase.PeriodicBackupRunner.UpdateConfigurations(record1);
                    responsibleDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateActiveByOtherNodeStatus_UpdateConfigurations = false;
                    responsibleDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateActiveByCurrentNode_UpdateConfigurations = true;
                    responsibleDatabase.PeriodicBackupRunner.UpdateConfigurations(record1);
                    tcs.SetResult(null);

                    responsibleDatabase.PeriodicBackupRunner._forTestingPurposes = null;
                    var getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(taskId);
                    PeriodicBackupStatus status = null;
                    var val = WaitForValue(() =>
                    {
                        status = store.Maintenance.Send(getPeriodicBackupStatus).Status;
                        return status?.LastFullBackup != null;
                    }, true, timeout: 66666, interval: 444);
                    Assert.NotNull(status);
                    Assert.Null(status.Error);
                    Assert.True(val, "Failed to complete the backup in time");

                    var pb2 = responsibleDatabase.PeriodicBackupRunner.PeriodicBackups.FirstOrDefault();
                    Assert.NotNull(pb2);
                    Assert.True(pb2.HasScheduledBackup(), "Completed backup didn't schedule next one.");
                }
                finally
                {
                    try
                    {
                        tcs.TrySetResult(null);
                    }
                    catch
                    {
                        // ignored
                    }

                    documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().OnBackupTaskRunHoldBackupExecution = null;
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_restore_smuggler_with_escaped_quotes()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            const string docId = "\"users/1\"";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Grisha" }, docId);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(docId);
                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDatabaseEtag;
                await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: lastEtag);
            }

            using (var store = GetDocumentStore())
            {
                var backupDirectory = Directory.GetDirectories(backupPath).First();

                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), backupDirectory);
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(docId);
                    Assert.Null(user);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task can_backup_and_restore_cluster_transactions_with_document_collection_change()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                const string id = "users/1";
                const string country = "Israel";

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Grisha"
                    }, id);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    session.Delete("users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(new Address
                    {
                        Country = country
                    }, id);
                    await session.SaveChangesAsync();
                }

                await Backup.RunBackupAndReturnStatusAsync(Server, backupTaskId, store, isFullBackup: false, expectedEtag: 4);

                var databaseName = $"restored_database-{Guid.NewGuid()}";

                using (Backup.RestoreDatabase(store,
                           new RestoreBackupConfiguration
                           {
                               BackupLocation = Directory.GetDirectories(backupPath).First(),
                               DatabaseName = databaseName
                           }))
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var address = await session.LoadAsync<Address>(id);
                        Assert.NotNull(address);
                        Assert.Equal(country, address.Country);
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task BackupHistory_AssertEndpointsResponses()
        {
            const int clusterSize = 3;
            const string taskName = "RavenDB-19358 Backup history assert endpoints responses";

            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();

            var (_, leaderServer) = await CreateRaftCluster(clusterSize, shouldRunInMemory: false);

            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);

            using (var store = new DocumentStore
                   {
                Urls = new[] { leaderServer.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
                   })
            {
                store.Initialize();
                var documentDatabase = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).ConfigureAwait(false);

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: clusterSize - 1);

                    await session.StoreAsync(new User { Name = "RavenDB-19358" }, "user/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * 3 * *", mentorNode: leaderServer.ServerStore.NodeTag, name: taskName);
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var taskId = result.TaskId;

                var backupStatusesList = new List<PeriodicBackupStatus>
                {
                    await Backup.RunBackupAndReturnStatusAsync(leaderServer, taskId, store, isFullBackup: true),
                    await Backup.RunBackupAndReturnStatusAsync(leaderServer, taskId, store, isFullBackup: false)
                };

                using (leaderServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
                using (serverStoreContext.OpenReadTransaction())
                using (documentDatabase.ConfigurationStorage.ContextPool.AllocateOperationContext(out TransactionOperationContext configurationStorageContext))
                using (configurationStorageContext.OpenReadTransaction())
                {
                    var fromResponseDictionary = GetBackupHistoryFromEndpoint(serverStoreContext, store, documentDatabase.Name);
                    var detailsFromStorageDictionary =
                        documentDatabase.ConfigurationStorage.BackupHistoryStorage.ReadItems(configurationStorageContext, BackupHistoryItemType.Details);

                    Assert.Equal(2, fromResponseDictionary.Count);
                    Assert.Equal(2, detailsFromStorageDictionary.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var entryFromResponse = fromResponseDictionary.Values.ToArray()[i];
                        var status = backupStatusesList[i];
                        Assert.NotNull(status);

                        DateTime expectedCreatedAt;
                        if (entryFromResponse.IsFull)
                        {
                            Assert.True(status.LastFullBackup.HasValue);
                            expectedCreatedAt = status.LastFullBackup.Value;
                        }
                        else
                        {
                            Assert.True(status.LastIncrementalBackup.HasValue);
                            expectedCreatedAt = status.LastIncrementalBackup.Value;
                        }

                        Assert.Equal(status.BackupType, entryFromResponse.BackupType);
                        Assert.Equal(expectedCreatedAt, entryFromResponse.CreatedAt);
                        Assert.Equal(databaseName, entryFromResponse.DatabaseName);
                        Assert.Equal(status.DurationInMs, entryFromResponse.DurationInMs);
                        Assert.Equal(status.Error?.Exception, entryFromResponse.Error);
                        Assert.Equal(status.IsFull, entryFromResponse.IsFull);
                        Assert.Equal(status.NodeTag, entryFromResponse.NodeTag);
                        Assert.Equal(status.LastFullBackup, entryFromResponse.LastFullBackup);
                        Assert.Equal(status.TaskId, entryFromResponse.TaskId);

                        var details = GetDetailsFromEndpoint(serverStoreContext, entryFromResponse);

                        details.TryGet(nameof(BackupResult.Documents), out BlittableJsonReaderObject documents);
                        documents.TryGet(nameof(BackupResult.Documents.Processed), out bool docsProcessed);
                        documents.TryGet(nameof(BackupResult.Documents.ReadCount), out long docsReadCount);

                        details.TryGet(nameof(BackupResult.LocalBackup), out BlittableJsonReaderObject localBackup);
                        localBackup.TryGet(nameof(BackupResult.LocalBackup.BackupDirectory), out string backupDirectory);
                        var expectedBackupDirectory =
                            $"{entryFromResponse.CreatedAt.ToLocalTime().ToString(BackupTask.DateTimeFormat, CultureInfo.InvariantCulture)}.ravendb-{entryFromResponse.DatabaseName}-{entryFromResponse.NodeTag}-{entryFromResponse.BackupType.ToString().ToLower()}";

                        if (entryFromResponse.IsFull)
                        {
                            Assert.True(docsProcessed);
                            Assert.Equal(1, docsReadCount);
                            Assert.Equal(expectedBackupDirectory, backupDirectory);
                        }
                        else
                        {
                            Assert.True(docsProcessed);
                            Assert.Equal(0, docsReadCount);
                            Assert.Null(backupDirectory);
                        }

                        var taskNameFromResponse = fromResponseDictionary.Keys.ToArray()[i].TaskName;
                        Assert.Equal(taskName, taskNameFromResponse);
                    }
                }

                BlittableJsonReaderObject GetDetailsFromEndpoint(JsonOperationContext context, BackupHistoryEntry backupHistoryEntry)
                {
                    var client = store.GetRequestExecutor().HttpClient;
                    var response = AsyncHelpers.RunSync(() =>
                        client.GetAsync(
                            $"{store.Urls.First()}/databases/{backupHistoryEntry.DatabaseName}/backup-history-details?taskId={backupHistoryEntry.TaskId}&id={backupHistoryEntry.CreatedAt.Ticks}"));
                    string result = response.Content.ReadAsStringAsync().Result;

                    var detailsFromResponse = context.Sync.ReadForMemory(result, "Result");
                    detailsFromResponse.TryGet(nameof(BackupHistoryItemType.Details), out BlittableJsonReaderObject details);

                    return details;
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task BackupHistory_ShouldStoreFaultedBackups()
        {
            const int clusterSize = 3;
            const string taskName = "RavenDB-19358 Backup History should store faulted backups";
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();
            var expectedError = new Exception(nameof(DocumentDatabase.PeriodicBackupRunner._forTestingPurposes.SimulateFailedBackup)).ToString();

            var (_, leaderServer) = await CreateRaftCluster(clusterSize, shouldRunInMemory: false);

            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);

            using (var store = new DocumentStore
            {
                Urls = new[] { leaderServer.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
            })
            {
                store.Initialize();
                var documentDatabase = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).ConfigureAwait(false);

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: clusterSize - 1);

                    await session.StoreAsync(new User { Name = "RavenDB-19358" }, "user/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * 3 * *", mentorNode: leaderServer.ServerStore.NodeTag, name: taskName);
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var taskId = result.TaskId;
                documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateFailedBackup = true;

                await Backup.RunBackupAsync(leaderServer, taskId, store, isFullBackup: true, OperationStatus.Faulted);

                using (leaderServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
                using (serverStoreContext.OpenReadTransaction())
                {
                    Dictionary<(DateTime, bool IsFull, string TaskName), BackupHistoryEntry> fromResponseDictionary = default;
                    WaitForValue(() =>
                    {
                        fromResponseDictionary = GetBackupHistoryFromEndpoint(serverStoreContext, store, documentDatabase.Name);

                        return fromResponseDictionary.Count;
                    }, 1);

                    var entryPeriodical = fromResponseDictionary.Single();
                    
                    Assert.Equal(BackupType.Backup, entryPeriodical.Value.BackupType);
                    Assert.True(entryPeriodical.Key.IsFull);
                    Assert.Equal(entryPeriodical.Key.TaskName, taskName);
                    Assert.True(entryPeriodical.Value.Error.Contains(expectedError));

                    using (documentDatabase.ConfigurationStorage.ContextPool.AllocateOperationContext(out TransactionOperationContext configurationStorageContext))
                    using (configurationStorageContext.OpenReadTransaction())
                    {
                        var actualDetailsCount = documentDatabase.ConfigurationStorage.BackupHistoryStorage.ReadItems(configurationStorageContext, BackupHistoryItemType.Details).Count;
                        Assert.Equal(1, actualDetailsCount);
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task BackupHistory_ShouldTakeIntoAccountLimitConfiguration_ShouldDeleteOldestEntriesFirst()
        {
            const int historySizeLimit = 1;
            const int clusterSize = 3;
            const string taskName = "RavenDB-19358 Backup History should take into account limit configuration should delete oldest entries first";

            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();

            var (_, leaderServer) = await CreateRaftCluster(clusterSize, shouldRunInMemory: false, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Backup.MaxNumberOfFullBackupsInBackupHistory)] = historySizeLimit.ToString()
            });

            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);

            using (var store = new DocumentStore
            {
                Urls = new[] { leaderServer.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
            })
            {
                store.Initialize();
                var documentDatabase = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).ConfigureAwait(false);

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: clusterSize - 1);

                    await session.StoreAsync(new User { Name = "RavenDB-19358" }, "user/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * 3 * *", mentorNode: leaderServer.ServerStore.NodeTag, name: taskName);
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var taskId = result.TaskId;

                (DateTime, bool IsFull, string TaskName) entryKeyToRemove = default;
                var detailsKeysToRemove = new List<string>();
                for (int i = 1; i <= historySizeLimit + 1; i++)
                {
                    var backupPlan = new[] { true, false };
                    var countFullBackups = backupPlan.Count(isFull => isFull);
                    var expectedFullBackupCount = countFullBackups <= historySizeLimit ? countFullBackups : historySizeLimit;

                    await RunBackupsAccordingPlan(backupPlan, leaderServer, taskId, store);

                    using (leaderServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
                    using (serverStoreContext.OpenReadTransaction())
                    using (documentDatabase.ConfigurationStorage.ContextPool.AllocateOperationContext(out TransactionOperationContext configurationStorageContext))
                    using (configurationStorageContext.OpenReadTransaction())
                    {
                        int actualFullBackupsCount;
                        Dictionary<(DateTime, bool IsFull, string TaskName), BackupHistoryEntry> fromResponseDictionary = default;
                        Dictionary<string, BlittableJsonReaderObject> detailsFromStorageDictionary = default;
                        switch (i)
                        {
                            case 1:
                                WaitForValue(() =>
                                {
                                    fromResponseDictionary = GetBackupHistoryFromEndpoint(serverStoreContext, store, documentDatabase.Name);
                                    detailsFromStorageDictionary = documentDatabase.ConfigurationStorage.BackupHistoryStorage.ReadItems(configurationStorageContext, BackupHistoryItemType.Details);
                                    return fromResponseDictionary.Count == backupPlan.Length && detailsFromStorageDictionary.Count == backupPlan.Length;
                                }, true);
                                actualFullBackupsCount = fromResponseDictionary.Count(entry => entry.Key.IsFull);
                                Assert.Equal(expectedFullBackupCount, actualFullBackupsCount);

                                entryKeyToRemove = fromResponseDictionary.First().Key;
                                Assert.True(entryKeyToRemove.IsFull);

                                detailsKeysToRemove.AddRange(detailsFromStorageDictionary.Keys);
                                break;

                            case historySizeLimit + 1:
                                WaitForValue(() =>
                                {
                                    fromResponseDictionary = GetBackupHistoryFromEndpoint(serverStoreContext, store, documentDatabase.Name);
                                    detailsFromStorageDictionary = documentDatabase.ConfigurationStorage.BackupHistoryStorage.ReadItems(configurationStorageContext, BackupHistoryItemType.Details);
                                    return fromResponseDictionary.Count == historySizeLimit * backupPlan.Length &&
                                           detailsFromStorageDictionary.Count == historySizeLimit * backupPlan.Length;
                                }, true);

                                actualFullBackupsCount = fromResponseDictionary.Count(entry => entry.Key.IsFull);
                                Assert.Equal(historySizeLimit, actualFullBackupsCount);
                                Assert.False(fromResponseDictionary.ContainsKey(entryKeyToRemove));

                                detailsKeysToRemove.ForEach(key => Assert.False(detailsFromStorageDictionary.ContainsKey(key)));
                                break;

                            default:
                                WaitForValue(() =>
                                {
                                    fromResponseDictionary = GetBackupHistoryFromEndpoint(serverStoreContext, store, documentDatabase.Name);
                                    detailsFromStorageDictionary = documentDatabase.ConfigurationStorage.BackupHistoryStorage.ReadItems(configurationStorageContext, BackupHistoryItemType.Details);
                                    return fromResponseDictionary.Count == i * backupPlan.Length && detailsFromStorageDictionary.Count == i * backupPlan.Length;
                                }, true);

                                actualFullBackupsCount = fromResponseDictionary.Count(entry => entry.Key.IsFull);
                                Assert.Equal(i, actualFullBackupsCount);
                                break;
                        }
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task BackupHistory_ShouldStorePersistently_BackupHistoryEntriesInTemporaryStorageAndDetails()
        {
            const int clusterSize = 3;
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();

            var (nodes, leaderServer) = await CreateRaftCluster(clusterSize, shouldRunInMemory: false);
            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leaderServer.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
            })
            {
                leaderStore.Initialize();

                using (var session = leaderStore.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: clusterSize - 1);

                    await session.StoreAsync(new User { Name = "RavenDB-19358" }, "user/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * 3 * *", mentorNode: leaderServer.ServerStore.NodeTag, name: "RavenDB-19358");
                var taskId = await Backup.UpdateConfigAndRunBackupAsync(leaderServer, config, leaderStore);

                // Simulate cluster down state
                var listOfNodeDisposeResults = new List<(string, string, string)>();
                foreach (var node in nodes.Where(x => x != leaderServer))
                    listOfNodeDisposeResults.Add(await DisposeServerAndWaitForFinishOfDisposalAsync(node));

                var requestExecutor = leaderStore.GetRequestExecutor(databaseName);
                var getTopologyCommand = new GetClusterTopologyCommand(leaderServer.DebugTag);
                using (leaderServer.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    await WaitForValueAsync(async () =>
                    {
                        await requestExecutor.ExecuteAsync(getTopologyCommand, context);
                        return getTopologyCommand.Result.Status.Any(nodeStatus => nodeStatus.Value.Connected);
                    }, false);
                }
                
                // In the current state backup history entry will be stored in the temporary storage
                await Backup.RunBackupAsync(leaderServer, taskId, leaderStore, isFullBackup: false);

                var documentDatabase = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).ConfigureAwait(false);
                using (leaderServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
                using (serverStoreContext.OpenReadTransaction())
                using (documentDatabase.ConfigurationStorage.ContextPool.AllocateOperationContext(out TransactionOperationContext configurationStorageContext))
                using (configurationStorageContext.OpenReadTransaction())
                    WaitForValue(() => documentDatabase.ConfigurationStorage.BackupHistoryStorage.ReadItems(configurationStorageContext, BackupHistoryItemType.HistoryEntry).Count, 1);

                // Restart leader server
                var leaderDisposingResult = await DisposeServerAndWaitForFinishOfDisposalAsync(leaderServer);
                var newLeaderServer = CreateServerFromDisposeResult(leaderDisposingResult);
                listOfNodeDisposeResults.ForEach(disposeResult => CreateServerFromDisposeResult(disposeResult));

                using (var store = new DocumentStore
                {
                    Urls = new[] { newLeaderServer.WebUrl },
                    Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                    Database = databaseName
                })
                {
                    store.Initialize();

                    await WaitForValueAsync(async () =>
                    {
                        documentDatabase = await newLeaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).ConfigureAwait(false);
                        if (documentDatabase == null)
                            return false;

                        using (documentDatabase.ConfigurationStorage.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var entriesFromTemporaryStorage = documentDatabase.ConfigurationStorage.BackupHistoryStorage.ReadItems(context, BackupHistoryItemType.HistoryEntry);
                            return entriesFromTemporaryStorage != null;
                        }
                    }, true);

                    // Assertion
                    using (newLeaderServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
                    using (serverStoreContext.OpenReadTransaction())
                    using (documentDatabase.ConfigurationStorage.ContextPool.AllocateOperationContext(out TransactionOperationContext configurationStorageContext))
                    using (configurationStorageContext.OpenReadTransaction())
                    {
                        var entriesFromClusterDictionary = GetBackupHistoryFromClusterStorage(serverStoreContext, newLeaderServer, documentDatabase.Name);
                        var entriesFromTemporaryDictionary = GetBackupHistoryFromTemporaryStorage(
                            documentDatabase.ConfigurationStorage.BackupHistoryStorage.ReadItems(configurationStorageContext, BackupHistoryItemType.HistoryEntry));
                        var fromResponseDictionary = GetBackupHistoryFromEndpoint(serverStoreContext, store, documentDatabase.Name);
                        var actualDetailsCount = documentDatabase.ConfigurationStorage.BackupHistoryStorage.ReadItems(configurationStorageContext, BackupHistoryItemType.Details).Count;

                        Assert.Equal(1, entriesFromClusterDictionary.Count);
                        Assert.Equal(1, entriesFromTemporaryDictionary.Count);
                        Assert.Equal(2, fromResponseDictionary.Count);
                        Assert.Equal(2, actualDetailsCount);
                    }
                }

                RavenServer CreateServerFromDisposeResult((string DataDirectory, string Url, string NodeTag) disposeResult)
                {
                    var newServer = GetNewServer(new ServerCreationOptions
                    {
                        DeletePrevious = false,
                        RunInMemory = false,
                        DataDirectory = disposeResult.DataDirectory,
                        CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = disposeResult.Url, }
                    });
                    Assert.NotNull(newServer);
                    return newServer;
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task BackupHistory_ShouldGetBackupHistory_FromEachNode_AboutEachNode()
        {
            const string backupName = "RavenDB-19358";
            const BackupType backupType = BackupType.Backup;
            const int clusterSize = 3;
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();

            var (nodes, leaderServer) = await CreateRaftCluster(clusterSize, customSettings: new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Backup.MaxNumberOfFullBackupsInBackupHistory)] = "2"
            });
            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);

            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leaderServer.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
            })
            {
                leaderStore.Initialize();

                using var session = leaderStore.OpenAsyncSession();
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: clusterSize - 1);

                await session.StoreAsync(new User { Name = backupName }, "user/1");
                await session.SaveChangesAsync();
            }

            var backupPlan = new[] { true, false, false, true };
            var expectedNodeTagOrder = new List<string>();
            var expectedCountOfBackupEntries = backupPlan.Length * clusterSize;

            foreach (var node in nodes)
            {
                using var store = new DocumentStore
                {
                    Urls = new[] { node.WebUrl },
                    Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                    Database = databaseName
                };
                store.Initialize();

                var config = Backup.CreateBackupConfiguration(
                    backupPath: backupPath,
                    backupType: backupType,
                    name: $"{backupName}-{node.ServerStore.NodeTag}",
                    mentorNode: node.ServerStore.NodeTag);
                var result = store.Maintenance.Send(new UpdatePeriodicBackupOperation(config));
                var taskId = result.TaskId;
                expectedNodeTagOrder.Add(node.ServerStore.NodeTag);

                await RunBackupsAccordingPlan(backupPlan, node, taskId, store);
            }

            foreach (var node in nodes)
            {
                using var store = new DocumentStore
                {
                    Urls = new[] { node.WebUrl }, Conventions = new DocumentConventions { DisableTopologyUpdates = true }, Database = databaseName
                };
                store.Initialize();
                var documentDatabase = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).ConfigureAwait(false);

                Dictionary<(DateTime, bool, string TaskName), BackupHistoryEntry> fromResponseDictionary = default;
                Dictionary<(DateTime, bool, string TaskName), BackupHistoryEntry> entriesFromClusterDictionary = default;

                using (leaderServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
                using (serverStoreContext.OpenReadTransaction())
                    WaitForValue(() =>
                    {
                        fromResponseDictionary = GetBackupHistoryFromEndpoint(serverStoreContext, store, documentDatabase.Name);
                        entriesFromClusterDictionary = GetBackupHistoryFromClusterStorage(serverStoreContext, node, documentDatabase.Name);

                        return fromResponseDictionary.Count == expectedCountOfBackupEntries && entriesFromClusterDictionary.Count == expectedCountOfBackupEntries;
                    }, true);

                for (int i = 0; i < expectedCountOfBackupEntries; i++)
                {
                    var index = (int)Math.Ceiling((double)(i + 1) / backupPlan.Length) - 1;
                    var expectedNodeTag = expectedNodeTagOrder[index];
                    var responseNodeTag = fromResponseDictionary.Values.ToList()[i].NodeTag;
                    var storageNodeTag = entriesFromClusterDictionary.Values.ToList()[i].NodeTag;

                    Assert.Equal(expectedNodeTag, responseNodeTag);
                    Assert.Equal(expectedNodeTag, storageNodeTag);

                    var expectedTaskName = $"{backupName}-{expectedNodeTag}";
                    var responseTaskName = fromResponseDictionary.Keys.ToList()[i].TaskName;
                    var storageTaskName = entriesFromClusterDictionary.Keys.ToList()[i].TaskName;

                    Assert.Equal(expectedTaskName, responseTaskName);
                    Assert.Equal(expectedTaskName, storageTaskName);
                }

                using (documentDatabase.ConfigurationStorage.ContextPool.AllocateOperationContext(out TransactionOperationContext configurationStorageContext))
                using (configurationStorageContext.OpenReadTransaction())
                {
                    var actualDetailsCount = documentDatabase.ConfigurationStorage.BackupHistoryStorage.ReadItems(configurationStorageContext, BackupHistoryItemType.Details).Count;
                    Assert.Equal(backupPlan.Length, actualDetailsCount);
                }
            }
        }

        [Theory, Trait("Category", "Smuggler")]
        // It matters which backup was made first after the cluster down. We need to test all scenarios.
        [InlineData(new[] { true, false, false, true }, new[] { false, true })]
        [InlineData(new[] { true, false, true, true, false }, new[] { true, false })]
        [InlineData(new bool[] { }, new[] { true, false, true, false, false })]
        public async Task BackupHistory_IfClusterDown_ShouldCorrectStore_ShouldGiveCorrectResponseOnRequest(bool[] backupPlanBeforeClusterDown, bool[] backupPlanAfterClusterDown)
        {
            const int clusterSize = 3;

            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();

            var (nodes, leaderServer) = await CreateRaftCluster(clusterSize);
            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);

            using (var leaderStore = new DocumentStore
                   {
                Urls = new[] { leaderServer.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
                   })
            {
                leaderStore.Initialize();

                using (var session = leaderStore.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: clusterSize - 1);

                    await session.StoreAsync(new User { Name = "RavenDB-19358" }, "user/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * 3 * *", mentorNode: leaderServer.ServerStore.NodeTag, name: "RavenDB-19358");
                var result = leaderStore.Maintenance.Send(new UpdatePeriodicBackupOperation(config));
                var taskId = result.TaskId;

                await RunBackupsAccordingPlan(backupPlanBeforeClusterDown, leaderServer, taskId, leaderStore);

                // Simulate Cluster Down state
                List<(string DataDirectory, string Url, string NodeTag)> disposeResults = new();
                foreach (var node in nodes.Where(x => x != leaderServer))
                {
                    var disposingResult = await DisposeServerAndWaitForFinishOfDisposalAsync(node);
                    disposeResults.Add(disposingResult);
                }

                var requestExecutor = leaderStore.GetRequestExecutor(databaseName);
                var getTopologyCommand = new GetClusterTopologyCommand(leaderServer.DebugTag);
                using (leaderServer.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    await WaitForValueAsync(async () =>
                    {
                        await requestExecutor.ExecuteAsync(getTopologyCommand, context);
                        return getTopologyCommand.Result.Status.Any(nodeStatus => nodeStatus.Value.Connected);
                    }, false);
                }

                await RunBackupsAccordingPlan(backupPlanAfterClusterDown, leaderServer, taskId, leaderStore);

                var documentDatabase = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                var generalBackupPlan = backupPlanBeforeClusterDown.Concat(backupPlanAfterClusterDown).ToList();
                var backupCount = generalBackupPlan.Count;

                CollectAndAssertBackupHistoryData(
                    expectedCountOfEntriesInClusterStorage: backupPlanBeforeClusterDown.Length,
                    expectedCountOfEntriesInTemporaryStorage: backupPlanAfterClusterDown.Length);

                // Restore previously disposed cluster's nodes
                foreach ((string dataDirectory, string url, _) in disposeResults)
                {
                    var newServer = GetNewServer(new ServerCreationOptions
                    {
                        DeletePrevious = false,
                        RunInMemory = false,
                        DataDirectory = dataDirectory,
                        CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = url, }
                    });
                    Assert.NotNull(newServer);
                    nodes.Add(newServer);
                }

                await Task.Delay(3000);

                generalBackupPlan.Add(false);
                await Backup.RunBackupAsync(leaderServer, taskId, leaderStore, isFullBackup: generalBackupPlan.Last());
                backupCount++;

                CollectAndAssertBackupHistoryData(
                    expectedCountOfEntriesInClusterStorage: backupCount,
                    expectedCountOfEntriesInTemporaryStorage: 0);

                void CollectAndAssertBackupHistoryData(int expectedCountOfEntriesInClusterStorage, int expectedCountOfEntriesInTemporaryStorage)
                {
                    Dictionary<(DateTime, bool IsFull, string), BackupHistoryEntry> fromResponseDictionary;
                    Dictionary<(DateTime, bool, string), BackupHistoryEntry> entriesFromClusterDictionary;
                    Dictionary<(DateTime, bool, string), BackupHistoryEntry> entriesFromTemporaryDictionary;

                    using (leaderServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
                    using (serverStoreContext.OpenReadTransaction())
                    {
                        fromResponseDictionary = GetBackupHistoryFromEndpoint(serverStoreContext, leaderStore, documentDatabase.Name);
                        entriesFromClusterDictionary = GetBackupHistoryFromClusterStorage(serverStoreContext, leaderServer, documentDatabase.Name);
                    }

                    using (documentDatabase.ConfigurationStorage.ContextPool.AllocateOperationContext(out TransactionOperationContext configurationStorageContext))
                    using (configurationStorageContext.OpenReadTransaction())
                    {
                        entriesFromTemporaryDictionary = GetBackupHistoryFromTemporaryStorage(
                            documentDatabase.ConfigurationStorage.BackupHistoryStorage.ReadItems(configurationStorageContext, BackupHistoryItemType.HistoryEntry));
                        var actualDetailsCount = documentDatabase.ConfigurationStorage.BackupHistoryStorage.ReadItems(configurationStorageContext, BackupHistoryItemType.Details).Count;
                        Assert.Equal(expectedCountOfEntriesInClusterStorage + expectedCountOfEntriesInTemporaryStorage, actualDetailsCount);
                    }

                    var fromStoragesDictionary = new Dictionary<(DateTime, bool, string), BackupHistoryEntry>();
                    foreach (var entry in entriesFromClusterDictionary)
                        fromStoragesDictionary.Add(entry.Key, entry.Value);
                    foreach (var entry in entriesFromTemporaryDictionary)
                        fromStoragesDictionary.Add(entry.Key, entry.Value);

                    // Asserts
                    Assert.Equal(expectedCountOfEntriesInClusterStorage, entriesFromClusterDictionary.Count);
                    Assert.Equal(expectedCountOfEntriesInTemporaryStorage, entriesFromTemporaryDictionary.Count);

                    Assert.Equal(backupCount, fromStoragesDictionary.Count);
                    Assert.Equal(backupCount, fromResponseDictionary.Count);

                    for (int i = 0; i < backupCount; i++)
                    {
                        var entryFromStorage = fromStoragesDictionary.Values.ToList()[i];
                        var entryFromResponse = fromResponseDictionary.Values.ToList()[i];
                        var writtenAsFullBackup = fromResponseDictionary.Keys.ToList()[i].IsFull;
                        var expectedIsFull = generalBackupPlan[i];

                        Assert.Equal(entryFromStorage, entryFromResponse);
                        Assert.Equal(expectedIsFull, writtenAsFullBackup);
                    }
                }
            }
        }

        private static string GetBackupPath(IDocumentStore store, long backTaskId, bool incremental = true)
        {
            var status = store.Maintenance.Send(new GetPeriodicBackupStatusOperation(backTaskId)).Status;

            var backupDirectory = status.LocalBackup.BackupDirectory;

            string datePrefix;
            if (incremental)
            {
                Debug.Assert(status.LastIncrementalBackup.HasValue);
                datePrefix = status.LastIncrementalBackup.Value.ToLocalTime().ToString(BackupTask.DateTimeFormat);
            }
            else
            {
                var folderName = status.FolderName;
                var indexOf = folderName.IndexOf(".", StringComparison.OrdinalIgnoreCase);
                Debug.Assert(indexOf != -1);
                datePrefix = folderName.Substring(0, indexOf);
            }

            var fileExtension = incremental
                ? Constants.Documents.PeriodicBackup.IncrementalBackupExtension
                : Constants.Documents.PeriodicBackup.FullBackupExtension;

            return Path.Combine(backupDirectory, $"{datePrefix}{fileExtension}");
        }

        private static IDisposable ReadOnly(string path)
        {
            var files = Directory.GetFiles(path);
            var attributes = new FileInfo(files[0]).Attributes;
            foreach (string file in files)
            {
                File.SetAttributes(file, FileAttributes.ReadOnly);
            }

            return new DisposableAction(() =>
            {
                foreach (string file in files)
                {
                    File.SetAttributes(file, attributes);
                }
            });
        }


        public IDocumentStore RestoreAndGetStore(IDocumentStore store, string backupPath, out IDisposable releaseDatabase, TimeSpan? timeout = null)
        {
            var restoredDatabaseName = GetDatabaseName();

            releaseDatabase = Backup.RestoreDatabase(store, new RestoreBackupConfiguration
            {
                BackupLocation = Directory.GetDirectories(backupPath).First(),
                DatabaseName = restoredDatabaseName
            }, timeout);

            return GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => restoredDatabaseName,
                CreateDatabase = false,
                DeleteDatabaseOnDispose = true
            });
        }
    }
}
