using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Basic.Entities;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class PeriodicBackupTestsSlow : ClusterTestBase
    {
        public PeriodicBackupTestsSlow(ITestOutputHelper output) : base(output)
        {
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

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };
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
                var periodicBackupRunner = (await GetDocumentDatabaseInstanceFor(store)).PeriodicBackupRunner;

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
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);
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
                var periodicBackupRunner = (await GetDocumentDatabaseInstanceFor(store)).PeriodicBackupRunner;

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
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren 2" }, "users/2");
                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);
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

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);
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

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                var backupStatus = await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete("users/1");
                    await session.SaveChangesAsync();
                }

                Operation<StartBackupOperationResult> newBackupStatus;
                do
                {
                    newBackupStatus = await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                }
                //Race condition between reading the backup status and creating new backup
                while (newBackupStatus.Id == backupStatus.Id);

                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, 2);
                Assert.Equal(2, value);
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

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, "users/2");
                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);
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

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
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

                OperationState backupOperation = null;
                var status = WaitForValue(() =>
                {
                    backupOperation = store.Maintenance.Send(new GetOperationStateOperation(backupOperationId.Value));
                    return backupOperation.Status;
                }, OperationStatus.Completed);
                Assert.Equal(OperationStatus.Completed, status);

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

                var backupLocation = Directory.GetDirectories(backupPath).First();

                using (ReadOnly(backupLocation))
                using (RestoreDatabase(store, new RestoreBackupConfiguration
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

        [Theory]
        [InlineData("* * * * *", null)]
        [InlineData(null, "* * * * *")]
        [InlineData("0 0 1 * *", null)]
        [InlineData(null, "0 0 1 * *")]
        public async Task next_full_backup_time_calculated_correctly(string fullBackupFrequency, string incrementalBackupFrequency)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    FullBackupFrequency = fullBackupFrequency,
                    IncrementalBackupFrequency = incrementalBackupFrequency
                };

                var backup = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
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

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    SnapshotSettings = new SnapshotSettings
                    {
                        CompressionLevel = compressionLevel
                    },
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
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
                var backupLocation = Directory.GetDirectories(backupPath).First();
                using (ReadOnly(backupLocation))
                using (RestoreDatabase(store, new RestoreBackupConfiguration
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
                        Assert.Contains($"A:10-{restoredDatabase.DbBase64Id}", databaseChangeVector);
                    }
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

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 2);
                Assert.Equal(2, value);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                using (RestoreDatabase(store, new RestoreBackupConfiguration
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
                var baseline = DateTime.Today;
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

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 6);
                Assert.Equal(6, value);

                var backupStatus = store.Maintenance.Send(operation);
                var backupOperationId = backupStatus.Status.LastOperationId;

                var backupOperation = store.Maintenance.Send(new GetOperationStateOperation(backupOperationId.Value));

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
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                using (RestoreDatabase(store, new RestoreBackupConfiguration
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
                var baseline = DateTime.Today;

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

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 6);
                Assert.Equal(6, value);

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
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);

                // restore the database with a different name
                string restoredDatabaseName = $"restored_database_snapshot-{Guid.NewGuid()}";
                using (RestoreDatabase(store, new RestoreBackupConfiguration
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

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    LocalSettings = new LocalSettings { FolderPath = backupPath },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                await WaitForValueAsync(async () =>
                {
                    var status = (await store.Maintenance.SendAsync(operation)).Status;
                    return status != null;
                }, true);

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

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    LocalSettings = new LocalSettings { FolderPath = backupPath },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                var op = await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                await op.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                long? statusLastEtag = null;
                await WaitForValueAsync(async () =>
                {
                    var status = (await store.Maintenance.SendAsync(operation)).Status;
                    if (status == null)
                        return false;
                    statusLastEtag = status.LastEtag;
                    return statusLastEtag > 0;
                }, true);

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
                op = await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                await op.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Patch<User, string>(entity.Id, u => u.Name, "Patched");
                    await session.SaveChangesAsync();
                }
                op = await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                await op.WaitForCompletionAsync(TimeSpan.FromSeconds(15));

                await WaitForValueAsync(async () =>
                {
                    var status = (await store.Maintenance.SendAsync(operation)).Status;
                    return status != null && status.LastEtag > statusLastEtag;
                }, true);

                await RestoreAndCheckTimeSeriesConfiguration(store, backupPath, timeSeriesConfiguration);
            }
        }

        private async Task RestoreAndCheckTimeSeriesConfiguration(IDocumentStore store, string backupPath, TimeSeriesConfiguration timeSeriesConfiguration)
        {
            string restoredDatabaseName = $"{store.Database}-restored";
            using (RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = Directory.GetDirectories(backupPath).First(), DatabaseName = restoredDatabaseName }))
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

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);

                restoreConfiguration.BackupLocation = backupPath;
                restoreConfiguration.DataDirectory = backupPath;
                restoreBackupTask = new RestoreBackupOperation(restoreConfiguration);
                e = Assert.Throws<RavenException>(() => store.Maintenance.Server.Send(restoreBackupTask));
                Assert.Contains("New data directory must be empty of any files or folders", e.InnerException.Message);

                // perform restore with a valid db name
                var emptyFolder = NewDataPath(suffix: "BackupFolderRestore123");
                var validDbName = "日本語-שלום-cześć_Привет.123";

                using (RestoreDatabase(store, new RestoreBackupConfiguration
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

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 8);
                Assert.Equal(8, value);

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
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);

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
                var baseline = DateTime.Today;

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

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 6);
                Assert.Equal(6, value);

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
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);

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

        [Fact]
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

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };
                var operation = new UpdatePeriodicBackupOperation(config);
                var result = await store.Maintenance.SendAsync(operation);
                var periodicBackupTaskId = result.TaskId;

                await WaitForRaftIndexToBeAppliedInCluster(periodicBackupTaskId, TimeSpan.FromSeconds(15));

                await store.Maintenance.SendAsync(new StartBackupOperation(true, result.TaskId));

                var getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(periodicBackupTaskId);
                var done = SpinWait.SpinUntil(() => store.Maintenance.Send(getPeriodicBackupStatus).Status?.LastFullBackup != null, TimeSpan.FromSeconds(180));
                Assert.True(done, "Failed to complete the backup in time");

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

                backupInfoResult = await store.Maintenance.SendAsync(backupInfo);
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupInfoResult.TaskId));

                getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(periodicBackupTaskId);
                done = SpinWait.SpinUntil(() => store.Maintenance.Send(getPeriodicBackupStatus).Status?.LastFullBackup != null, TimeSpan.FromSeconds(180));
                Assert.True(done, "Failed to complete the backup in time");

                backupInfoResult = await store.Maintenance.SendAsync(backupInfo);
                Assert.Equal(originalNode, backupInfoResult.ResponsibleNode.NodeTag);
            }
        }

        [Fact]
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

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "* */6 * * *",
                    BackupType = BackupType.Backup
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP

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

        [Fact]
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

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings { FolderPath = backupPath },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "users/2" }, "users/2");
                    await session.SaveChangesAsync();
                }

                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);

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
                await store.Maintenance.SendAsync(new StartBackupOperation(false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LastEtag, lastEtag);
                Assert.Equal(lastEtag, value);

                var databaseName = GetDatabaseName() + "restore";

                using (RestoreDatabase(
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

        [Fact]
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

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings { FolderPath = backupPath },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);

                await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: false, backupTaskId));
                value = WaitForValue(() => store.Maintenance.Send(operation).Status.LocalBackup.IncrementalBackupDurationInMs, 0);
                Assert.Equal(0, value);

                string backupFolder = Directory.GetDirectories(backupPath).OrderBy(Directory.GetCreationTime).Last();
                var backupsToRestore = Directory.GetFiles(backupFolder).Where(BackupUtils.IsBackupFile)
                    .OrderBackups()
                    .ToArray();

                Assert.Equal(1, backupsToRestore.Length);

                var databaseName = GetDatabaseName() + "restore";

                using (RestoreDatabase(
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

        [Fact]
        public async Task FirstBackupWithClusterDownStatusShouldRearrangeTheTimer()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore(new Options { DeleteDatabaseOnDispose = true, Path = NewDataPath() }))
            {
                var documentDatabase = await GetDatabase(store.Database);
                documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateClusterDownStatus = true;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGR" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var operation = new UpdatePeriodicBackupOperation(config);
                var result = await store.Maintenance.SendAsync(operation);
                var periodicBackupTaskId = result.TaskId;
                var val = WaitForValue(() => documentDatabase.PeriodicBackupRunner._forTestingPurposes.ClusterDownStatusSimulated, true, timeout: 66666, interval: 333);
                Assert.True(val, "Failed to simulate ClusterDown Status");
                documentDatabase.PeriodicBackupRunner._forTestingPurposes = null;
                var getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(periodicBackupTaskId);
                val = WaitForValue(() => store.Maintenance.Send(getPeriodicBackupStatus).Status?.LastFullBackup != null, true, timeout: 66666, interval: 333);
                Assert.True(val, "Failed to complete the backup in time");
            }
        }

        [Fact]
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
                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    LocalSettings = localSettings,
                    IncrementalBackupFrequency = "0 0 1 1 *"
                };

                var backupTaskId = (store.Maintenance.Send(new UpdatePeriodicBackupOperation(config))).TaskId;
                store.Maintenance.Send(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                PeriodicBackupStatus status = null;
                var value = WaitForValue(() =>
                {
                    status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 4);
                Assert.True(4 == value, $"4 == value, Got status: {status != null}, exception: {status?.Error?.Exception}");
                Assert.True(status.LastOperationId != null, $"status.LastOperationId != null, Got status: {status != null}, exception: {status?.Error?.Exception}");

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
                        Assert.Equal($"A:4-{originalDatabase.DbBase64Id}", databaseChangeVector);
                    }
                }
            }
        }

        [Fact]
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

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "0 0 1 1 *",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupEncryptionSettings = new BackupEncryptionSettings()
                    {
                        EncryptionMode = EncryptionMode.UseDatabaseKey
                    }
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                PeriodicBackupStatus status = null;
                var value = WaitForValue(() =>
                {
                    status = store.Maintenance.Send(operation).Status;
                    return status == null;
                }, false);
                Assert.False(value);
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

                await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, backupTaskId));
                operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var etag = WaitForValue(() =>
                {
                    status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 1);
                Assert.Equal(1, etag);

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

        [Fact]
        public async Task ShouldRearrangeTheTimeIfBackupAfterTimerCallbackGotActiveByOtherNode()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options
            {
                Server = server
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "EGR" }, "users/1");
                    await session.SaveChangesAsync();
                }

                while (DateTime.Now.Second > 55)
                    await Task.Delay(1000);

                await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
                {
                    FullBackupFrequency = "*/1 * * * *",
                    LocalSettings = new LocalSettings { FolderPath = backupPath },
                }));

                var record1 = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var backups1 = record1.PeriodicBackups;
                Assert.Equal(1, backups1.Count);

                var taskId = backups1.First().TaskId;
                var responsibleDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                Assert.NotNull(responsibleDatabase);
                var tag = responsibleDatabase.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                Assert.Equal(server.ServerStore.NodeTag, tag);

                responsibleDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateActiveByOtherNodeStatus = true;
                var pb = responsibleDatabase.PeriodicBackupRunner.PeriodicBackups.First();
                Assert.NotNull(pb);

                var val = WaitForValue(() => pb.HasScheduledBackup(), false, timeout: 66666, interval: 444);
                Assert.False(val, "PeriodicBackup should cancel the ScheduledBackup if the task status is ActiveByOtherNode, " +
                                  "so when the task status is back to be ActiveByCurrentNode, UpdateConfigurations will be able to reassign the backup timer");

                responsibleDatabase.PeriodicBackupRunner._forTestingPurposes = null;
                responsibleDatabase.PeriodicBackupRunner.UpdateConfigurations(record1);
                var getPeriodicBackupStatus = new GetPeriodicBackupStatusOperation(taskId);

                val = WaitForValue(() => store.Maintenance.Send(getPeriodicBackupStatus).Status?.LastFullBackup != null, true, timeout: 66666, interval: 444);
                Assert.True(val, "Failed to complete the backup in time");
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task FullBackupShouldSkipDeadSegments()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
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

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                var result = await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                await result.WaitForCompletionAsync();

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                using (RestoreDatabase(store, new RestoreBackupConfiguration
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
                var baseline = DateTime.Today;
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

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                var result = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, backupTaskId));
                await result.WaitForCompletionAsync();

                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor("users/1", "Heartrate").Delete();
                    await session.SaveChangesAsync();
                }

                result = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: false, backupTaskId));
                await result.WaitForCompletionAsync();

                // restore the database with a different name
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                using (RestoreDatabase(store, new RestoreBackupConfiguration
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

        [Theory]
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
                    using var bjro = ctx.ReadForMemory(result, "test");
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
                using (RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupLocation, DatabaseName = databaseName }))
                {
                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var usr = await session.LoadAsync<User>("users/1");
                        Assert.Equal(name, usr.Name);
                    }
                }
            }
        }

        [Fact]
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

        [Fact]
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
                    using var bjro = ctx.ReadForMemory(result, "test");
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

        [Fact]
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

                var config = new PeriodicBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "0 0 1 1 *",
                    FullBackupFrequency = "0 0 1 1 *"
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                var result = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, backupTaskId));
                await result.WaitForCompletionAsync();

                result = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: false, backupTaskId));
                await result.WaitForCompletionAsync();
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                PeriodicBackupStatus status = store.Maintenance.Send(operation).Status;

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

        [Fact]
        public async Task can_move_database_with_backup()
        {
            DoNotReuseServer();

            var cluster = await CreateRaftCluster(2);
            var databaseName = GetDatabaseName();
            await CreateDatabaseInCluster(databaseName, 2, cluster.Nodes[0].WebUrl);

            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = new DocumentStore
            {
                Urls = new[]
                {
                    cluster.Nodes[0].WebUrl,
                    cluster.Nodes[1].WebUrl
                },
                Database = databaseName
            }.Initialize())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Grisha" }, "users/1");
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1);
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    IncrementalBackupFrequency = "* * * * *"
                };

                var backupTaskId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                await store.Maintenance.SendAsync(new StartBackupOperation(true, backupTaskId));
                var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
                var value = WaitForValue(() =>
                {
                    var status = store.Maintenance.Send(operation).Status;
                    return status?.LastEtag;
                }, 1);
                Assert.Equal(1, value);

                var responsibleNode = store.Maintenance.Send(operation).Status.NodeTag;

                store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: responsibleNode, timeToWaitForConfirmation: TimeSpan.FromSeconds(30)));
                await WaitForDatabaseToBeDeleted(store, TimeSpan.FromSeconds(30));

                var server = cluster.Nodes.FirstOrDefault(x => x.ServerStore.NodeTag == responsibleNode == false);
                server.ServerStore.LicenseManager.LicenseStatus.Attributes["highlyAvailableTasks"] = false;

                var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).ConfigureAwait(false);
                var backupStatus = database.PeriodicBackupRunner.GetBackupStatus(backupTaskId);
                var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                config.TaskId = backupTaskId;
                var newResponsibleNode = database.WhoseTaskIsIt(databaseRecord.Topology, config, backupStatus, keepTaskOnOriginalMemberNode: true);

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

        [Fact]
        public async Task Backup_WhenContainRevisionWithoutConfiguration_ShouldBackupRevisions()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            var userForFullBackup = new User();
            var userForIncrementalBackup = new User();
            using (var src = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings { FolderPath = backupPath },
                    IncrementalBackupFrequency = "* * * * *" //every minute
                };
                var backupTaskId = (await src.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;

                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(userForFullBackup);
                    await session.StoreAsync(userForIncrementalBackup);
                    await session.SaveChangesAsync();

                    session.Advanced.Revisions.ForceRevisionCreationFor(userForFullBackup.Id);
                    await session.SaveChangesAsync();
                }
                await BackupAndAssertWait(src, backupTaskId, true);

                using (var session = src.OpenAsyncSession())
                {
                    session.Advanced.Revisions.ForceRevisionCreationFor(userForIncrementalBackup.Id);
                    await session.SaveChangesAsync();
                }
                await BackupAndAssertWait(src, backupTaskId, false);
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

        private async Task BackupAndAssertWait(IDocumentStore store, long backupTaskId, bool isFullBackup)
        {
            await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup, backupTaskId));
            var operation = new GetPeriodicBackupStatusOperation(backupTaskId);
            var databaseLastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDatabaseEtag;
            var backupLastEtag = await WaitForValueAsync(async () => (await store.Maintenance.SendAsync(operation)).Status.LastEtag, databaseLastEtag);
            Assert.Equal(databaseLastEtag, backupLastEtag);
        }

        internal static void RunBackup(long taskId, DocumentDatabase documentDatabase, bool isFullBackup, DocumentStore store, OperationStatus opStatus = OperationStatus.Completed)
        {
            var periodicBackupRunner = documentDatabase.PeriodicBackupRunner;
            var op = periodicBackupRunner.StartBackupTask(taskId, isFullBackup);
            var value = WaitForValue(() =>
            {
                var status = store.Maintenance.Send(new GetOperationStateOperation(op)).Status;
                return status;
            }, opStatus);

            Assert.Equal(opStatus, value);
        }

        private static string GetBackupPath(IDocumentStore store, long backTaskId, bool incremental = true)
        {
            var status = store.Maintenance.Send(new GetPeriodicBackupStatusOperation(backTaskId)).Status;

            var backupDirectory = status.LocalBackup.BackupDirectory;

            string datePrefix;
            if (incremental)
            {
                Debug.Assert(status.LastIncrementalBackup.HasValue);
                datePrefix = status.LastIncrementalBackup.Value.ToLocalTime().ToString("yyyy-MM-dd-HH-mm-ss");
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

        internal static string PrintBackupStatus(PeriodicBackupStatus status)
        {
            var sb = new StringBuilder();
            if (status == null)
                return $"{nameof(PeriodicBackupStatus)} is null";

            var isFull = status.IsFull ? "a full" : "an incremental";
            sb.AppendLine($"{nameof(PeriodicBackupStatus)} of backup task '{status.TaskId}', executed {isFull} '{status.BackupType}' on node '{status.NodeTag}' in '{status.DurationInMs}' ms.");
            sb.AppendLine("Debug Info: ");
            sb.AppendLine($"{nameof(PeriodicBackupStatus.LastDatabaseChangeVector)}: '{status.LastDatabaseChangeVector}'");
            sb.AppendLine($"{nameof(PeriodicBackupStatus.LastEtag)}: {status.LastEtag}'");
            sb.AppendLine($"{nameof(PeriodicBackupStatus.LastOperationId)}: '{status.LastOperationId}'");
            sb.AppendLine($"{nameof(PeriodicBackupStatus.LastRaftIndex)}: '{status.LastRaftIndex}'");
            sb.AppendLine($"{nameof(PeriodicBackupStatus.LastFullBackupInternal)}: '{status.LastFullBackupInternal}'");
            sb.AppendLine($"{nameof(PeriodicBackupStatus.LastIncrementalBackupInternal)}: '{status.LastIncrementalBackupInternal}'");
            sb.AppendLine($"{nameof(PeriodicBackupStatus.LastFullBackup)}: '{status.LastFullBackup}'");
            sb.AppendLine($"{nameof(PeriodicBackupStatus.LastIncrementalBackup)}: '{status.LastIncrementalBackup}'");
            sb.AppendLine();

            if (status.Error == null && string.IsNullOrEmpty(status.LocalBackup?.Exception))
            {
                sb.AppendLine("There were no errors.");
            }
            else
            {
                sb.AppendLine("There were the following errors during backup task execution:");
                if (status.Error != null)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus)}.{nameof(PeriodicBackupStatus.Error)}: ");
                    sb.AppendLine(status.Error.Exception);
                }

                if (string.IsNullOrEmpty(status.LocalBackup?.Exception) == false)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus)}.{nameof(PeriodicBackupStatus.LocalBackup)}.{nameof(PeriodicBackupStatus.LocalBackup.Exception)}: ");
                    sb.AppendLine(status.LocalBackup?.Exception);
                }
            }

            sb.AppendLine();
            sb.AppendLine("Backup upload status:");

            if (status.UploadToAzure == null)
            {
                sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToAzure)} of backup task '{status.TaskId}' is null.");
            }
            else if (status.UploadToAzure.Skipped)
            {
                sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToAzure)} of backup task '{status.TaskId}' was skipped.");
            }
            else
            {
                if (string.IsNullOrEmpty(status.UploadToAzure.Exception) == false)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus)}.{nameof(PeriodicBackupStatus.UploadToAzure)}.{nameof(PeriodicBackupStatus.UploadToAzure.Exception)}:");
                    sb.AppendLine(status.UploadToAzure?.Exception);
                }
                else
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToAzure)} of backup task '{status.TaskId}', ran successfully in '{status.UploadToAzure.UploadProgress.UploadTimeInMs}' ms, size: '{status.UploadToAzure.UploadProgress.TotalInBytes}' bytes.");
                }
            }
            if (status.UploadToFtp == null)
            {
                sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToFtp)} of backup task '{status.TaskId}' is null.");
            }
            else if (status.UploadToFtp.Skipped)
            {
                sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToFtp)} of backup task '{status.TaskId}' was skipped.");
            }
            else
            {
                if (string.IsNullOrEmpty(status.UploadToFtp.Exception) == false)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus)}.{nameof(PeriodicBackupStatus.UploadToFtp)}.{nameof(PeriodicBackupStatus.UploadToFtp.Exception)}:");
                    sb.AppendLine(status.UploadToFtp?.Exception);
                }
                else
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToFtp)} of backup task '{status.TaskId}', ran successfully in '{status.UploadToFtp.UploadProgress.UploadTimeInMs}' ms, size: '{status.UploadToFtp.UploadProgress.TotalInBytes}' bytes.");
                }
            }
            if (status.UploadToGlacier == null)
            {
                sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToGlacier)} of backup task '{status.TaskId}' is null.");
            }
            else if (status.UploadToGlacier.Skipped)
            {
                sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToGlacier)} of backup task '{status.TaskId}' was skipped.");
            }
            else
            {
                if (string.IsNullOrEmpty(status.UploadToGlacier.Exception) == false)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus)}.{nameof(PeriodicBackupStatus.UploadToGlacier)}.{nameof(PeriodicBackupStatus.UploadToGlacier.Exception)}:");
                    sb.AppendLine(status.UploadToGlacier?.Exception);
                }
                else
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToGlacier)} of backup task '{status.TaskId}', ran successfully in '{status.UploadToGlacier.UploadProgress.UploadTimeInMs}' ms, size: '{status.UploadToGlacier.UploadProgress.TotalInBytes}' bytes.");
                }
            }
            if (status.UploadToGoogleCloud == null)
            {
                sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToGoogleCloud)} of backup task '{status.TaskId}' is null.");
            }
            else if (status.UploadToGoogleCloud.Skipped)
            {
                sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToGoogleCloud)} of backup task '{status.TaskId}' was skipped.");
            }
            else
            {
                if (string.IsNullOrEmpty(status.UploadToGoogleCloud.Exception) == false)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus)}.{nameof(PeriodicBackupStatus.UploadToGoogleCloud)}.{nameof(PeriodicBackupStatus.UploadToGoogleCloud.Exception)}:");
                    sb.AppendLine(status.UploadToGoogleCloud?.Exception);
                }
                else
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToGoogleCloud)} of backup task '{status.TaskId}', ran successfully in '{status.UploadToGoogleCloud.UploadProgress.UploadTimeInMs}' ms, size: '{status.UploadToGoogleCloud.UploadProgress.TotalInBytes}' bytes.");
                }
            }
            if (status.UploadToS3 == null)
            {
                sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToS3)} of backup task '{status.TaskId}' is null.");
            }
            else if (status.UploadToS3.Skipped)
            {
                sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToS3)} of backup task '{status.TaskId}' was skipped.");
            }
            else
            {
                if (string.IsNullOrEmpty(status.UploadToS3.Exception) == false)
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus)}.{nameof(PeriodicBackupStatus.UploadToS3)}.{nameof(PeriodicBackupStatus.UploadToS3.Exception)}:");
                    sb.AppendLine(status.UploadToS3?.Exception);
                }
                else
                {
                    sb.AppendLine($"{nameof(PeriodicBackupStatus.UploadToS3)} of backup task '{status.TaskId}', ran successfully in '{status.UploadToS3.UploadProgress.UploadTimeInMs}' ms, size: '{status.UploadToS3.UploadProgress.TotalInBytes}' bytes.");
                }
            }

            return sb.ToString();
        }
    }
}
