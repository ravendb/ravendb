using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Backups.Sharding;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Platform;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;
using BackupTask = Raven.Server.Documents.PeriodicBackup.BackupTask;
using Directory = System.IO.Directory;

namespace SlowTests.Sharding.Backup
{
    public class ShardedBackupTests : ClusterTestBase
    {
        public ShardedBackupTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanBackupSharded(Options options)
        {
            var backupPath = NewDataPath(suffix: $"{options.DatabaseMode}_BackupFolder");

            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Backup.MaxNumberOfConcurrentBackups)] = 1.ToString()
                }
            });
            using (var store1 = Sharding.GetDocumentStore(new Options
            {
                Server = server
            }))
            using (var store2 = GetDocumentStore(options))
            {
                await Sharding.Backup.InsertData(store1);

                // assert that index was created on all shards
                await foreach (var shard in Sharding.GetShardsDocumentDatabaseInstancesFor(store1))
                {
                    Assert.Equal(1, shard.IndexStore.Count);
                }

                var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store1, server);

                var config = Backup.CreateBackupConfiguration(backupPath);
                await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(server, store1, config);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                // import

                var dirs = Directory.GetDirectories(backupPath);

                Assert.Equal(3, dirs.Length);
                var importOptions = new DatabaseSmugglerImportOptions();

                foreach (var dir in dirs)
                {
                    await store2.Smuggler.ImportIncrementalAsync(importOptions, dir);
                    importOptions.OperateOnTypes &= ~DatabaseSmugglerOptions.OperateOnFirstShardOnly;
                }

                await Sharding.Backup.CheckData(store2, options.DatabaseMode, expectedRevisionsCount: 21);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanBackupShardedIncremental()
        {
            var backupPath = NewDataPath(suffix: "_BackupFolder");

            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                var shardNumToDocIds = new Dictionary<int, List<string>>();
                var dbRecord = await store1.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store1.Database));
                var shardedCtx = new ShardedDatabaseContext(Server.ServerStore, dbRecord);

                // generate data on store1, keep track of doc-ids per shard
                using (var session = store1.OpenAsyncSession())
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    for (int i = 0; i < 100; i++)
                    {
                        var user = new User { Name = i.ToString() };
                        var id = $"users/{i}";

                        var shardNumber = shardedCtx.GetShardNumberFor(context, id);
                        if (shardNumToDocIds.TryGetValue(shardNumber, out var ids) == false)
                        {
                            shardNumToDocIds[shardNumber] = ids = new List<string>();
                        }
                        ids.Add(id);

                        await session.StoreAsync(user, id);
                    }

                    Assert.Equal(3, shardNumToDocIds.Count);

                    await session.SaveChangesAsync();
                }

                var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store1);

                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "* * * * *");
                long taskId = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store1, config);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                // import
                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(3, dirs.Length);

                foreach (var dir in dirs)
                {
                    await store2.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), dir);
                }

                // assert
                await AssertDocsInShardedDb(store2, shardNumToDocIds);

                // add more data to store1
                using (var session = store1.OpenAsyncSession())
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    for (int i = 100; i < 200; i++)
                    {
                        var user = new User { Name = i.ToString() };
                        var id = $"users/{i}";

                        var shardNumber = shardedCtx.GetShardNumberFor(context, id);
                        shardNumToDocIds[shardNumber].Add(id);

                        await session.StoreAsync(user, id);
                    }

                    await session.SaveChangesAsync();
                }

                waitHandles = await Sharding.Backup.WaitForBackupToComplete(store1);
                
                await Sharding.Backup.RunBackupAsync(store1, taskId, isFullBackup: false);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                // import

                foreach (var dir in dirs)
                {
                    var backupFiles = Directory.GetFiles(Path.Combine(backupPath, dir));
                    Assert.Equal(2, backupFiles.Length);
                    Assert.Equal(1, backupFiles.Count(x => x.Contains("incremental")));
                }
                
                foreach (var dir in dirs)
                {
                    await store2.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), dir);
                }

                // assert
                await AssertDocsInShardedDb(store2, shardNumToDocIds);
            }
        }

        [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanAddServerWideBackup(Options options)
        {
            DoNotReuseServer();

            using (var store1 = GetDocumentStore(options))
            using (var store2 = Sharding.GetDocumentStore())
            using (var store3 = GetDocumentStore())
            {
                await store1.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
                {
                    FullBackupFrequency = "* * * * *",
                    Disabled = true,
                    Name = "test"
                }));

                foreach (var store in new[] { store1, store2, store3 })
                {
                    var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    Assert.Equal(1, databaseRecord.PeriodicBackups.Count);
                    Assert.Equal("Server Wide Backup, test", databaseRecord.PeriodicBackups[0].Name);

                }
            }
        }

        [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanBackupShardedServerWide(Options options)
        {
            DoNotReuseServer();

            const string usersPrefix = "Users";
            const string ordersPrefix = "Orders";

            var backupPath = NewDataPath(suffix: "_BackupFolder");

            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                // generate data on store1 and store2
                using (var session1 = store1.OpenAsyncSession())
                using (var session2 = store2.OpenAsyncSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        await session1.StoreAsync(new User(), $"{usersPrefix}/{i}");
                        await session2.StoreAsync(new Order(), $"{ordersPrefix}/{i}");
                    }

                    await session1.SaveChangesAsync();
                    await session2.SaveChangesAsync();
                }

                // define server wide backup
                await store1.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
                {
                    FullBackupFrequency = "0 0 1 1 *",
                    Disabled = false,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                }));

                // wait for backups to complete
                var backupsDone = await Sharding.Backup.WaitForBackupToComplete(store1);
                var backupsDone2 = await Backup.WaitForBackupToComplete(store2);

                foreach (var store in new[] { store1, store2 })
                {
                    var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    Assert.Equal(1, databaseRecord.PeriodicBackups.Count);

                    var taskId = databaseRecord.PeriodicBackups[0].TaskId;
                    if (databaseRecord.IsSharded)
                        await Sharding.Backup.RunBackupAsync(store, taskId, isFullBackup: true);
                    else
                        await Backup.RunBackupAsync(Server, taskId, store, isFullBackup: true);
                }

                Assert.True(WaitHandle.WaitAll(backupsDone, TimeSpan.FromMinutes(1)));
                Assert.True(WaitHandle.WaitAll(backupsDone2, TimeSpan.FromMinutes(1)));

                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(2, dirs.Length);

                var store1Backups = Directory.GetDirectories(Path.Combine(backupPath, store1.Database));
                var store2Backup = Directory.GetDirectories(Path.Combine(backupPath, store2.Database));

                Assert.Equal(3, store1Backups.Length); // one per shard
                Assert.Single(store2Backup);

                // import data to new stores and assert
                using (var store3 = GetDocumentStore(options))
                using (var store4 = GetDocumentStore(options))
                {
                    foreach (var dir in store1Backups)
                    {
                        await store3.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), dir);
                    }

                    await store4.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), store2Backup[0]);

                    await AssertDocs(store3, idPrefix: usersPrefix, dbMode: options.DatabaseMode);
                    await AssertDocs(store4, idPrefix: ordersPrefix, dbMode: options.DatabaseMode);
                }
            }
        }

        [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanBackupShardedServerWide_UsingScript(Options options)
        {
            DoNotReuseServer();

            const string usersPrefix = "Users";
            const string ordersPrefix = "Orders";

            var backupPath = NewDataPath(suffix: "_BackupFolder");

            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                // generate data on store1 and store2
                using (var session1 = store1.OpenAsyncSession())
                using (var session2 = store2.OpenAsyncSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        await session1.StoreAsync(new User(), $"{usersPrefix}/{i}");
                        await session2.StoreAsync(new Order(), $"{ordersPrefix}/{i}");
                    }

                    await session1.SaveChangesAsync();
                    await session2.SaveChangesAsync();
                }

                // use backup configuration script for local settings
                var scriptPath = GenerateConfigurationScript(backupPath, out var command);
                var config = new ServerWideBackupConfiguration
                {
                    FullBackupFrequency = "0 0 1 1 *",
                    Disabled = false,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath,
                        GetBackupConfigurationScript = new GetBackupConfigurationScript
                        {
                            Exec = command,
                            Arguments = scriptPath
                        }
                    }
                };

                // define server wide backup
                await store1.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(config));

                // wait for backups to complete
                var backupsDone = await Sharding.Backup.WaitForBackupToComplete(store1);
                var backupsDone2 = await Backup.WaitForBackupToComplete(store2);

                foreach (var store in new[] { store1, store2 })
                {
                    var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    Assert.Equal(1, databaseRecord.PeriodicBackups.Count);

                    var taskId = databaseRecord.PeriodicBackups[0].TaskId;
                    if (databaseRecord.IsSharded)
                        await Sharding.Backup.RunBackupAsync(store, taskId, isFullBackup: true);
                    else
                        await Backup.RunBackupAsync(Server, taskId, store, isFullBackup: true);
                }

                Assert.True(WaitHandle.WaitAll(backupsDone, TimeSpan.FromMinutes(1)));
                Assert.True(WaitHandle.WaitAll(backupsDone2, TimeSpan.FromMinutes(1)));

                // one backup folder per database
                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(2, dirs.Length);

                var shardedBackupPath = dirs.First(x => x.Contains(store1.Database));
                var nonShardedBackupPath = dirs.First(x => x.Contains(store2.Database));

                // should have one root folder for all shards 
                Assert.Contains(store1.Database, shardedBackupPath);
                Assert.DoesNotContain('$', nonShardedBackupPath);

                var store1Backups = Directory.GetDirectories(Path.Combine(backupPath, store1.Database)).OrderBy(x => x).ToArray();
                var store2Backup = Directory.GetDirectories(Path.Combine(backupPath, store2.Database));

                Assert.Equal(3, store1Backups.Length); // one per shard
                Assert.Single(store2Backup);

                Assert.Contains(ShardHelper.ToShardName(store1.Database, 0), store1Backups[0]);
                Assert.Contains(ShardHelper.ToShardName(store1.Database, 1), store1Backups[1]);
                Assert.Contains(ShardHelper.ToShardName(store1.Database, 2), store1Backups[2]);

                // import data to new stores and assert
                using (var store3 = GetDocumentStore(options))
                using (var store4 = GetDocumentStore(options))
                {
                    foreach (var dir in store1Backups)
                    {
                        await store3.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), dir);
                    }

                    await store4.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), store2Backup[0]);

                    await AssertDocs(store3, idPrefix: usersPrefix, dbMode: options.DatabaseMode);
                    await AssertDocs(store4, idPrefix: ordersPrefix, dbMode: options.DatabaseMode);
                }
            }
        }

        [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task OneTimeBackupSharded(Options options)
        {
            var backupPath = NewDataPath(suffix: "_BackupFolder");
            const string idPrefix = "users";

            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        await session.StoreAsync(new User(), $"{idPrefix}/{i}");
                    }
                    await session.SaveChangesAsync();
                }

                var config = new BackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                };

                var operation = await store.Maintenance.SendAsync(new BackupOperation(config));

                var result = await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
                var backupResult = (ShardedBackupResult)result;
                Assert.NotNull(backupResult);
                Assert.Equal(100, backupResult.Results.Sum(x => x.Result.Documents.ReadCount));

                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(3, dirs.Length);
                using (var store2 = GetDocumentStore(options))
                {
                    foreach (var dir in dirs)
                    {
                        await store2.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), dir);
                    }

                    await AssertDocs(store2, idPrefix, options.DatabaseMode);
                }
            }
        }

        [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task BackupNowSharded(Options options)
        {
            var backupPath = NewDataPath(suffix: "_BackupFolder");
            const string idPrefix = "users";

            using (var store = GetDocumentStore(options))
            {
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 2 * * 0", incrementalBackupFrequency: "0 2 * * 1");

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var backupTaskId = result.TaskId;
                Sharding.Backup.WaitForResponsibleNodeUpdate(Server.ServerStore, store.Database, backupTaskId);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        await session.StoreAsync(new User(), $"{idPrefix}/{i}");
                    }
                    await session.SaveChangesAsync();
                }

                var op = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, backupTaskId));
                var operationResult = await op.WaitForCompletionAsync();

                string[] dirs = null;
                if (options.DatabaseMode == RavenDatabaseMode.Single)
                {
                    var backupResult = operationResult as BackupResult;
                    Assert.Equal(100, backupResult.Documents.ReadCount);
                    dirs = Directory.GetDirectories(backupPath);
                    Assert.Equal(1, dirs.Length);
                }
                else if (options.DatabaseMode == RavenDatabaseMode.Sharded)
                {
                    var sbr = operationResult as ShardedBackupResult;
                    Assert.Equal(3, sbr.Results.Count);
                    Assert.Equal(100, sbr.Results.Sum(x => x.Result.Documents.ReadCount));
                    dirs = Directory.GetDirectories(backupPath);
                    Assert.Equal(3, dirs.Length);
                }

                using (var store2 = GetDocumentStore(options))
                {
                    foreach (var dir in dirs)
                    {
                        await store2.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), dir);
                    }

                    await AssertDocs(store2, idPrefix, options.DatabaseMode);
                }
            }
        }

        [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public async Task ShardedPeriodicBackup_DontAllowMentorNode(Options options)
        {
            var backupPath = NewDataPath(suffix: "_BackupFolder");
            
            using (var store = GetDocumentStore(options))
            {
                var config = Backup.CreateBackupConfiguration(backupPath, mentorNode: "A");

                var error = await Assert.ThrowsAnyAsync<RavenException>(async () =>
                {
                    var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                });
                Assert.Contains("Choosing a mentor node for an ongoing task is not supported in sharding", error.Message);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanGetShardedBackupStatus()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var cluster = await CreateRaftCluster(3, watcherCluster: true);

            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);
            using (var store = Sharding.GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User(), $"users/{i}");
                    }

                    session.SaveChanges();
                }

                var waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(backupTaskId + 3, TimeSpan.FromSeconds(10));

                var dirs = Directory.GetDirectories(backupPath).ToList();
                Assert.Equal(cluster.Nodes.Count, dirs.Count);

                var status = store.Maintenance.Send(new GetShardedPeriodicBackupStatusOperation(backupTaskId));

                Assert.Equal(3, status.Statuses.Count);
                foreach (var (shardNumber, shardBackupStatus) in status.Statuses)
                {
                    Assert.NotNull(shardBackupStatus);
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task ShouldThrowOnAttemptToGetNonShardedBackupStatusFromShardedDb()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var cluster = await CreateRaftCluster(3, watcherCluster: true);

            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);
            using (var store = Sharding.GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User(), $"users/{i}");
                    }

                    session.SaveChanges();
                }

                var waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                var dirs = Directory.GetDirectories(backupPath).ToList();
                Assert.Equal(cluster.Nodes.Count, dirs.Count);

                var op = store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(backupTaskId));

                var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await op);

                Assert.Contains($"Database is sharded, can't use {nameof(GetPeriodicBackupStatusOperation)}, " +
                                $"use {nameof(GetShardedPeriodicBackupStatusOperation)} instead", ex.Message);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task ShouldThrowOnAttemptToGetShardedBackupStatusFromNonShardedDb()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User(), $"users/{i}");
                    }

                    session.SaveChanges();
                }

                var waitHandles = await Backup.WaitForBackupToComplete(store);

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                var dirs = Directory.GetDirectories(backupPath).ToList();
                Assert.Equal(1, dirs.Count);

                var op = store.Maintenance.SendAsync(new GetShardedPeriodicBackupStatusOperation(backupTaskId));

                var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await op);

                Assert.Contains($"Database is not sharded, can't use {nameof(GetShardedPeriodicBackupStatusOperation)}, " +
                                $"use {nameof(GetPeriodicBackupStatusOperation)} instead", ex.Message);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task ShardedBackupNowShouldUseSameStartTimeForAllShards()
        {
            var backupPath = NewDataPath(suffix: "_BackupFolder");

            var cluster = await CreateRaftCluster(3, watcherCluster: true);

            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);
            using (var store = Sharding.GetDocumentStore(options))
            {
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 2 * * 0", incrementalBackupFrequency: "0 2 * * 1");

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var backupTaskId = result.TaskId;

                Sharding.Backup.WaitForResponsibleNodeUpdateInCluster(store, cluster.Nodes, backupTaskId);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        await session.StoreAsync(new User(), $"users/{i}");
                    }
                    await session.SaveChangesAsync();
                }

                var op = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, backupTaskId));
                await op.WaitForCompletionAsync(TimeSpan.FromSeconds(10));
                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(3, dirs.Length);

                var dates = new HashSet<DateTime>();
                foreach (var path in dirs)
                {
                    var backupFolderName = Path.GetRelativePath(backupPath, path);
                    var datePart = backupFolderName[..backupFolderName.IndexOf('.')];

                    Assert.True(DateTime.TryParseExact(datePart, 
                        BackupTask.DateTimeFormat, 
                        CultureInfo.InvariantCulture, 
                        DateTimeStyles.RoundtripKind, 
                        out var date));

                    dates.Add(date);
                }

                Assert.Single(dates);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task ServerWideSnapshotBackupShouldExcludeShardedDbs()
        {
            DoNotReuseServer();

            using (var store1 = GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            using (var store3 = GetDocumentStore())
            {
                const string name = "test";
                await store1.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
                {
                    BackupType = BackupType.Snapshot,
                    FullBackupFrequency = "* * * * *",
                    Disabled = true,
                    Name = name
                }));

                foreach (var store in new[] { store1, store2, store3 })
                {
                    var databaseRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    if (databaseRecord.IsSharded)
                    {
                        Assert.Equal(0, databaseRecord.PeriodicBackups.Count);
                        continue;
                    }

                    Assert.Equal(1, databaseRecord.PeriodicBackups.Count);
                    Assert.Equal($"Server Wide Backup, {name}", databaseRecord.PeriodicBackups[0].Name);
                }

                // add new databases
                using (var store4 = Sharding.GetDocumentStore())
                using (var store5 = GetDocumentStore())
                {
                    // sharded db should be excluded 
                    var databaseRecord = await store4.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store4.Database));
                    Assert.Equal(0, databaseRecord.PeriodicBackups.Count);

                    // non-sharded db should not be excluded 
                    databaseRecord = await store4.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store5.Database));
                    Assert.Equal(1, databaseRecord.PeriodicBackups.Count);
                    Assert.Equal($"Server Wide Backup, {name}", databaseRecord.PeriodicBackups[0].Name);

                    // validate ExcludedDatabases list in server-wide config
                    using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var serverWideBackups = Server.ServerStore.Cluster.Read(context, ClusterStateMachine.ServerWideConfigurationKey.Backup);
                        Assert.True(serverWideBackups.TryGet(name, out BlittableJsonReaderObject configuration));

                        var serverWideConfig = JsonDeserializationCluster.ServerWideBackupConfiguration(configuration);
                        Assert.Equal(2, serverWideConfig.ExcludedDatabases.Length);
                        Assert.Contains(store2.Database, serverWideConfig.ExcludedDatabases);
                        Assert.Contains(store4.Database, serverWideConfig.ExcludedDatabases);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding, Skip = "waiting for RavenDB-20544")]
        public async Task CanKillShardedBackupOperation()
        {
            var backupPath = NewDataPath(suffix: "_BackupFolder");

            using (var store = Sharding.GetDocumentStore())
            {
                await Sharding.Backup.InsertData(store);

                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 2 * * 0", incrementalBackupFrequency: "0 2 * * 1");

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var backupTaskId = result.TaskId;

                using (var reqEx = store.GetRequestExecutor())
                using (reqEx.ContextPool.AllocateOperationContext(out var context))
                {
                    var op = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, backupTaskId));
                    await reqEx.ExecuteAsync(new KillOperationCommand(op.Id), context);

                    Assert.Throws<TaskCanceledException>(() => op.WaitForCompletion());
                }
            }
        }

        private Task AssertDocs(IDocumentStore store, string idPrefix, RavenDatabaseMode dbMode, int count = 100)
        {
            return dbMode switch
            {
                RavenDatabaseMode.Single => AssertDocs(store, idPrefix, count),
                RavenDatabaseMode.Sharded => AssertDocsInShardedDb(store, idPrefix, count),
                _ => throw new ArgumentOutOfRangeException()
            };
        }

        private async Task AssertDocs(IDocumentStore store, string idPrefix, int count = 100)
        {
            var ids = Enumerable.Range(0, count).Select(x => $"{idPrefix}/{x}").ToList();
            var db = await GetDocumentDatabaseInstanceFor(store);
            AssertDocs(db, ids);
        }

        private static void AssertDocs(DocumentDatabase database, IReadOnlyCollection<string> ids)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var docs = database.DocumentsStorage.GetDocumentsFrom(context, 0).ToList();
                Assert.NotEmpty(docs);
                Assert.Equal(ids.Count, docs.Count);

                foreach (var doc in docs)
                {
                    Assert.Contains(doc.Id, ids);
                }
            }
        }

        private async Task AssertDocsInShardedDb(IDocumentStore store, string idPrefix, int count = 100)
        {
            var dbRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            if (dbRecord.IsSharded == false)
                throw new InvalidOperationException($"database {store.Database} is not sharded");

            var shardedCtx = new ShardedDatabaseContext(Server.ServerStore, dbRecord);
            var shardNumToDocIds = new Dictionary<int, List<string>>();

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                for (int i = 0; i < count; i++)
                {
                    var id = $"{idPrefix}/{i}";
                    var shardNumber = shardedCtx.GetShardNumberFor(context, id);
                    if (shardNumToDocIds.TryGetValue(shardNumber, out var ids) == false)
                    {
                        shardNumToDocIds[shardNumber] = ids = new List<string>();
                    }

                    ids.Add(id);

                }

                Assert.Equal(dbRecord.Sharding.Shards.Count, shardNumToDocIds.Count);
            }

            await AssertDocsInShardedDb(store, shardNumToDocIds);
        }

        private async Task AssertDocsInShardedDb(IDocumentStore store, Dictionary<int, List<string>> shardNumToDocIds)
        {
            foreach ((int shardNumber, List<string> ids) in shardNumToDocIds)
            {
                var shard = await GetDocumentDatabaseInstanceFor(store, $"{store.Database}${shardNumber}");
                AssertDocs(shard, ids);
            }
        }

        private static string GenerateConfigurationScript(string path, out string command)
        {
            var scriptPath = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            var localSetting = new LocalSettings { FolderPath = path };
            var localSettingsString = JsonConvert.SerializeObject(localSetting);

            string script;
            if (PlatformDetails.RunningOnPosix)
            {
                command = "bash";
                script = $"#!/bin/bash\r\necho '{localSettingsString}'";
                File.WriteAllText(scriptPath, script);
                Process.Start("chmod", $"700 {scriptPath}");
            }
            else
            {
                command = "powershell";
                script = $"echo '{localSettingsString}'";
                File.WriteAllText(scriptPath, script);
            }

            return scriptPath;
        }

    }
}
