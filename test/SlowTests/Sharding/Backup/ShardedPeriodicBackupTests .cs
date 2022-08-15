using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Backups.Sharding;
using Raven.Client.Documents.Operations.Identities;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Utils;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Backup
{
    public class ShardedPeriodicBackupTests : RavenTestBase
    {
        public ShardedPeriodicBackupTests(ITestOutputHelper output) : base(output)
        {
        }


        private const DatabaseItemType OperateOnTypes = DatabaseItemType.Documents
                                                | DatabaseItemType.TimeSeries
                                                | DatabaseItemType.CounterGroups
                                                | DatabaseItemType.Attachments
                                                | DatabaseItemType.Tombstones
                                                | DatabaseItemType.DatabaseRecord
                                                | DatabaseItemType.Subscriptions
                                                | DatabaseItemType.Identities
                                                | DatabaseItemType.CompareExchange
                                                | DatabaseItemType.CompareExchangeTombstones
                                                | DatabaseItemType.RevisionDocuments
                                                | DatabaseItemType.Indexes
                                                | DatabaseItemType.LegacyAttachments
                                                | DatabaseItemType.LegacyAttachmentDeletions
                                                | DatabaseItemType.LegacyDocumentDeletions;

        [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanBackupSharded(Options options)
        {
            var file = GetTempFileName();
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png"
            };
            try
            {
                var backupPath = NewDataPath(suffix: $"{options.DatabaseMode}_BackupFolder");

                using (var store1 = GetDocumentStore(new Options { ModifyDatabaseName = s => $"{s}_1" }))
                using (var store2 = Sharding.GetDocumentStore())
                using (var store3 = GetDocumentStore(options))
                {
                    await Sharding.Backup.InsertData(store1, names);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.Documents
                                         | DatabaseItemType.TimeSeries
                                         | DatabaseItemType.CounterGroups
                                         | DatabaseItemType.Attachments
                                         | DatabaseItemType.Tombstones
                                         | DatabaseItemType.DatabaseRecord
                                         | DatabaseItemType.Subscriptions
                                         | DatabaseItemType.Identities
                                         | DatabaseItemType.CompareExchange
                                         | DatabaseItemType.CompareExchangeTombstones
                                         | DatabaseItemType.RevisionDocuments
                                         | DatabaseItemType.Indexes
                                         | DatabaseItemType.LegacyAttachments
                                         | DatabaseItemType.LegacyAttachmentDeletions
                                         | DatabaseItemType.LegacyDocumentDeletions
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(20));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions()
                    {
                        OperateOnTypes = DatabaseItemType.Documents
                                         | DatabaseItemType.TimeSeries
                                         | DatabaseItemType.CounterGroups
                                         | DatabaseItemType.Attachments
                                         | DatabaseItemType.Tombstones
                                         | DatabaseItemType.DatabaseRecord
                                         | DatabaseItemType.Subscriptions
                                         | DatabaseItemType.Identities
                                         | DatabaseItemType.CompareExchange
                                         | DatabaseItemType.CompareExchangeTombstones
                                         | DatabaseItemType.RevisionDocuments
                                         | DatabaseItemType.Indexes
                                         | DatabaseItemType.LegacyAttachments
                                         | DatabaseItemType.LegacyAttachmentDeletions
                                         | DatabaseItemType.LegacyDocumentDeletions


                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                    var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store2);

                    var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *");
                    await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store2, config);

                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    // import

                    var dirs = Directory.GetDirectories(backupPath);

                    Assert.Equal(3, dirs.Length);
                    var importOptions = new DatabaseSmugglerImportOptions();

                    foreach (var dir in dirs)
                    {
                        await store3.Smuggler.ImportIncrementalAsync(importOptions, dir);
                        importOptions.OperateOnTypes &= ~DatabaseSmugglerOptions.OperateOnFirstShardOnly;
                    }

                    await Sharding.Backup.CheckData(store3, names, options.DatabaseMode);
                }
            }
            finally
            {
                File.Delete(file);
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

                        var shardNumber = shardedCtx.GetShardNumber(context, id);
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
                await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store1, config);

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

                        var shardNumber = shardedCtx.GetShardNumber(context, id);
                        shardNumToDocIds[shardNumber].Add(id);

                        await session.StoreAsync(user, id);
                    }

                    await session.SaveChangesAsync();
                }

                waitHandles = await Sharding.Backup.WaitForBackupToComplete(store1);

                await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store1, config);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                // import
                var newDirs = Directory.GetDirectories(backupPath).Except(dirs).ToList();
                Assert.Equal(3, newDirs.Count);

                foreach (var dir in newDirs)
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
                    FullBackupFrequency = "* * * * *",
                    Disabled = false,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                }));

                // wait for backups to complete
                var backupsDone = await Sharding.Backup.WaitForBackupsToComplete(new[] { store1, store2 });

                Assert.True(WaitHandle.WaitAll(backupsDone, TimeSpan.FromMinutes(2)));

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

                var result = await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(95));
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

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanBackupAndRestoreSharded()
        {
            var file = GetTempFileName();
            var names = new[]
            {
                "background-photo.jpg",
                "fileNAME_#$1^%_בעברית.txt",
                "profile.png"
            };
            try
            {
                var backupPath = NewDataPath(suffix: "BackupFolder");

                using (var store1 = GetDocumentStore())
                using (var store2 = Sharding.GetDocumentStore())
                using (var store3 = GetDocumentStore())
                {
                    //await Sharding.Backup.InsertData(store1, names);
                    await InsertData(store1, names);

                    var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions
                    {
                        OperateOnTypes = OperateOnTypes
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                    operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions
                    {
                        OperateOnTypes = OperateOnTypes
                    }, file);
                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                    var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store2);

                    var config = Backup.CreateBackupConfiguration(backupPath);
                    await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store2, config);

                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    var dirs = Directory.GetDirectories(backupPath);
                    Assert.Equal(3, dirs.Length);

                    var settings = new ShardedRestoreSettings
                    {
                        Shards = new SingleShardRestoreSetting[dirs.Length]
                    };

                    var sharding = await Sharding.GetShardingConfigurationAsync(store2);

                    for (int i = 0; i < dirs.Length; i++)
                    {
                        var dir = dirs[i];
                        var shardIndexPosition = dir.LastIndexOf('$') + 1;
                        var shardNumber = int.Parse(dir[shardIndexPosition].ToString());

                        settings.Shards[shardNumber] = new SingleShardRestoreSetting
                        {
                            ShardNumber = shardNumber,
                            BackupPath = dir,
                            NodeTag = sharding.Shards[shardNumber].Members[0]
                        };
                    }

                    // restore the database with a different name
                    var databaseName = $"restored_database-{Guid.NewGuid()}";
                    using (ReadOnly(backupPath))
                    using (Backup.RestoreDatabase(store3, new RestoreBackupConfiguration
                    {
                        DatabaseName = databaseName,
                        ShardRestoreSettings = settings

                    }, timeout: TimeSpan.FromSeconds(60)))
                    {
                        var dbRec = await store3.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));

                        Assert.Equal(DatabaseStateStatus.Normal, dbRec.DatabaseState);
                        Assert.Equal(3, dbRec.Sharding.Shards.Length);

                        WaitForUserToContinueTheTest(store3, database: databaseName);

                        await CheckData(Server.ServerStore, store3, names, databaseName);
                    }
                }
            }
            finally
            {
                File.Delete(file);
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
                    var shardNumber = shardedCtx.GetShardNumber(context, id);
                    if (shardNumToDocIds.TryGetValue(shardNumber, out var ids) == false)
                    {
                        shardNumToDocIds[shardNumber] = ids = new List<string>();
                    }

                    ids.Add(id);

                }

                Assert.Equal(dbRecord.Sharding.Shards.Length, shardNumToDocIds.Count);
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

        internal static IDisposable ReadOnly(string path)
        {
            var allFiles = new List<string>();
            var dirs = Directory.GetDirectories(path);
            FileAttributes attributes = default;
            foreach (string dir in dirs)
            {
                var files = Directory.GetFiles(dir);
                if (attributes != default)
                    attributes = new FileInfo(files[0]).Attributes;
    
                foreach (string file in files)
                {
                    File.SetAttributes(file, FileAttributes.ReadOnly);
                }

                allFiles.AddRange(files);
            }


            return new DisposableAction(() =>
            {
                foreach (string file in allFiles)
                {
                    File.SetAttributes(file, attributes);
                }
            });
        }

        private static async Task InsertData(IDocumentStore store, IReadOnlyList<string> names)
        {
            using (var session = store.OpenAsyncSession())
            {
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false
                    }
                }));

                //Docs
                await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1", Age = 5 }, "users/1");
                await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2", Age = 78 }, "users/2");
                await session.StoreAsync(new User { Name = "Name1", LastName = "LastName3", Age = 4 }, "users/3");
                await session.StoreAsync(new User { Name = "Name2", LastName = "LastName4", Age = 15 }, "users/4");

                //Time series
                session.TimeSeriesFor("users/1", "Heartrate")
                    .Append(DateTime.Now, 59d, "watches/fitbit");
                session.TimeSeriesFor("users/3", "Heartrate")
                    .Append(DateTime.Now.AddHours(6), 59d, "watches/fitbit");
                //counters
                session.CountersFor("users/2").Increment("Downloads", 100);
                //Attachments
                await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                await using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                    session.Advanced.Attachments.Store("users/2", names[1], fileStream);
                    session.Advanced.Attachments.Store("users/3", names[2], profileStream, "image/png");
                    await session.SaveChangesAsync();
                }
            }

            //tombstone + revision
            using (var session = store.OpenAsyncSession())
            {
                session.Delete("users/4");
                var user = await session.LoadAsync<User>("users/1");
                user.Age = 10;
                await session.SaveChangesAsync();
            }

            //subscription
            //await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>());

            //Identity
            store.Maintenance.Send(new SeedIdentityForOperation("users", 1990));

            /*//CompareExchange
            var user1 = new User
            {
                Name = "Toli"
            };
            var user2 = new User
            {
                Name = "Mitzi"
            };

            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("cat/toli", user1, 0));
            var operationResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("cat/mitzi", user2, 0));
            await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("cat/mitzi", operationResult.Index));

            //Cluster transaction
            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                var user5 = new User { Name = "Ayende" };
                await session.StoreAsync(user5, "users/5");
                await session.StoreAsync(new { ReservedFor = user5.Id }, "usernames/" + user5.Name);

                await session.SaveChangesAsync();
            }*/

            //Index
            await new Index().ExecuteAsync(store);
        }

        private static async Task CheckData(ServerStore serverStore, IDocumentStore store, IReadOnlyList<string> attachmentNames, string database)
        {
            long docsCount = default, tombstonesCount = default, revisionsCount = default;
            database ??= store.Database;
            var dbs = serverStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(database);
            foreach (var task in dbs)
            {
                var shard = await task;
                var storage = shard.DocumentsStorage;

                docsCount += storage.GetNumberOfDocuments();
                using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    tombstonesCount += storage.GetNumberOfTombstones(context);
                    revisionsCount += storage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                }

                //Index
                Assert.Equal(1, shard.IndexStore.Count);
            }

            // todo handle compare exchange
            //doc
            //Assert.Equal(5, docsCount);
            Assert.Equal(3, docsCount);

            //Assert.Equal(1, detailedStats.CountOfCompareExchangeTombstones); //TODO - Not working for 4.2

            //tombstone
            Assert.Equal(1, tombstonesCount);

            // todo handle compare exchange
            //revisions
            //Assert.Equal(28, revisionsCount);
            Assert.Equal(15, revisionsCount);

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal, "handle subscriptions");
            //Subscriptions
            //var subscriptionDocuments = await store.Subscriptions.GetSubscriptionsAsync(0, 10, database);
            //Assert.Equal(1, subscriptionDocuments.Count);

            using (var session = store.OpenSession(database))
            {
                //Time series
                var val = session.TimeSeriesFor("users/1", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);

                val = session.TimeSeriesFor("users/3", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);

                //Counters
                var counterValue = session.CountersFor("users/2").Get("Downloads");
                Assert.Equal(100, counterValue.Value);
            }

            using (var session = store.OpenAsyncSession(database))
            {
                for (var i = 0; i < attachmentNames.Count; i++)
                {
                    var user = await session.LoadAsync<User>("users/" + (i + 1));
                    var metadata = session.Advanced.GetMetadataFor(user);

                    //Attachment
                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(1, attachments.Length);
                    var attachment = attachments[0];
                    Assert.Equal(attachmentNames[i], attachment.GetString(nameof(AttachmentName.Name)));
                    var hash = attachment.GetString(nameof(AttachmentName.Hash));
                    if (i == 0)
                    {
                        Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                        Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                    else if (i == 1)
                    {
                        Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                        Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                    else if (i == 2)
                    {
                        Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                        Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                    }
                }

                await session.StoreAsync(new User() { Name = "Toli" }, "users|");
                await session.SaveChangesAsync();
            }
            //Identity
            using (var session = store.OpenAsyncSession(database))
            {
                var user = await session.LoadAsync<User>("users/1991");
                Assert.NotNull(user);
            }

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal, "handle CompareExchange");
            /*//CompareExchange
            using (var session = store.OpenAsyncSession(new SessionOptions { Database = database, TransactionMode = TransactionMode.ClusterWide }))
            {
                var user = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("cat/toli");
                Assert.NotNull(user);

                user = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("rvn-atomic/usernames/Ayende");
                Assert.NotNull(user);

                user = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("rvn-atomic/users/5");
                Assert.NotNull(user);

                var user2 = await session.LoadAsync<User>("users/5");
                Assert.NotNull(user2);

                user2 = await session.LoadAsync<User>("usernames/Ayende");
                Assert.NotNull(user2);
            }*/
        }

        private class Index : AbstractIndexCreationTask<User>
        {
            public Index()
            {
                Map = items =>
                    from item in items
                    select new
                    {
                        _ = new[]
                        {
                            CreateField("foo", "a"),
                            CreateField("foo", "b"),
                        }
                    };
            }
        }
    }
}
