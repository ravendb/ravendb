using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class RavenDB_11139 : RavenTestBase
    {
        public RavenDB_11139(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CreateFullAndIncrementalBackupWithCompareExchange()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var user = new User
                {
                    Name = "💩"
                };

                var operationResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/poo", user, 0));

                //make sure that the PutCompareExchangeValueOperation succeeds
                //because otherwise we might have NRE at the Assert.Equal() calls
                Assert.True(operationResult.Successful,"Failing early because the test will fail anyways - the PutCompareExchangeValueOperation failed...");

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Backup
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP

                var user2 = new User
                {
                    Name = "💩🤡"
                };

                operationResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/pooclown", user2, 0));
                Assert.True(operationResult.Successful,"Failing early because the test will fail anyways - the PutCompareExchangeValueOperation failed...");

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL

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
                        
                        //precaution, those shouldn't be null
                        Assert.NotNull(user1);
                        Assert.NotNull(user3);
                        
                        Assert.Equal(user.Name, user1.Value.Name);
                        Assert.Equal(user2.Name, user3.Value.Name);
                    }
                }
            }
        }

        [Fact]
        public async Task CreateFullAndIncrementalBackupWithCompareExchangeAndRestoreOnlyIncremental()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var user = new User
                {
                    Name = "💩"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/poo", user, 0));

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Backup
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP

                var user2 = new User
                {
                    Name = "💩🤡"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/pooclown", user2, 0));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                var files = Directory.GetFiles(backupDirectory)
                    .Where(BackupUtils.IsBackupFile)
                    .OrderBackups()
                    .ToArray();

                File.Delete(files.First());

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
                        var user3 = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("emojis/pooclown"));
                        Assert.Equal(user2.Name, user3.Value.Name);

                        var stats = store2.Maintenance.ForDatabase(databaseName).Send(new GetDetailedStatisticsOperation());
                        Assert.Equal(1, stats.CountOfCompareExchange);
                    }
                }
            }
        }

        [Fact]
        public async Task CreateFullAndIncrementalBackupWithCompareExchangeAndRestoreOnlyIncrementalBackups()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var user = new User
                {
                    Name = "💩"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/poo", user, 0));
                var val = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>("emojis/poo"));
                Assert.True(user.Name == val.Value.Name, "val.Value.Name = 'emojis/poo'");

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "0 0 1 1 *", // once a year on 1st january at 00:00
                    BackupType = BackupType.Backup
                };

                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP

                var user2 = new User
                {
                    Name = "🤡"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/clown", user2, 0));
                var val2 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>("emojis/clown"));
                Assert.True(user2.Name == val2.Value.Name, "val.Value.Name = 'emojis/clown'");

                config.FullBackupFrequency = null;
                config.IncrementalBackupFrequency = "0 0 1 1 *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL1

                var user3 = new User
                {
                    Name = "👺"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/goblin", user3, 0));
                var val3 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>("emojis/goblin"));
                Assert.True(user3.Name == val3.Value.Name, "val.Value.Name = 'emojis/goblin'");

                config.IncrementalBackupFrequency = "0 0 1 1 *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL2

                var user4 = new User
                {
                    Name = "👻"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/ghost", user4, 0));
                var val4 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>("emojis/ghost"));
                Assert.True(user4.Name == val4.Value.Name, "val.Value.Name = 'emojis/ghost'");

                config.IncrementalBackupFrequency = "0 0 1 1 *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL3

                var user5 = new User
                {
                    Name = "🤯"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/exploding_head", user5, 0));
                var val5 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>("emojis/exploding_head"));
                Assert.True(user5.Name == val5.Value.Name, "val.Value.Name = 'emojis/exploding_head'");

                var emojisNum = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation()).CountOfCompareExchange;
                Assert.True(emojisNum == 5, "CountOfCompareExchange == 5");

                config.IncrementalBackupFrequency = "0 0 1 1 *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL4

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                var files = Directory.GetFiles(backupDirectory)
                    .Where(BackupUtils.IsBackupFile)
                    .OrderBackups()
                    .ToArray();

                Assert.Equal(5, files.Length);
                Assert.True(files.First().EndsWith("ravendb-full-backup"), "files.First().EndsWith('ravendb-full-backup')");

                File.Delete(files.First());                            // delete full backup file     

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
                        var users = await session.Advanced.ClusterTransaction.GetCompareExchangeValuesAsync<User>(new[] { "emojis/clown", "emojis/goblin", "emojis/ghost", "emojis/exploding_head" });
                        foreach (var v in users.Values)
                        {
                            Assert.True(v.Value != null, $"compare exchange with key: {v.Key} is null.");
                        }

                        var stats = store2.Maintenance.ForDatabase(databaseName).Send(new GetDetailedStatisticsOperation());
                        Assert.True(4 == stats.CountOfCompareExchange, $"all backup files: {string.Join(", ", files)}");

                        Assert.Equal(user2.Name, users["emojis/clown"].Value.Name);
                        Assert.Equal(user3.Name, users["emojis/goblin"].Value.Name);
                        Assert.Equal(user4.Name, users["emojis/ghost"].Value.Name);
                        Assert.Equal(user5.Name, users["emojis/exploding_head"].Value.Name);
                    }
                }
            }
        }

        [Fact]
        public async Task CreateFullAndIncrementalBackupWithCompareExchangeAndDeleteBetween()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var user = new User
                {
                    Name = "poo"
                };
                var pooResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/💩", user, 0));

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Backup
                };

                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP

                var user2 = new User
                {
                    Name = "clown"
                };
                var clownResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/🤡", user2, 0));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 1

                // delete poo
                await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("emojis/💩", pooResult.Index));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 2

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
                        var stats = store2.Maintenance.ForDatabase(databaseName).Send(new GetDetailedStatisticsOperation());
                        Assert.Equal(1, stats.CountOfCompareExchange);

                        var user22 = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("emojis/🤡"));
                        Assert.Equal(user2.Name, user22.Value.Name);
                    }
                }
            }
        }

        [Fact]
        public async Task CreateFullAndIncrementalBackupWithCompareExchangeAndDeleteBetweenBackups()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var user = new User
                {
                    Name = "poo"
                };
                var pooResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/💩", user, 0));

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Backup
                };

                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP

                var user2 = new User
                {
                    Name = "clown"
                };
                var clownResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/🤡", user2, 0));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 1

                // delete poo
                await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("emojis/💩", pooResult.Index));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 2

                var user3 = new User
                {
                    Name = "PirateFlag"
                };
                var pirateFlagResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/🏴‍☠️", user3, 0));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 3

                // delete clown
                await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("emojis/🤡", clownResult.Index));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 4

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
                        var stats = store2.Maintenance.ForDatabase(databaseName).Send(new GetDetailedStatisticsOperation());
                        Assert.Equal(1, stats.CountOfCompareExchange);

                        var user33 = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("emojis/🏴‍☠️"));
                        Assert.Equal(user3.Name, user33.Value.Name);
                    }
                }
            }
        }

        [Fact]
        public async Task CreateFullAndIncrementalBackupWithCompareExchangesAndDeleteBetween()
        {
            var list = new List<string>(new [] {"🐃", "🐂", "🐄", "🐎", "🐖",
                                                "🐏", "🐑", "🐐", "🦌", "🐕",
                                                "🐩", "🐈", "🐓", "🦃", "🕊",
                                                "🐇", "🐁", "🐀", "🐿", "🦔"});

            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var count = 0;
                foreach (var e in list)
                {
                    var user = new User
                    {
                        Name = $"emoji_{count}"
                    };
                    await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>($"emojis/{e}", user, 0));
                    count++;
                }
                var stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(count, stats.CountOfCompareExchange);

                list = ConcatStringInList(list);
                foreach (var e in list)
                {
                    var user = new User
                    {
                        Name = $"emoji_{count}"
                    };
                    await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>($"emojis/{e}", user, 0));
                    count++;
                }
                stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(count, stats.CountOfCompareExchange);

                list = ConcatStringInList(list);
                var indexesList = new List<long>();
                foreach (var e in list)
                {
                    var user = new User
                    {
                        Name = $"emoji_{count}"
                    };
                    var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>($"emojis/{e}", user, 0));
                    indexesList.Add(res.Index);
                    count++;
                }
                stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(count, stats.CountOfCompareExchange);
                Assert.Equal(list.Count, indexesList.Count);

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Backup
                };

                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP

                for (var i = 0; i < list.Count; i++)
                {
                    if (list[i].Length == 8)
                    {
                        var res = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>($"emojis/{list[i]}", indexesList[i]));
                        if (res.Value != null)
                            count--;
                    }
                }
                stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(count, stats.CountOfCompareExchange);

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 1

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
                    stats = store2.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(count, stats.CountOfCompareExchange);
                }
            }
        }

        [Fact]
        public async Task CreateFullAndIncrementalBackupWithCompareExchangesAndDeletePlusAddBetween()
        {
            var list = new List<string>(new[] { "🐃", "🐂", "🐄", "🐎", "🐖",
                                                "🐏", "🐑", "🐐", "🦌", "🐕",
                                                "🐩", "🐈", "🐓", "🦃", "🕊",
                                                "🐇", "🐁", "🐀", "🐿", "🦔" });

            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var count = 0;

                foreach (var e in list)
                {
                    var user = new User
                    {
                        Name = $"emoji_{count}"
                    };
                    await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>($"emojis/{e}", user, 0));
                    count++;
                }
                var stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(count, stats.CountOfCompareExchange);

                list = ConcatStringInList(list);
                foreach (var e in list)
                {
                    var user = new User
                    {
                        Name = $"emoji_{count}"
                    };
                    await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>($"emojis/{e}", user, 0));
                    count++;
                }
                stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(count, stats.CountOfCompareExchange);

                list = ConcatStringInList(list);
                var indexesList = new List<long>();
                foreach (var e in list)
                {
                    var user = new User
                    {
                        Name = $"emoji_{count}"
                    };
                    var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>($"emojis/{e}", user, 0));
                    indexesList.Add(res.Index);
                    count++;
                }
                stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(count, stats.CountOfCompareExchange);
                Assert.Equal(list.Count, indexesList.Count);

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Backup
                };

                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP

                for (var i = 0; i < list.Count; i++)
                {
                    if (list[i].Length == 8)
                    {
                        var res = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>($"emojis/{list[i]}", indexesList[i]));
                        if (res.Value != null)
                            count--;
                    }
                }

                list = ConcatStringInList(list);
                foreach (var e in list)
                {
                    var user = new User
                    {
                        Name = $"emoji_{count}"
                    };
                    await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>($"emojis/{e}", user, 0));
                    count++;
                }
                stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(count, stats.CountOfCompareExchange);

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 1

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
                    stats = store2.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(count, stats.CountOfCompareExchange);
                }
            }
        }

        [Fact]
        public async Task CreateFullAndIncrementalBackupWithIdentity()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var bestUser = new User
                    {
                        Name = "Egor"
                    };
                    await session.StoreAsync(bestUser, "users|");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Backup
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Egor2"
                    }, "users|");
                    await session.SaveChangesAsync();
                }

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL

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
                        await session.StoreAsync(new User
                        {
                            Name = "Egor3"
                        }, "users|");
                        await session.SaveChangesAsync();

                        var bestUser = await session.LoadAsync<User>("users/1");
                        var mediocreUser1 = await session.LoadAsync<User>("users/2");
                        var mediocreUser2 = await session.LoadAsync<User>("users/3");

                        Assert.NotNull(bestUser);
                        Assert.NotNull(mediocreUser1);
                        Assert.NotNull(mediocreUser2);

                        Assert.Equal("Egor", bestUser.Name);
                        Assert.Equal("Egor2", mediocreUser1.Name);
                        Assert.Equal("Egor3", mediocreUser2.Name);
                    }
                }
            }
        }

        [Fact]
        public async Task CreateFullAndIncrementalBackupWithIdentityAndRestoreOnlyIncremental()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var bestUser = new User
                    {
                        Name = "Egor"
                    };
                    await session.StoreAsync(bestUser, "users|");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Backup
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Egor2"
                    }, "users|");
                    await session.SaveChangesAsync();
                }

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                var files = Directory.GetFiles(backupDirectory)
                    .Where(BackupUtils.IsBackupFile)
                    .OrderBackups()
                    .ToArray();

                File.Delete(files.First());

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
                        await session.StoreAsync(new User
                        {
                            Name = "Egor3"
                        }, "users|");
                        await session.SaveChangesAsync();

                        var bestUser = await session.LoadAsync<User>("users/1");
                        var mediocreUser1 = await session.LoadAsync<User>("users/2");
                        var mediocreUser2 = await session.LoadAsync<User>("users/3");

                        var stats = store2.Maintenance.ForDatabase(databaseName).Send(new GetDetailedStatisticsOperation());
                        Assert.Equal(1, stats.CountOfIdentities);
                        Assert.Equal(2, stats.CountOfDocuments);

                        Assert.Null(bestUser);
                        Assert.NotNull(mediocreUser1);
                        Assert.NotNull(mediocreUser2);

                        Assert.Equal("Egor2", mediocreUser1.Name);
                        Assert.Equal("Egor3", mediocreUser2.Name);
                    }
                }
            }
        }

        [Fact]
        public void AllCompareExchangeAndIdentitiesPreserveAfterSchemaUpgrade()
        {
            var folder = NewDataPath(forceCreateDir: true);
            DoNotReuseServer();

            var zipPath = new PathSetting("SchemaUpgrade/Issues/SystemVersion/Identities_CompareExchange_RavenData.zip");
            Assert.True(File.Exists(zipPath.FullPath));

            ZipFile.ExtractToDirectory(zipPath.FullPath, folder);

            using (var server = GetNewServer(new ServerCreationOptions {DeletePrevious = false, RunInMemory = false, DataDirectory = folder, RegisterForDisposal = false}))
            {
                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var dbs = server.ServerStore.Cluster.GetDatabaseNames(context);
                    var dbsList = dbs.ToList();

                    Assert.Equal(2, dbsList.Count);
                    var dbName2 = dbsList[0];
                    Assert.Equal("demo", dbName2);
                    var dbName1 = dbsList[1];
                    Assert.Equal("testoso", dbName1);

                    var numOfIdentities = server.ServerStore.Cluster.GetNumberOfIdentities(context, dbName1);
                    Assert.Equal(2, numOfIdentities);
                    numOfIdentities = server.ServerStore.Cluster.GetNumberOfIdentities(context, dbName2);
                    Assert.Equal(1, numOfIdentities);

                    var numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, dbName1);
                    Assert.Equal(3, numOfCompareExchanges);
                    numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, dbName2);
                    Assert.Equal(2, numOfCompareExchanges);
                }
            }
        }

        [Fact]
        public async Task CreateSnapshotBackupWithCompareExchangeAndIdentity()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var user = new User
                {
                    Name = "💩"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/poo", user, 0));

                using (var session = store.OpenAsyncSession())
                {
                    var bestUser = new User
                    {
                        Name = "Egor"
                    };
                    await session.StoreAsync(bestUser, "users|");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "snapshot",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Snapshot
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, true, store);    // Snapshot BACKUP

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).OrderBackups().Last()
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
                        await session.StoreAsync(new User
                        {
                            Name = "Egor2"
                        }, "users|");
                        await session.SaveChangesAsync();

                        var bestUser = await session.LoadAsync<User>("users/1");
                        var mediocreUser = await session.LoadAsync<User>("users/2");
                        Assert.NotNull(bestUser);
                        Assert.NotNull(mediocreUser);

                        Assert.Equal("Egor", bestUser.Name);
                        Assert.Equal("Egor2", mediocreUser.Name);

                        var user1 = (await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("emojis/poo"));
                        Assert.Equal(user.Name, user1.Value.Name);
                    }
                }
            }
        }

        [Fact]
        public async Task CreateSnapshotAndIncrementalBackupWithCompareExchangeAndIdentity()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var user1 = new User
                {
                    Name = "💩"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/poo", user1, 0));

                using (var session = store.OpenAsyncSession())
                {
                    var bestUser = new User
                    {
                        Name = "Egor"
                    };
                    await session.StoreAsync(bestUser, "users|");
                    await session.SaveChangesAsync();
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "snapshot",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Snapshot
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, true, store);    // Snapshot BACKUP


                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Egor2"
                    }, "users|");
                    await session.SaveChangesAsync();
                }

                var user2 = new User
                {
                    Name = "💩🤡"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/pooclown", user2, 0));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).OrderBackups().Last()//if we don't sort the backups, we may get incorrect 'last' item to restore
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
                        await session.StoreAsync(new User
                        {
                            Name = "Egor3"
                        }, "users|");
                        await session.SaveChangesAsync();
                        
                        var bestUser = await session.LoadAsync<User>("users/1");
                        var mediocreUser1 = await session.LoadAsync<User>("users/2");
                        var mediocreUser2 = await session.LoadAsync<User>("users/3");
                        Assert.NotNull(bestUser);
                        Assert.NotNull(mediocreUser1);
                        Assert.NotNull(mediocreUser2);

                        Assert.Equal("Egor", bestUser.Name);
                        Assert.Equal("Egor2", mediocreUser1.Name);
                        Assert.Equal("Egor3", mediocreUser2.Name);


                        var user11 = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("emojis/poo");
                        Assert.Equal(user1.Name, user11.Value.Name);
                        var user22 = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("emojis/pooclown");
                        Assert.Equal(user2.Name, user22.Value.Name);
                    }
                }
            }
        }

        [Fact]
        public async Task CreateSnapshotAndIncrementalBackupsWithCompareExchangeAndIdentityAndDeleteBetween()
        {
            var list = new List<string>(new[] {"🐃", "🐂", "🐄", "🐎", "🐖",
                                                "🐏", "🐑", "🐐", "🦌", "🐕",
                                                "🐩", "🐈", "🐓", "🦃", "🕊",
                                                "🐇", "🐁", "🐀", "🐿", "🦔"});

            int userId = 1;

            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var count = 0;
                foreach (var e in list)
                {
                    var user = new User
                    {
                        Name = $"emoji_{count}"
                    };
                    await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>($"emojis/{e}", user, 0));
                    count++;
                }
                var stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(count, stats.CountOfCompareExchange);

                list = ConcatStringInList(list);
                foreach (var e in list)
                {
                    var user = new User
                    {
                        Name = $"emoji_{count}"
                    };
                    await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>($"emojis/{e}", user, 0));
                    count++;
                }
                stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(count, stats.CountOfCompareExchange);

                list = ConcatStringInList(list);
                var indexesList = new List<long>();
                foreach (var e in list)
                {
                    var user = new User
                    {
                        Name = $"emoji_{count}"
                    };
                    var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>($"emojis/{e}", user, 0));
                    indexesList.Add(res.Index);
                    count++;
                }
                stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(count, stats.CountOfCompareExchange);
                Assert.Equal(list.Count, indexesList.Count);

                using (var session = store.OpenAsyncSession())
                {
                    var user = new User
                    {
                        Name = $"Egor_{userId}"
                    };
                    await session.StoreAsync(user, "users|");
                    await session.SaveChangesAsync();
                    userId++;
                }

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "snapshot",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Snapshot
                };

                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, true, store);    // Snapshot BACKUP

                for (var i = 0; i < list.Count; i++)
                {
                    if (list[i].Length == 8)
                    {
                        var res = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>($"emojis/{list[i]}", indexesList[i]));
                        if (res.Value != null)
                            count--;
                    }
                }
                stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(count, stats.CountOfCompareExchange);

                using (var session = store.OpenAsyncSession())
                {
                    var user = new User
                    {
                        Name = $"Egor_{userId}"
                    };
                    await session.StoreAsync(user, "users|");
                    await session.SaveChangesAsync();
                    userId++;
                }

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 1

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).OrderBackups().Last()
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
                    stats = store2.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                    Assert.Equal(count, stats.CountOfCompareExchange);


                    using (var session = store.OpenAsyncSession())
                    {
                        var user = new User
                        {
                            Name = $"Egor_{userId}"
                        };
                        await session.StoreAsync(user, "users|");
                        await session.SaveChangesAsync();

                        var user3 = await session.LoadAsync<User>($"users/{userId}");
                        userId--;
                        var user2 = await session.LoadAsync<User>($"users/{userId}");
                        userId--;
                        var user1 = await session.LoadAsync<User>($"users/{userId}");

                        Assert.NotNull(user1);
                        Assert.NotNull(user2);
                        Assert.NotNull(user3);

                        Assert.Equal("Egor_3", user3.Name);
                        Assert.Equal("Egor_2", user2.Name);
                        Assert.Equal("Egor_1", user1.Name);
                    }
                }
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(1024)]
        public async Task CompareExchangeTombstonesShouldBeClearedAfterBackup(int number)
        {
            var list = new List<string>(new[] { "🐃", "🐂", "🐄", "🐎", "🐖",
                                                "🐏", "🐑", "🐐", "🦌", "🐕",
                                                "🐩", "🐈", "🐓", "🦃", "🕊",
                                                "🐇", "🐁", "🐀", "🐿", "🦔" });

            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options { Server = server }))
            {
                WaitForFirstCompareExchangeTombstonesClean(server);
                var count = 1;
                var indexesList = new List<long>();

                for (int i = 0; i < number; i++)
                {
                    var k = i % 20;
                    var user = new User
                    {
                        Name = $"emoji_{i}"
                    };
                    var str = "";
                    for (int j = 0; j < count; j++)
                    {

                        str += list[k];
                    }

                    var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>($"emojis/{str}", user, 0));
                    indexesList.Add(res.Index);

                    if (k == 0)
                        count++;
                }

                var stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(number, stats.CountOfCompareExchange);
                Assert.Equal(number, indexesList.Count);

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Backup
                };

                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                Assert.NotNull(documentDatabase);
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, true, store); // FULL BACKUP

                var delCount = 0;
                var allCount = number;
                count = 1;
                for (var i = 0; i < number; i++)
                {
                    var k = i % 20;

                    var str = "";
                    for (int j = 0; j < count; j++)
                    {
                        str += list[k];
                    }

                    if (k < 10)
                    {
                        var res = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>($"emojis/{str}", indexesList[i]));
                        if (res.Value != null)
                        {
                            delCount++;
                            allCount--;
                        }
                    }

                    if (k == 0)
                        count++;
                }

                Assert.True(delCount > 0);

                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);
                    var numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, store.Database);

                    Assert.Equal(delCount, numOfCompareExchangeTombstones);
                    Assert.Equal(allCount, numOfCompareExchanges);
                }

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store); // INCREMENTAL 1

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store); // INCREMENTAL 2

                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    // clean tombstones
                    await server.ServerStore.Observer.CleanUpCompareExchangeTombstones(store.Database, context);
                }

                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);
                    var numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, store.Database);

                    Assert.Equal(0, numOfCompareExchangeTombstones);
                    Assert.Equal(allCount, numOfCompareExchanges);
                }
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(1024)]
        public async Task CompareExchangeTombstonesShouldBeClearedIfThereIsNoIncrementalBackup(int number)
        {
            var list = new List<string>(new[] { "🐃", "🐂", "🐄", "🐎", "🐖",
                                                "🐏", "🐑", "🐐", "🦌", "🐕",
                                                "🐩", "🐈", "🐓", "🦃", "🕊",
                                                "🐇", "🐁", "🐀", "🐿", "🦔" });

            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options { Server = server }))
            {
                WaitForFirstCompareExchangeTombstonesClean(server);

                var count = 1;
                var indexesList = new List<long>();

                for (int i = 0; i < number; i++)
                {
                    var k = i % 20;
                    var user = new User
                    {
                        Name = $"emoji_{i}"
                    };
                    var str = "";
                    for (int j = 0; j < count; j++)
                    {

                        str += list[k];
                    }

                    var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>($"emojis/{str}", user, 0));
                    indexesList.Add(res.Index);

                    if (k == 0)
                        count++;
                }

                var stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(number, stats.CountOfCompareExchange);
                Assert.Equal(number, indexesList.Count);

                var delCount = 0;
                var allCount = number;
                count = 1;
                for (var i = 0; i < number; i++)
                {
                    var k = i % 20;

                    var str = "";
                    for (int j = 0; j < count; j++)
                    {
                        str += list[k];
                    }

                    if (k < 10)
                    {
                        var res = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>($"emojis/{str}", indexesList[i]));
                        if (res.Value != null)
                        {
                            delCount++;
                            allCount--;
                        }
                    }

                    if (k == 0)
                        count++;
                }

                Assert.True(delCount > 0);

                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);
                    var numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, store.Database);

                    Assert.Equal(delCount, numOfCompareExchangeTombstones);
                    Assert.Equal(allCount, numOfCompareExchanges);
                }

                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    // clean tombstones
                    await server.ServerStore.Observer.CleanUpCompareExchangeTombstones(store.Database, context);
                }

                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);
                    var numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, store.Database);

                    Assert.Equal(0, numOfCompareExchangeTombstones);
                    Assert.Equal(allCount, numOfCompareExchanges);
                }
            }
        }

        [Fact]
        public async Task CompareExchangeTombstonesShouldBeClearedWhenThereIsOnlyFullBackup()
        {
            var list = new List<string>(new[] { "🐃", "🐂", "🐄", "🐎", "🐖",
                                                "🐏", "🐑", "🐐", "🦌", "🐕",
                                                "🐩", "🐈", "🐓", "🦃", "🕊",
                                                "🐇", "🐁", "🐀", "🐿", "🦔" });

            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options { Server = server }))
            {
                WaitForFirstCompareExchangeTombstonesClean(server);

                var indexesList = new Dictionary<string, long>();
                // create 3 unique values
                for (int i = 0; i < 3; i++)
                {
                    var key = $"emojis/{list[i]}";
                    var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>(key, new User { Name = $"emoji_{i}" }, 0));
                    indexesList.Add(key, res.Index);
                }

                var stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(3, stats.CountOfCompareExchange);

                // delete 1 unique value
                var k = $"emojis/{list[2]}";
                var del = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>(k, indexesList[k]));
                Assert.NotNull(del.Value);
                indexesList.Remove(k);

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Backup
                };

                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, recursive: true);

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                Assert.NotNull(documentDatabase);
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, isFullBackup: true, store); // FULL BACKUP

                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);
                    Assert.Equal(1, numOfCompareExchangeTombstones);
                    await server.ServerStore.Observer.CleanUpCompareExchangeTombstones(store.Database, context);
                }
                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);
                    var numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, store.Database);
                    Assert.Equal(0, numOfCompareExchangeTombstones);
                    Assert.Equal(2, numOfCompareExchanges);
                }

                // add 1 cmpxchng
                var uniqueValueKey = $"emojis/{list[3]}";
                var res1 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>(uniqueValueKey, new User { Name = $"emoji_3" }, 0));
                indexesList.Add(uniqueValueKey, res1.Index);

                // delete 1 unique value
                k = $"emojis/{list[1]}";
                var del1 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>(k, indexesList[k]));
                Assert.NotNull(del1.Value);
                indexesList.Remove(k);

                config.IncrementalBackupFrequency = "0 0 1 1 *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL

                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);
                    Assert.Equal(1, numOfCompareExchangeTombstones);
                    // clean tombstones
                    await server.ServerStore.Observer.CleanUpCompareExchangeTombstones(store.Database, context);
                }
                using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);
                    var numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, store.Database);
                    Assert.Equal(0, numOfCompareExchangeTombstones);
                    Assert.Equal(2, numOfCompareExchanges);
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "egor"
                    });
                    session.SaveChanges();
                }
                PeriodicBackupTestsSlow.RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                AssertDumpFiles(backupDirectory, list);
            }
        }

        private void AssertDumpFiles(string backupPath, List<string> list)
        {
            var files = Directory.GetFiles(backupPath)
                .Where(BackupUtils.IsBackupFile)
                .OrderBackups()
                .ToArray();

            for (int i = 0; i < files.Length; i++)
            {
                Assert.True(File.Exists(files[i]));
                var file = new FileInfo(files[i]);
                using (FileStream fs = file.OpenRead())
                {
                    using (var stream = new MemoryStream())
                    using (var gz = new GZipStream(fs, CompressionMode.Decompress))
                    {
                        gz.CopyTo(stream);
                        stream.Position = 0;

                        using (var ctx = JsonOperationContext.ShortTermSingleUse())
                        using (var bjro = ctx.ReadForMemory(stream, "test"))
                        {
                            Assert.True(bjro.TryGet(nameof(DatabaseItemType.CompareExchange), out BlittableJsonReaderArray uniqueValues));
                            Assert.True(bjro.TryGet(nameof(DatabaseItemType.CompareExchangeTombstones), out BlittableJsonReaderArray tombstones));

                            switch (i)
                            {
                                case 0:
                                    Assert.Equal(2, uniqueValues.Length);
                                    Assert.Equal(1, tombstones.Length);

                                    for (int j = 0; j < uniqueValues.Length; j++)
                                    {
                                        var obj = uniqueValues[i] as BlittableJsonReaderObject;
                                        Assert.NotNull(obj);
                                        Assert.True(obj.TryGet("Key", out string key));
                                        Assert.True(key == $"emojis/{list[0]}" || key == $"emojis/{list[1]}");
                                    }
                                    var obj2 = tombstones[0] as BlittableJsonReaderObject;
                                    Assert.NotNull(obj2);
                                    Assert.True(obj2.TryGet("Key", out string key2));
                                    Assert.Equal($"emojis/{list[2]}", key2);
                                    break;
                                case 1:
                                    Assert.Equal(1, uniqueValues.Length);
                                    Assert.Equal(1, tombstones.Length);

                                    var obj4 = uniqueValues[0] as BlittableJsonReaderObject;
                                    Assert.NotNull(obj4);
                                    Assert.True(obj4.TryGet("Key", out string key4));
                                    Assert.Equal(key4, $"emojis/{list[3]}");
                                    var obj3 = tombstones[0] as BlittableJsonReaderObject;
                                    Assert.NotNull(obj3);
                                    Assert.True(obj3.TryGet("Key", out string key3));
                                    Assert.Equal($"emojis/{list[1]}", key3);
                                    break;
                                case 2:
                                    Assert.Equal(0, uniqueValues.Length);
                                    Assert.Equal(0, tombstones.Length);
                                    break;
                                default:
                                    Assert.True(false);
                                    break;
                            }
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task TombstoneCleanerShouldNotClearIfAnyBackupIsErroredOnFirstRun()
        {
            var backupPath1 = NewDataPath(suffix: "BackupFolder1");
            var backupPath2 = NewDataPath(suffix: "BackupFolder2");
            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options { Server = server }))
            {
                WaitForFirstCompareExchangeTombstonesClean(server);
                var indexesList = new Dictionary<string, long>();
                // create 3 unique values
                for (int i = 0; i < 3; i++)
                {
                    var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<int>($"{i}", i, 0));
                    indexesList.Add($"{i}", res.Index);
                }

                // delete 1 unique value
                var del = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<int>("2", indexesList["2"]));
                Assert.NotNull(del.Value);
                indexesList.Remove("2");

                // full backup without incremental
                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath1
                    },
                    Name = "full error",
                    IncrementalBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Backup
                };

                if (Directory.Exists(backupPath1))
                    Directory.Delete(backupPath1, recursive: true);

                var result1 = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                Assert.NotNull(documentDatabase);

                config.LocalSettings.FolderPath = backupPath2;
                config.Name = "backupPath2";
                if (Directory.Exists(backupPath2))
                    Directory.Delete(backupPath2, recursive: true);

                var result2 = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                try
                {
                    documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateFailedBackup = true;
                    PeriodicBackupTestsSlow.RunBackup(result1.TaskId, documentDatabase, isFullBackup: true, store, OperationStatus.Faulted); // FULL Faulted BACKUP
                    documentDatabase.PeriodicBackupRunner._forTestingPurposes = null;
                    PeriodicBackupTestsSlow.RunBackup(result2.TaskId, documentDatabase, isFullBackup: true, store); // FULL BACKUP

                    using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);
                        Assert.Equal(1, numOfCompareExchangeTombstones);
                        // clean tombstones
                        await server.ServerStore.Observer.CleanUpCompareExchangeTombstones(store.Database, context);
                    }
                    using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);
                        var numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, store.Database);
                        Assert.Equal(1, numOfCompareExchangeTombstones);
                        Assert.Equal(2, numOfCompareExchanges);
                    }
                }
                finally
                {
                    documentDatabase.PeriodicBackupRunner._forTestingPurposes = null;
                }
            }
        }

        [Fact]
        public async Task TombstoneCleanerShouldClearUpToLastRaftIndexIfLastBackupIsErrored()
        {
            var backupPath1 = NewDataPath(suffix: "BackupFolder1");
            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options { Server = server }))
            {
                WaitForFirstCompareExchangeTombstonesClean(server);
                var indexesList = new Dictionary<string, long>();
                // create 3 unique values
                for (int i = 0; i < 3; i++)
                {
                    var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<int>($"{i}", i, 0));
                    indexesList.Add($"{i}", res.Index);
                }

                // delete 1 unique value
                var del = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<int>("2", indexesList["2"]));
                Assert.NotNull(del.Value);
                indexesList.Remove("2");

                // full backup without incremental
                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath1
                    },
                    Name = "full",
                    IncrementalBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Backup
                };

                if (Directory.Exists(backupPath1))
                    Directory.Delete(backupPath1, recursive: true);

                var result1 = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                Assert.NotNull(documentDatabase);

                try
                {
                    PeriodicBackupTestsSlow.RunBackup(result1.TaskId, documentDatabase, isFullBackup: true, store); // FULL BACKUP
                    documentDatabase.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateFailedBackup = true;
                    PeriodicBackupTestsSlow.RunBackup(result1.TaskId, documentDatabase, isFullBackup: true, store, OperationStatus.Faulted); // FULL Faulted BACKUP

                    using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);
                        Assert.Equal(1, numOfCompareExchangeTombstones);
                        // clean tombstones
                        await server.ServerStore.Observer.CleanUpCompareExchangeTombstones(store.Database, context);
                    }
                    using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var numOfCompareExchangeTombstones = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, store.Database);
                        var numOfCompareExchanges = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, store.Database);
                        Assert.Equal(0, numOfCompareExchangeTombstones);
                        Assert.Equal(2, numOfCompareExchanges);
                    }
                }
                finally
                {
                    documentDatabase.PeriodicBackupRunner._forTestingPurposes = null;
                }
            }
        }

        private static List<string> ConcatStringInList(List<string> list)
        {
            for (var i = 0; i < list.Count; i++)
            {
                var e = list[i];
                e += e;
                list[i] = e;
            }

            return list;
        }

        private void WaitForFirstCompareExchangeTombstonesClean(RavenServer server)
        {
            Assert.True(WaitForValue(() =>
            {
                // wait for compare exchange tombstone cleaner run
                if (server.ServerStore.Observer == null)
                    return false;

                if (server.ServerStore.Observer._lastTombstonesCleanupTimeInTicks == 0)
                    return false;

                return true;
            }, true));
        }
    }
}
