using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class RavenDB_11139 : RavenTestBase
    {
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
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/poo", user, 0));

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

                var user2 = new User
                {
                    Name = "💩🤡"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/pooclown", user2, 0));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).Last()
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
                    FullBackupFrequency = "* */6 * * *",
                    BackupType = BackupType.Backup
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP

                var user2 = new User
                {
                    Name = "💩🤡"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/pooclown", user2, 0));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";
                File.Delete(Directory.GetFiles(backupDirectory).First());
                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).Last()
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

                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP

                var user2 = new User
                {
                    Name = "🤡"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/clown", user2, 0));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL1

                var user3 = new User
                {
                    Name = "👺"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/goblin", user3, 0));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL2

                var user4 = new User
                {
                    Name = "👻"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/ghost", user4, 0));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL3

                var user5 = new User
                {
                    Name = "🤯"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/exploding_head", user5, 0));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL4

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                DeleteFilesByExtension(".ravendb-full-backup", backupDirectory);     // delete full backup file         

                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).Last()
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
                        var stats = store2.Maintenance.ForDatabase(databaseName).Send(new GetDetailedStatisticsOperation());
                        Assert.Equal(4, stats.CountOfCompareExchange);

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
                    FullBackupFrequency = "* */6 * * *",
                    BackupType = BackupType.Backup
                };

                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP

                var user2 = new User
                {
                    Name = "clown"
                };
                var clownResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/🤡", user2, 0));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 1

                // delete poo
                await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("emojis/💩", pooResult.Index));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 2

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";
                RestoreBackupConfiguration restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).Last()
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
                    FullBackupFrequency = "* */6 * * *",
                    BackupType = BackupType.Backup
                };

                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP

                var user2 = new User
                {
                    Name = "clown"
                };
                var clownResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/🤡", user2, 0));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 1

                // delete poo
                await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("emojis/💩", pooResult.Index));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 2

                var user3 = new User
                {
                    Name = "PirateFlag"
                };
                var pirateFlagResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/🏴‍☠️", user3, 0));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 3

                // delete clown
                await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("emojis/🤡", clownResult.Index));

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 4

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).Last()
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
                    FullBackupFrequency = "* */6 * * *",
                    BackupType = BackupType.Backup
                };

                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP

                for (var i = 0; i < list.Count; i++)
                {
                    if (list[i].Length == 8)
                    {
                        var res = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>($"emojis/{list[i]}", indexesList[i]));
                        if(res.Value != null)
                            count--;
                    }
                }
                stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(count, stats.CountOfCompareExchange);

                config.IncrementalBackupFrequency = "* */300 * * *";
                config.TaskId = result.TaskId;
                result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 1

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).Last()
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
                    FullBackupFrequency = "* */6 * * *",
                    BackupType = BackupType.Backup
                };

                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP

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
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL 1

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).Last()
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
                    FullBackupFrequency = "* */6 * * *",
                    BackupType = BackupType.Backup
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP
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
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).Last()
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
                    FullBackupFrequency = "* */6 * * *",
                    BackupType = BackupType.Backup
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));
                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                RunBackup(result.TaskId, documentDatabase, true, store);    // FULL BACKUP
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
                RunBackup(result.TaskId, documentDatabase, false, store);   // INCREMENTAL

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";
                File.Delete(Directory.GetFiles(backupDirectory).First());

                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).Last()
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
            Assert.Equal(true, File.Exists(zipPath.FullPath));
            Assert.Equal(FileAttributes.Archive, File.GetAttributes(zipPath.FullPath));

            ZipFile.ExtractToDirectory(zipPath.FullPath, folder);

            using (var server = GetNewServer(deletePrevious: false, runInMemory: false, partialPath: folder))
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

        private static void DeleteFilesByExtension(string extension, string backupFolderPath)
        {
            var di = new DirectoryInfo(backupFolderPath);
            var files = di.GetFiles($"*{extension}")
                .Where(p => p.Extension == $"{extension}").ToArray();
            foreach (var file in files)
                try
                {
                    file.Attributes = FileAttributes.Normal;
                    File.Delete(file.FullName);
                }
                catch
                {
                    throw new IOException($"Could not remove files from {backupFolderPath} with extension {extension}");
                }
        }

        private void RunBackup(long taskId, Raven.Server.Documents.DocumentDatabase documentDatabase, bool isFullBackup, DocumentStore store)
        {
            var periodicBackupRunner = documentDatabase.PeriodicBackupRunner;
            var op = periodicBackupRunner.StartBackupTask(taskId, isFullBackup);
            var value = WaitForValue(() =>
            {
                var status = store.Maintenance.Send(new GetOperationStateOperation(op)).Status;
                return status;
            }, OperationStatus.Completed);

            Assert.Equal(OperationStatus.Completed, value);
        }
    }
}
