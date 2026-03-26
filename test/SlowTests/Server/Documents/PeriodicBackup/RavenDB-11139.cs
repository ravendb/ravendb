using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class RavenDB_11139 : ClusterTestBase
    {
        private readonly ITestOutputHelper _output;

        public RavenDB_11139(ITestOutputHelper output) : base(output)
        {
            _output = output;
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
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
                Assert.True(operationResult.Successful, "Failing early because the test will fail anyways - the PutCompareExchangeValueOperation failed...");

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                var user2 = new User
                {
                    Name = "💩🤡"
                };

                operationResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/pooclown", user2, 0));
                Assert.True(operationResult.Successful, "Failing early because the test will fail anyways - the PutCompareExchangeValueOperation failed...");

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);
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
                await (await store.Maintenance.Server.SendAsync(restoreOperation))
                    .WaitForCompletionAsync(TimeSpan.FromSeconds(30));

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

        [RavenFact(RavenTestCategory.BackupExportImport)]
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

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                var user2 = new User
                {
                    Name = "💩🤡"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/pooclown", user2, 0));

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

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
                await (await store.Maintenance.Server.SendAsync(restoreOperation))
                    .WaitForCompletionAsync(TimeSpan.FromSeconds(30));

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

        [RavenFact(RavenTestCategory.BackupExportImport)]
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

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                var user2 = new User
                {
                    Name = "🤡"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/clown", user2, 0));
                var val2 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>("emojis/clown"));
                Assert.True(user2.Name == val2.Value.Name, "val.Value.Name = 'emojis/clown'");


                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

                var user3 = new User
                {
                    Name = "👺"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/goblin", user3, 0));
                var val3 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>("emojis/goblin"));
                Assert.True(user3.Name == val3.Value.Name, "val.Value.Name = 'emojis/goblin'");

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

                var user4 = new User
                {
                    Name = "👻"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/ghost", user4, 0));
                var val4 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>("emojis/ghost"));
                Assert.True(user4.Name == val4.Value.Name, "val.Value.Name = 'emojis/ghost'");

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

                var user5 = new User
                {
                    Name = "🤯"
                };
                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/exploding_head", user5, 0));
                var val5 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<User>("emojis/exploding_head"));
                Assert.True(user5.Name == val5.Value.Name, "val.Value.Name = 'emojis/exploding_head'");

                var emojisNum = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation()).CountOfCompareExchange;
                Assert.True(emojisNum == 5, "CountOfCompareExchange == 5");

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

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
                await (await store.Maintenance.Server.SendAsync(restoreOperation))
                    .WaitForCompletionAsync(TimeSpan.FromSeconds(30));

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

        [RavenFact(RavenTestCategory.BackupExportImport)]
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

                var config = Backup.CreateBackupConfiguration(backupPath);
                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                var user2 = new User
                {
                    Name = "clown"
                };
                var clownResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/🤡", user2, 0));
                Assert.True(clownResult.Successful);

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

                // delete poo
                await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("emojis/💩", pooResult.Index));

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

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
                await (await store.Maintenance.Server.SendAsync(restoreOperation))
                    .WaitForCompletionAsync(TimeSpan.FromSeconds(30));

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

        [RavenFact(RavenTestCategory.BackupExportImport)]
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
                var config = Backup.CreateBackupConfiguration(backupPath);

                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                var user2 = new User
                {
                    Name = "clown"
                };
                var clownResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/🤡", user2, 0));

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

                // delete poo
                await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("emojis/💩", pooResult.Index));

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

                var user3 = new User
                {
                    Name = "PirateFlag"
                };
                var pirateFlagResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("emojis/🏴‍☠️", user3, 0));

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

                // delete clown
                await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("emojis/🤡", clownResult.Index));

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

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
                await (await store.Maintenance.Server.SendAsync(restoreOperation))
                    .WaitForCompletionAsync(TimeSpan.FromSeconds(30));

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

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task CreateFullAndIncrementalBackupWithCompareExchangesAndDeleteBetween()
        {
            var list = new List<string>(new[] {"🐃", "🐂", "🐄", "🐎", "🐖",
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

                var config = Backup.CreateBackupConfiguration(backupPath);
                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

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

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

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
                await (await store.Maintenance.Server.SendAsync(restoreOperation))
                    .WaitForCompletionAsync(TimeSpan.FromSeconds(30));

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

        [RavenFact(RavenTestCategory.BackupExportImport)]
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

                var config = Backup.CreateBackupConfiguration(backupPath);
                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

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

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

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
                await (await store.Maintenance.Server.SendAsync(restoreOperation))
                    .WaitForCompletionAsync(TimeSpan.FromSeconds(30));

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

        [RavenFact(RavenTestCategory.BackupExportImport)]
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
                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Egor2"
                    }, "users|");
                    await session.SaveChangesAsync();
                }

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

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
                await (await store.Maintenance.Server.SendAsync(restoreOperation))
                    .WaitForCompletionAsync(TimeSpan.FromSeconds(30));

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

        [RavenFact(RavenTestCategory.BackupExportImport)]
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

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Egor2"
                    }, "users|");
                    await session.SaveChangesAsync();
                }

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

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
                await (await store.Maintenance.Server.SendAsync(restoreOperation))
                    .WaitForCompletionAsync(TimeSpan.FromSeconds(30));

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

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public void AllCompareExchangeAndIdentitiesPreserveAfterSchemaUpgrade()
        {
            var folder = NewDataPath(forceCreateDir: true);
            DoNotReuseServer();

            var zipPath = new PathSetting("SchemaUpgrade/Issues/SystemVersion/Identities_CompareExchange_RavenData.zip");
            Assert.True(File.Exists(zipPath.FullPath));

            ZipFile.ExtractToDirectory(zipPath.FullPath, folder);

            using (var server = GetNewServer(new ServerCreationOptions { DeletePrevious = false, RunInMemory = false, DataDirectory = folder, RegisterForDisposal = false }))
            {
                using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
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

        [RavenFact(RavenTestCategory.BackupExportImport)]
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

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).OrderBackups().Last()
                };

                var restoreOperation = new RestoreBackupOperation(restoreConfig);
                await (await store.Maintenance.Server.SendAsync(restoreOperation))
                    .WaitForCompletionAsync(TimeSpan.FromSeconds(30));

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

        [RavenFact(RavenTestCategory.BackupExportImport)]
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

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

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

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).OrderBackups().Last()//if we don't sort the backups, we may get incorrect 'last' item to restore
                };

                var restoreOperation = new RestoreBackupOperation(restoreConfig);
                await (await store.Maintenance.Server.SendAsync(restoreOperation))
                    .WaitForCompletionAsync(TimeSpan.FromSeconds(30));

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

        [RavenFact(RavenTestCategory.BackupExportImport)]
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

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                if (Directory.Exists(backupPath))
                    Directory.Delete(backupPath, true);

                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

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

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

                var backupDirectory = Directory.GetDirectories(backupPath).First();
                var databaseName = GetDatabaseName() + "restore";

                var restoreConfig = new RestoreBackupConfiguration()
                {
                    BackupLocation = backupDirectory,
                    DatabaseName = databaseName,
                    LastFileNameToRestore = Directory.GetFiles(backupDirectory).OrderBackups().Last()
                };

                var restoreOperation = new RestoreBackupOperation(restoreConfig);
                await (await store.Maintenance.Server.SendAsync(restoreOperation))
                    .WaitForCompletionAsync(TimeSpan.FromSeconds(30));

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

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task CompareExchangeTombstoneCleaner_ShouldCleanUp_WithoutBackupTasks()
        {
            var diagnosticLogBuilder = new StringBuilder();
            var serverCreationOptions = new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "100" } }
            };

            using var server = GetNewServer(serverCreationOptions);
            using var store = GetDocumentStore(new Options { Server = server });

            server.ServerStore.Observer.ForTestingPurposesOnly().OnDiagnosticLog += logLine => diagnosticLogBuilder.AppendLine($"[{DateTime.Now:O}] {logLine}");
            server.ServerStore.ForTestingPurposesOnly().IgnoreClusterTransactionIndexInCompareExchangeCleaner = true;
            Cluster.WaitForFirstCompareExchangeTombstonesClean(server);

            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/1", 1, 0));
            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/2", 1, 0));
            await CreateCompareExchangeTombstone(store, "cx/3");

            AssertCompareExchangeCounts(server, store.Database, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 2, "Before compare exchange tombstone cleanup", diagnosticLogBuilder);
            await CompareExchangeTombstoneCleanerTestHelper.Clean(nodes: [server], store.Database, ignoreClustrTrx: true);
            AssertCompareExchangeCounts(server, store.Database, expectedTombstonesNumber: 0, expectedCompareExchangeNumber: 2, "After compare exchange tombstone cleanup", diagnosticLogBuilder);
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.CompareExchange)]
        public async Task CompareExchangeTombstoneCleaner_ShouldCleanUp_WithBackupTask_TombstonesCreatedBeforeBackupTaskCreation()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var diagnosticLogBuilder = new StringBuilder();
            var serverCreationOptions = new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "100" } }
            };

            using var server = GetNewServer(serverCreationOptions);
            using var store = GetDocumentStore(new Options { Server = server});

            server.ServerStore.Observer.ForTestingPurposesOnly().OnDiagnosticLog += logLine => diagnosticLogBuilder.AppendLine($"[{DateTime.Now:O}] {logLine}");
            server.ServerStore.ForTestingPurposesOnly().IgnoreClusterTransactionIndexInCompareExchangeCleaner = true;
            Cluster.WaitForFirstCompareExchangeTombstonesClean(server);

            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/1", 1, 0));
            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/2", 1, 0));
            await CreateCompareExchangeTombstone(store, "cx/3");

            AssertCompareExchangeCounts(server, store.Database, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 2, "Before compare exchange tombstone cleanup", diagnosticLogBuilder);

            var backupConfiguration = Backup.CreateBackupConfiguration(backupPath, name: "FirstBackupConfiguration");
            _ = await Backup.UpdateConfigAsync(server, backupConfiguration, store);

            await CompareExchangeTombstoneCleanerTestHelper.Clean(nodes: [server], store.Database, ignoreClustrTrx: true);
            AssertCompareExchangeCounts(server, store.Database, expectedTombstonesNumber: 0, expectedCompareExchangeNumber: 2, "After compare exchange tombstone cleanup", diagnosticLogBuilder);
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task CompareExchangeTombstoneCleaner_ShouldCleanUp_WithBackupTask_TombstonesCreatedAfterBackupTaskCreation()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var diagnosticLogBuilder = new StringBuilder();
            var serverCreationOptions = new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "100" } }
            };

            using var server = GetNewServer(serverCreationOptions);
            using var store = GetDocumentStore(new Options { Server = server });

            server.ServerStore.Observer.ForTestingPurposesOnly().OnDiagnosticLog += logLine => diagnosticLogBuilder.AppendLine($"[{DateTime.Now:O}] {logLine}");
            server.ServerStore.ForTestingPurposesOnly().IgnoreClusterTransactionIndexInCompareExchangeCleaner = true;
            Cluster.WaitForFirstCompareExchangeTombstonesClean(server);

            var backupConfiguration = Backup.CreateBackupConfiguration(backupPath, name: "FirstBackupConfiguration");
            _ = await Backup.UpdateConfigAsync(server, backupConfiguration, store);

            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/1", 1, 0));
            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/2", 1, 0));
            await CreateCompareExchangeTombstone(store, "cx/3");

            AssertCompareExchangeCounts(server, store.Database, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 2, "Before compare exchange tombstone cleanup", diagnosticLogBuilder);
            await CompareExchangeTombstoneCleanerTestHelper.Clean(nodes: [server], store.Database, ignoreClustrTrx: true);
            AssertCompareExchangeCounts(server, store.Database, expectedTombstonesNumber: 0, expectedCompareExchangeNumber: 2, "After compare exchange tombstone cleanup", diagnosticLogBuilder);
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.CompareExchange)]
        public async Task CompareExchangeTombstoneCleaner_ShouldNotCleanUp_WithBackupTask_TaskDisabled()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var diagnosticLogBuilder = new StringBuilder();
            var serverCreationOptions = new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "100" } }
            };

            using var server = GetNewServer(serverCreationOptions);
            using var store = GetDocumentStore(new Options { Server = server });

            server.ServerStore.Observer.ForTestingPurposesOnly().OnDiagnosticLog += logLine => diagnosticLogBuilder.AppendLine($"[{DateTime.Now:O}] {logLine}");
            server.ServerStore.ForTestingPurposesOnly().IgnoreClusterTransactionIndexInCompareExchangeCleaner = true;
            Cluster.WaitForFirstCompareExchangeTombstonesClean(server);

            var backupConfiguration = Backup.CreateBackupConfiguration(backupPath, name: "FirstBackupConfiguration", disabled: true);
            _ = await Backup.UpdateConfigAsync(server, backupConfiguration, store);

            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/1", 1, 0));
            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/2", 1, 0));
            await CreateCompareExchangeTombstone(store, "cx/3");

            AssertCompareExchangeCounts(server, store.Database, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 2, "Before compare exchange tombstone cleanup", diagnosticLogBuilder);
            await CompareExchangeTombstoneCleanerTestHelper.Clean(nodes: [server], store.Database, ignoreClustrTrx: true);
            AssertCompareExchangeCounts(server, store.Database, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 2, "After compare exchange tombstone cleanup", diagnosticLogBuilder);
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task CompareExchangeTombstoneCleaner_ShouldCleanUp_FirstBackup_IsFaulted()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var diagnosticLogBuilder = new StringBuilder();
            var serverCreationOptions = new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "100" } }
            };

            using var server = GetNewServer(serverCreationOptions);
            using var store = GetDocumentStore(new Options { Server = server });

            server.ServerStore.Observer.ForTestingPurposesOnly().OnDiagnosticLog += logLine => diagnosticLogBuilder.AppendLine($"[{DateTime.UtcNow:O}] {logLine}");
            server.ServerStore.ForTestingPurposesOnly().IgnoreClusterTransactionIndexInCompareExchangeCleaner = true;
            Cluster.WaitForFirstCompareExchangeTombstonesClean(server);

            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/1", 1, 0));
            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/2", 1, 0));
            await CreateCompareExchangeTombstone(store, "cx/3");

            AssertCompareExchangeCounts(server, store.Database, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 2, "Before compare exchange tombstone cleanup", diagnosticLogBuilder);

            var backupConfiguration = Backup.CreateBackupConfiguration(backupPath, name: "FirstBackupConfiguration");

            var nextBackupWaiter = new NextBackupWaiter(clusterTestBase: this)
                .WithDatabase(store.Database)
                .WithBackupConfiguration(backupConfiguration)
                .WithClusterNodes([server])
                .WithClusterObserverConfirmation()
                .WithDiagnosticLog(diagnosticLogBuilder)
                .SetMentorNodeTo(server, store);

            await nextBackupWaiter
                .TriggerNextFaultedOccurenceNowAsync(BackupKind.Full);

            await CompareExchangeTombstoneCleanerTestHelper.Clean(nodes: [server], store.Database, ignoreClustrTrx: true);
            AssertCompareExchangeCounts(server, store.Database, expectedTombstonesNumber: 0, expectedCompareExchangeNumber: 2, "After compare exchange tombstone cleanup after failed full backup", diagnosticLogBuilder);
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.CompareExchange)]
        public async Task CompareExchangeTombstoneCleaner_ShouldNotCleanUp_FirstBackup_IsSuccessful_ThenSecondBackup_IsFaulted()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var diagnosticLogBuilder = new StringBuilder();
            var serverCreationOptions = new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "100" } }
            };

            using var server = GetNewServer(serverCreationOptions);
            using var store = GetDocumentStore(new Options { Server = server });

            server.ServerStore.Observer.ForTestingPurposesOnly().OnDiagnosticLog += logLine => diagnosticLogBuilder.AppendLine($"[{DateTime.UtcNow:O}] {logLine}");
            server.ServerStore.ForTestingPurposesOnly().IgnoreClusterTransactionIndexInCompareExchangeCleaner = true;
            Cluster.WaitForFirstCompareExchangeTombstonesClean(server);

            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/1", 1, 0));
            await store.Operations.SendAsync(new PutCompareExchangeValueOperation<int>("cx/2", 1, 0));
            await CreateCompareExchangeTombstone(store, "cx/3");

            AssertCompareExchangeCounts(server, store.Database, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 2, "After the first compare exchange tombstone creation", diagnosticLogBuilder);

            var backupConfiguration = Backup.CreateBackupConfiguration(backupPath, name: "FirstBackupConfiguration");

            var nextBackupWaiter = new NextBackupWaiter(clusterTestBase: this)
                .WithDatabase(store.Database)
                .WithBackupConfiguration(backupConfiguration)
                .WithClusterNodes([server])
                .WithClusterObserverConfirmation()
                .WithDiagnosticLog(diagnosticLogBuilder)
                .SetMentorNodeTo(server, store);

            await nextBackupWaiter
                .TriggerNextOccurenceNowAsync(BackupKind.Full);

            await CreateCompareExchangeTombstone(store, "cx/4");
            AssertCompareExchangeCounts(server, store.Database, expectedTombstonesNumber: 2, expectedCompareExchangeNumber: 2, "Before compare exchange tombstone cleanup after successful full backup", diagnosticLogBuilder);

            await nextBackupWaiter
                .TriggerNextFaultedOccurenceNowAsync(BackupKind.Full);

            await CompareExchangeTombstoneCleanerTestHelper.Clean(nodes: [server], store.Database, ignoreClustrTrx: true);
            AssertCompareExchangeCounts(server, store.Database, expectedTombstonesNumber: 1, expectedCompareExchangeNumber: 2, "After compare exchange tombstone cleanup after failed full backup", diagnosticLogBuilder);
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.CompareExchange)]
        public async Task IncrementalBackupWithCompareExchangeTombstones()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            var config = Backup.CreateBackupConfiguration(backupPath);

            if (Directory.Exists(backupPath))
                Directory.Delete(backupPath, true);

            using (var store = GetDocumentStore())
            {
                var res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<long>("dummy", 1L, 0));
                Assert.True(res.Successful);

                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                res = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<long>("dummy", res.Index));
                Assert.True(res.Successful);

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

                res = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<long>("dummy", 2L, 0)); // compare exchange doesn't exist so we need to pass index 0
                Assert.True(res.Successful);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "marker");
                    await session.SaveChangesAsync();
                }

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

                var databaseName = GetDatabaseName() + "restore";
                var backupDirectory = Directory.GetDirectories(backupPath).First();
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
                await (await store.Maintenance.Server.SendAsync(restoreOperation))
                    .WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                using (var store2 = GetDocumentStore(new Options()
                {
                    CreateDatabase = false,
                    ModifyDatabaseName = s => databaseName
                }))
                {
                    var val = await store2.Operations.SendAsync(new GetCompareExchangeValueOperation<long>("dummy"));
                    Assert.Equal(2, val.Value);
                }
            }
        }
        
        #region Helpers

        private class NextBackupWaiter
        {
            private DocumentStore _store;
            private List<RavenServer> _clusterNodes;
            private RavenServer _runningOnServer;
            private string _databaseName;
            private PeriodicBackupConfiguration _backupConfiguration;
            private bool _shouldWaitClusterObserverConfirmation = false;
            private BackupKind _expectedBackupKind;
            private StringBuilder _diagnosticLogBuilder;
            private readonly ClusterTestBase _parent;

            public NextBackupWaiter(ClusterTestBase clusterTestBase)
            {
                _parent = clusterTestBase;
            }

            public NextBackupWaiter WithClusterNodes(List<RavenServer> nodes)
            {
                _clusterNodes = nodes;
                return this;
            }

            public NextBackupWaiter WithDatabase(string databaseName)
            {
                _databaseName = databaseName;
                return this;
            }

            public NextBackupWaiter WithBackupConfiguration(PeriodicBackupConfiguration backupConfiguration)
            {
                _backupConfiguration = backupConfiguration;
                return this;
            }

            public NextBackupWaiter WithClusterObserverConfirmation()
            {
                _shouldWaitClusterObserverConfirmation = true;
                return this;
            }

            public NextBackupWaiter WithoutClusterObserverConfirmation()
            {
                _shouldWaitClusterObserverConfirmation = false;
                return this;
            }

            public NextBackupWaiter SetMentorNodeTo(RavenServer server, DocumentStore store)
            {
                _runningOnServer = server;
                _store = store;
                return this;
            }

            public NextBackupWaiter Expect(BackupKind backupKind)
            {
                _expectedBackupKind = backupKind;
                return this;
            }

            public NextBackupWaiter WithDiagnosticLog(StringBuilder diagnosticLogBuilder)
            {
                _diagnosticLogBuilder = diagnosticLogBuilder;
                return this;
            }

            public async Task WaitNextOccurrenceAsync(Func<Task<OperationStatus>> manualTrigger = null)
            {
                _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] --- Entering {nameof(WaitNextOccurrenceAsync)} for backup task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}`.");

                Assert.True(_store != null, "DocumentStore must be set before waiting for the next backup.");
                Assert.True(_clusterNodes is { Count: > 0 }, "Cluster nodes must be set before waiting for the next backup.");
                Assert.True(_runningOnServer != null, "Running server must be set before waiting for the next backup.");
                Assert.False(string.IsNullOrEmpty(_databaseName), "Database name must be set before waiting for the next backup.");

                (long operationId, var operationStatus) = await WaitForNextBackupOccurrenceAsync(manualTrigger);
                await WaitForFinishedBackupLocallyAsync(operationId, operationStatus);

                if (_shouldWaitClusterObserverConfirmation == false)
                    return;

                var status = await GetPeriodicBackupStatusAsync();
                await WaitClusterObservationConfirmation(_clusterNodes, status, _store);
                _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Cluster observer confirmed the backup task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}`.");
            }

            public async Task TriggerNextOccurenceNowAsync(BackupKind backupKind)
            {
                Expect(backupKind);
                await WaitNextOccurrenceAsync(async () =>
                {
                    await _parent.Backup.RunBackupAsync(_runningOnServer, _backupConfiguration.TaskId, _store, isFullBackup: backupKind == BackupKind.Full, opStatus: OperationStatus.InProgress);
                    return OperationStatus.Completed;
                });
            }

            public async Task TriggerNextFaultedOccurenceNowAsync(BackupKind backupKind)
            {
                Expect(backupKind);

                var database = await _parent.GetDatabase(_store.Database, _runningOnServer);
                database.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateFailedBackup = true;

                try
                {
                    await WaitNextOccurrenceAsync(async () =>
                    {
                        await _parent.Backup.RunBackupAsync(_runningOnServer, _backupConfiguration.TaskId, _store, isFullBackup: backupKind == BackupKind.Full, opStatus: OperationStatus.InProgress);
                        return OperationStatus.Faulted;
                    });
                }
                finally
                {
                    database.PeriodicBackupRunner.ForTestingPurposesOnly().SimulateFailedBackup = false;
                }
            }

            private async Task ChangeMentorNodeIfNeededAsync()
            {
                var database = await _parent.GetDatabase(_databaseName, _runningOnServer);
                Assert.NotNull(database);

                var periodicBackupConfiguration = database.ReadDatabaseRecord().PeriodicBackups.SingleOrDefault(x => x.TaskId == _backupConfiguration.TaskId);
                if (periodicBackupConfiguration == null)
                {
                    _backupConfiguration.TaskId = await _parent.Backup.UpdateConfigAsync(_runningOnServer, _backupConfiguration, _store);
                }
                else if (periodicBackupConfiguration.MentorNode == _runningOnServer.ServerStore.NodeTag &&
                          periodicBackupConfiguration.Disabled == false)
                {
                    _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Mentor node for backup task with ID `{_backupConfiguration.TaskId}` is enabled and already set to `{_runningOnServer.ServerStore.NodeTag}`. No need to change it.");
                    return;
                }

                _backupConfiguration.MentorNode = _runningOnServer.ServerStore.NodeTag;
                _backupConfiguration.Disabled = false;
                await _parent.Backup.UpdateConfigAsync(_runningOnServer, _backupConfiguration, _store);

                _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Setting mentor node for backup task with ID `{_backupConfiguration.TaskId}` to `{_runningOnServer.ServerStore.NodeTag}`.");

                await WaitAndAssertForValueAsync(() =>
                    {
                        var record = database.ReadDatabaseRecord();
                        if (record == null)
                            return Task.FromResult<string>(null);

                        var config = record.PeriodicBackups.SingleOrDefault(x => x.TaskId == _backupConfiguration.TaskId);
                        return Task.FromResult(config?.MentorNode);
                    },
                    expectedVal: _runningOnServer.ServerStore.NodeTag,
                    interval: (int)TimeSpan.FromSeconds(1).TotalMilliseconds,
                    timeout: (int)TimeSpan.FromSeconds(30).TotalMilliseconds);

                _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Updated backup task with ID `{_backupConfiguration.TaskId}` to have mentor node `{_runningOnServer.ServerStore.NodeTag}`.");
            }

            private async Task<(long, OperationStatus)> WaitForNextBackupOccurrenceAsync(Func<Task<OperationStatus>> manualTrigger = null)
            {
                long operationId = 0;
                OperationStatus operationStatus = OperationStatus.Completed;

                var database = await _parent.GetDatabase(_databaseName, _runningOnServer);
                Assert.NotNull(database);

                await _parent.Backup.HoldBackupExecutionIfNeededAndInvoke(database.PeriodicBackupRunner.ForTestingPurposesOnly(), async () =>
                {
                    await ChangeMentorNodeIfNeededAsync();

                    _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Starting to wait for the next backup occurrence for task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}`.");

                    if (manualTrigger != null)
                    {
                        _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Manually triggering backup for task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}`.");
                        operationStatus = await manualTrigger.Invoke();
                        _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Manual trigger for backup operation returned status: {operationStatus}.");
                    }

                    var onGoingTaskInfo = await _parent.Backup.WaitForOnGoingBackupNotNullAsync(_store, _backupConfiguration.TaskId);
                    var nextOperationId = database.Operations.GetNextOperationId() - 1;
                    Assert.True(nextOperationId == onGoingTaskInfo.OnGoingBackup.RunningBackupTaskId, $"Expected a new backup task to be started, but the last ongoing task ID is still `{onGoingTaskInfo.OnGoingBackup.RunningBackupTaskId}`." +
                                                                                                      $"{Environment.NewLine}Diagnostic Info: {_diagnosticLogBuilder?.ToString() ?? "N/A"}");

                    operationId = onGoingTaskInfo.OnGoingBackup.RunningBackupTaskId;

                    _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Backup operation with ID `{operationId}` started for task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}`.");

                    var ongoingBackupKind = onGoingTaskInfo.OnGoingBackup.IsFull ? BackupKind.Full : BackupKind.Incremental;
                    Assert.True(ongoingBackupKind == _expectedBackupKind, $"Expected the ongoing backup task to be a {_expectedBackupKind}, but it is {ongoingBackupKind}.{Environment.NewLine}Diagnostic Info: {_diagnosticLogBuilder?.ToString() ?? "N/A"}");
                }, tcs: new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));

                return (operationId, operationStatus);
            }

            private async Task WaitForFinishedBackupLocallyAsync(long operationId, OperationStatus expectedOperationStatus, int timeout = 15_000, int interval = 1_000)
            {
                RavenCommand<OperationState> command = null;
                await WaitForValueAsync(async () =>
                {
                    command = await _parent.Backup.ExecuteGetOperationStateCommand(_store, operationId, _runningOnServer.ServerStore.NodeTag);
                    return command.Result?.Status == expectedOperationStatus &&
                           command.StatusCode == HttpStatusCode.OK;
                },
                    expectedVal: true,
                    timeout: timeout,
                    interval: interval);

                Assert.True(command.Result?.Status == expectedOperationStatus,
                    $"Expected the backup operation with ID `{operationId}` for task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}` " +
                    $"to be {expectedOperationStatus}, but {(command.Result == null ? "command.Result is null" : $"it is {command.Result.Status}")}." +
                    $"{Environment.NewLine}Diagnostic Info: {_diagnosticLogBuilder?.ToString() ?? "N/A"}");

                Assert.True(command.StatusCode == HttpStatusCode.OK,
                    $"Expected the backup operation with ID `{operationId}` for task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}` " +
                    $"to return status code {HttpStatusCode.OK}, but it is {command.StatusCode}.{Environment.NewLine}Diagnostic Info: {_diagnosticLogBuilder?.ToString() ?? "N/A"}");

                await _parent.Backup.CheckBackupOperationStatus(expectedOperationStatus, command, _store, _backupConfiguration.TaskId, operationId, periodicBackupRunner: null);
                Assert.True(expectedOperationStatus == command.Result.Status, $"Expected the backup operation with ID `{operationId}` for task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}` to be {expectedOperationStatus}, but it is {command.Result.Status}.{Environment.NewLine}Diagnostic Info: {_diagnosticLogBuilder?.ToString() ?? "N/A"}");
                _diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {_runningOnServer.ServerStore.NodeTag}] Backup operation with ID `{operationId}` {command.Result.Status} for task with ID `{_backupConfiguration.TaskId}` on database `{_databaseName}`.");
            }

            private async Task<PeriodicBackupStatus> GetPeriodicBackupStatusAsync()
            {
                PeriodicBackupStatus status = null;
                await WaitAndAssertForValueAsync(() =>
                    {
                        status = _runningOnServer.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(_databaseName, _backupConfiguration.TaskId);
                        return Task.FromResult(status != null);
                    }, expectedVal: true,
                    interval: (int)TimeSpan.FromSeconds(1).TotalMilliseconds,
                    timeout: (int)TimeSpan.FromSeconds(90).TotalMilliseconds);

                return status;
            }

            private async Task WaitClusterObservationConfirmation(List<RavenServer> clusterNodes, PeriodicBackupStatus status, DocumentStore store)
            {
                await _parent.Backup.WaitAndAssertForClusterObserverToGetUpdatedBackupStatusAsync(store.Database, status, clusterNodes);
            }
        }

        internal static async Task CreateCompareExchangeTombstone(DocumentStore documentStore, string key)
        {
            var res = await documentStore.Operations.SendAsync(new PutCompareExchangeValueOperation<int>(key, 1, 0));
            await documentStore.Operations.SendAsync(new DeleteCompareExchangeValueOperation<int>(key, res.Index));
        }

        private void AssertCompareExchangeCounts(RavenServer server, string databaseName, long expectedTombstonesNumber, long expectedCompareExchangeNumber, string message = null, StringBuilder diagnosticLogBuilder = null)
        {
            using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            {
                var actualTombstonesNumber = WaitForValue(() =>
                    {
                        long value;
                        using (context.OpenReadTransaction())
                            value = server.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(context, databaseName);

                        return value;
                    },
                    expectedVal: expectedTombstonesNumber,
                    timeout: (int) TimeSpan.FromSeconds(10).TotalMilliseconds,
                    interval: (int) TimeSpan.FromMilliseconds(500).TotalMilliseconds);

                var actualCompareExchangeNumber = WaitForValue(() =>
                    {
                        long value;
                        using (context.OpenReadTransaction())
                            value = server.ServerStore.Cluster.GetNumberOfCompareExchange(context, databaseName);

                        return value;
                    },
                    expectedVal: expectedCompareExchangeNumber,
                    timeout: (int) TimeSpan.FromSeconds(10).TotalMilliseconds,
                    interval: (int) TimeSpan.FromMilliseconds(500).TotalMilliseconds);

                Assert.True(expectedTombstonesNumber == actualTombstonesNumber,
                    $"Tombstones check failed. Expected: {expectedTombstonesNumber}, Actual: {actualTombstonesNumber}. " +
                    $"Step: '{message ?? "N/A"}'{Environment.NewLine}Diagnostic Info: {diagnosticLogBuilder?.ToString() ?? "N/A"}");

                Assert.True(expectedCompareExchangeNumber == actualCompareExchangeNumber,
                    $"Values check failed. Expected: {expectedCompareExchangeNumber}, Actual: {actualCompareExchangeNumber}. " +
                    $"Step: '{message ?? "N/A"}' {Environment.NewLine}Diagnostic Info: {diagnosticLogBuilder?.ToString() ?? "N/A"}");

                diagnosticLogBuilder?.AppendLine($"[{DateTime.UtcNow:O}][Node {server.ServerStore.NodeTag}] On step '{message ?? "N/A"}': Tombstones: {actualTombstonesNumber}, Values: {actualCompareExchangeNumber}");
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

        #endregion
    }
}
