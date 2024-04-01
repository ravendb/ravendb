using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.DirectoryServices.ActiveDirectory;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Server.Documents;
using Tests.Infrastructure;
using Xunit.Abstractions;
using Assert = Xunit.Assert;

namespace SlowTests.Issues
{
    public class RavenDB_19481 : RavenTestBase
    {
        public RavenDB_19481(ITestOutputHelper output) : base(output)
        {
        }


        [RavenTheory(RavenTestCategory.Smuggler)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldntReapplyClusterTransactionTwiceInRestore(Options options)
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore(options))
            {
                const string id = "users/1";

                await RevisionsHelper.SetupRevisionsAsync(store, configuration: new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 100
                    }
                });

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

                await WaitAndAssertForValueAsync(async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var u = await session.LoadAsync<User>(id);
                        return u == null ? $"\"{id}\" is null" : u.Name;
                    }
                }, "Grisha");

                var stats = await store.Maintenance.SendAsync(new GetEssentialStatisticsOperation());
                Assert.Equal(1, stats.CountOfRevisionDocuments);

                var restoredDatabaseName = $"restored_database-{Guid.NewGuid()}";
                var backupPath = NewDataPath(suffix: "BackupFolder");
                var config = Backup.CreateBackupConfiguration(backupPath);
                RestoreBackupConfiguration restoreConfig;
                if (options.DatabaseMode == RavenDatabaseMode.Sharded)
                {
                    var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store);
                    await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store, config);
                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));
                    var dirs = Directory.GetDirectories(backupPath);
                    Assert.Equal(3, dirs.Length);
                    var sharding = await Sharding.GetShardingConfigurationAsync(store);
                    var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);
                    restoreConfig = new RestoreBackupConfiguration { DatabaseName = restoredDatabaseName, ShardRestoreSettings = settings };
                }
                else
                {
                    var waitHandles = await Backup.WaitForBackupToComplete(store);
                    await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                    restoreConfig = new RestoreBackupConfiguration { BackupLocation = Directory.GetDirectories(backupPath).First(), DatabaseName = restoredDatabaseName };
                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));
                }

                var clusterTransactions = new Dictionary<string, long>();
                Server.ServerStore.ForTestingPurposesOnly().BeforeExecuteClusterTransactionBatch = (dbName, batch) =>
                {
                    if (dbName == restoredDatabaseName)
                    {
                        foreach (var clusterTx in batch)
                        {
                            var raftRequestId = clusterTx.Options.TaskId;
                            if (clusterTransactions.ContainsKey(clusterTx.Options.TaskId) == false)
                                clusterTransactions.Add(raftRequestId, 1);
                            else
                                clusterTransactions[raftRequestId]++;
                        }
                    }
                };

                using (Backup.RestoreDatabase(store, restoreConfig))
                {
                }

                foreach (var kvp in clusterTransactions)
                {
                    var timesWasApplied = kvp.Value;
                    Assert.True(timesWasApplied <= 1, $"cluster transaction \"{kvp.Key}\" was reapplied more then once ({timesWasApplied} times)");
                }
            }
        }

        [RavenTheory(RavenTestCategory.Smuggler)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RestoredDocThatCreatedByClusterWideTransactionShouldntHaveDeleteRevision(Options options)
        {
            DoNotReuseServer();

            using var store = GetDocumentStore(options);

            const string id = "users/1";

            await RevisionsHelper.SetupRevisionsAsync(store, configuration: new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                }
            });

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                await session.StoreAsync(new Company { Name = "Grisha" }, id);
                await session.SaveChangesAsync();
            }

            await WaitAndAssertForValueAsync(async () =>
            {
                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(id);
                    return u==null? $"\"{id}\" is null" : u.Name;
                }
            }, "Grisha");

            var stats = await store.Maintenance.SendAsync(new GetEssentialStatisticsOperation());
            Assert.Equal(1, stats.CountOfRevisionDocuments);

            var restoredDatabaseName = $"restored_database-{Guid.NewGuid()}";
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var config = Backup.CreateBackupConfiguration(backupPath);
            RestoreBackupConfiguration restoreConfig;
            if (options.DatabaseMode == RavenDatabaseMode.Sharded)
            {
                var waitHandles = await Sharding.Backup.WaitForBackupToComplete(store);
                await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(Server, store, config);
                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));
                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(3, dirs.Length);
                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);
                restoreConfig = new RestoreBackupConfiguration { DatabaseName = restoredDatabaseName, ShardRestoreSettings = settings };
            }
            else
            {
                var waitHandles = await Backup.WaitForBackupToComplete(store);
                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);
                restoreConfig = new RestoreBackupConfiguration { BackupLocation = Directory.GetDirectories(backupPath).First(), DatabaseName = restoredDatabaseName };
                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));
            }
            

            
            using (Backup.RestoreDatabase(store, restoreConfig))
            {
                using (var session = store.OpenAsyncSession(new SessionOptions()
                {
                    Database = restoredDatabaseName
                }))
                {
                    var user = await session.LoadAsync<Company>(id);
                    Assert.NotNull(user);
                
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    foreach (var metadata in revisionsMetadata)
                    {
                        Assert.False(metadata.GetString(Constants.Documents.Metadata.Flags).Contains(DocumentFlags.DeleteRevision.ToString()),
                            $"Restored document \"{id}\" has \'DeleteRevision\'");
                    }
                }
            }

        }

        private class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
