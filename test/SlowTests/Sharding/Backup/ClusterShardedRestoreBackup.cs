using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Backup
{
    public class ClusterShardedRestoreBackup : ClusterTestBase
    {
        public ClusterShardedRestoreBackup(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task CanBackupAndRestoreShardedDatabaseInCluster()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var cluster = await CreateRaftCluster(3);
            var dbName = GetDatabaseName();

            await ShardingCluster.CreateShardedDatabaseInCluster(dbName, replicationFactor: 1, cluster, certificate: null);

            using (var store = new DocumentStore
            {
                Urls = cluster.Nodes.Select(s => s.WebUrl).ToArray(), 
                Database = dbName,
            }.Initialize())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User(), $"users/{i}");
                    }

                    session.SaveChanges();
                }

                var waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, dbName);

                var config = Backup.CreateBackupConfiguration(backupPath);
                await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(cluster.Nodes.Count, dirs.Length);

                var settings = new ShardRestoreSetting[dirs.Length];

                for (var i = 0; i < dirs.Length; i++)
                {
                    var dir = dirs[i];
                    settings[i] = new ShardRestoreSetting
                    {
                        ShardNumber = i,
                        BackupPath = dir,
                        NodeTag = cluster.Nodes[i].ServerStore.NodeTag
                    };
                }

                // restore the database with a different name
                var newDbName = $"restored_database-{Guid.NewGuid()}";
                using (ShardedPeriodicBackupTests.ReadOnly(backupPath))
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    DatabaseName = newDbName,
                    ShardRestoreSettings = settings
                }, timeout: TimeSpan.FromSeconds(60)))
                {
                    var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(newDbName));
                    Assert.Equal(DatabaseStateStatus.Normal, dbRec.DatabaseState);
                    Assert.Equal(3, dbRec.Sharding.Shards.Length);

                    var shardNodes = new HashSet<string>();
                    foreach (var shardTopology in dbRec.Sharding.Shards)
                    {
                        Assert.Equal(1, shardTopology.Members.Count);
                        Assert.True(shardNodes.Add(shardTopology.Members[0]));
                    }

                    using (var session = store.OpenSession(newDbName))
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            var doc = session.Load<User>($"users/{i}");
                            Assert.NotNull(doc);
                        }
                    }
                }
            }
        }
    }
}
