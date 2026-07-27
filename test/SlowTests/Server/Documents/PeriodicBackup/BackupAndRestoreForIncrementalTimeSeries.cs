using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class BackupAndRestoreForIncrementalTimeSeries : ClusterTestBase
    {
        public BackupAndRestoreForIncrementalTimeSeries(ITestOutputHelper output) : base(output)
        {
        }

        
        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task IncrementalTimeSeriesCanRestoreDeadValue()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var config = Backup.CreateBackupConfiguration(backupPath);
            config.BackupType = BackupType.Snapshot;

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
                    session.IncrementalTimeSeriesFor("users/1", "INC:Heartrate").Increment(baseline, 1);
                    session.IncrementalTimeSeriesFor("users/1", "INC:Heartrate").Increment(baseline.AddDays(20), 1);


                    await session.SaveChangesAsync();
                }

                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                using (var session = store.OpenAsyncSession())
                {
                    session.IncrementalTimeSeriesFor("users/1", "INC:Heartrate").Delete(baseline.AddDays(20));

                    await session.SaveChangesAsync();
                }

                await Backup.RunBackupAsync(Server, backupTaskId, store, isFullBackup: false);

                using (var restored = RestoreAndGetStore(store, backupPath, out var releaseDatabase))
                using (releaseDatabase)
                {
                    var stats = await restored.Maintenance.SendAsync(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(1, stats.CountOfTimeSeriesSegments);
                }
            }
        }

        [RavenTheory(RavenTestCategory.TimeSeries | RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task IncrementalTimeSeriesCanRestoreDeadValueInCluster(Options options)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var config = Backup.CreateBackupConfiguration(backupPath);
            config.BackupType = BackupType.Snapshot;
            
            var baseline = RavenTestHelper.UtcToday;

            var cluster = await CreateRaftCluster(numberOfNodes: 3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options(options)
            {
                Server = cluster.Leader,
                ReplicationFactor = 3
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.IncrementalTimeSeriesFor("users/1", "INC:Heartrate").Increment(baseline, 1);
                    session.IncrementalTimeSeriesFor("users/1", "INC:Heartrate").Increment(baseline.AddDays(20), 1);
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    await session.SaveChangesAsync();
                }

                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(cluster.Leader, config, store);
                
                var stores = GetDocumentStores(cluster.Nodes, store.Database, disableTopologyUpdates: true);
                using (var sessionA = stores[0].OpenAsyncSession())
                {
                    sessionA.IncrementalTimeSeriesFor("users/1", "INC:Heartrate").Increment(baseline.AddDays(20), 1);
                    sessionA.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    await sessionA.SaveChangesAsync();
                }

                using (var sessionB = stores[1].OpenAsyncSession())
                {
                    sessionB.IncrementalTimeSeriesFor("users/1", "INC:Heartrate").Delete(baseline.AddDays(20));
                    sessionB.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    await sessionB.SaveChangesAsync();
                }

                using (var sessionC = stores[2].OpenAsyncSession())
                {
                    sessionC.IncrementalTimeSeriesFor("users/1", "INC:Heartrate").Increment(baseline.AddDays(20), 1);
                    sessionC.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    await sessionC.SaveChangesAsync();
                }

                await Backup.RunBackupAsync(cluster.Leader, backupTaskId, store, isFullBackup: false);

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

                        Assert.Equal(2, values.Count);
                        Assert.Equal(baseline, values[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(1, values[0].Value);

                        Assert.Equal(baseline.AddDays(20), values[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(1, values[1].Value);
                    }
                }
            }
        }

        public IDocumentStore RestoreAndGetStore(IDocumentStore store, string backupPath, out IDisposable releaseDatabase, TimeSpan? timeout = null)
        {
            var restoredDatabaseName = GetDatabaseName();

            releaseDatabase = Backup.RestoreDatabase(store, new RestoreBackupConfiguration
            {
                BackupLocation = Directory.GetDirectories(backupPath).First(),
                DatabaseName = restoredDatabaseName
            }, timeout);

            var options = new Options
            {
                ModifyDatabaseName = s => restoredDatabaseName,
                CreateDatabase = false,
                DeleteDatabaseOnDispose = true,
                Server = GetServers().First()
            };

            options.ModifyDocumentStore = null;

            return GetDocumentStore(options);
        }
    }
}
