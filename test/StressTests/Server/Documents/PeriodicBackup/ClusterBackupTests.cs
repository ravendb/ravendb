using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NCrontab.Advanced;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Server.Documents.PeriodicBackup
{
    public class ClusterBackupTests_Stress : ClusterTestBase
    {
        public ClusterBackupTests_Stress(ITestOutputHelper output) : base(output)
        {
        }
        
        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster | RavenTestCategory.CompareExchange)]
        public async Task CompareExchangeTombstonesShouldNotGatherIndefinitelyAndShouldBeDeletedAfterNextFull()
        {
            // This test checks tombstones don't gather indefinitely because a non-responsible node is stuck with a backup status of low index that won't increase.
            // Once the non-responsible node is overdue on its full backup, the local status is deleted and tombstones will be deleted.

            var fullBackupFrequency = "*/2 * * * *";
            var incrementalFrequency = "* * * * *";

            var settings = new Dictionary<string, string>
            {
                { RavenConfiguration.GetKey(x => x.Cluster.MaxClusterTransactionCompareExchangeTombstoneCheckInterval), "0" },
                { RavenConfiguration.GetKey(x => x.Tombstones.CleanupInterval), 1.ToString()},
                { RavenConfiguration.GetKey(x => x.Cluster.WorkerSamplePeriod), 1000.ToString()}
            };
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0, watcherCluster: true, customSettings: settings);

            using (var store = GetDocumentStore(new Options { ReplicationFactor = 3, Server = leader }))
            {
                // create cx
                var cmpxchngRes1 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("users/1", new User { Name = "Stav1" }, 0));
                var cmpxchngRes2 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("users/2", new User { Name = "Stav2" }, 0));
                Assert.True(cmpxchngRes1.Successful);
                Assert.True(cmpxchngRes2.Successful);

                // stabilize test - if next full is very close, wait it out
                var timeUntilNextOccurence = GetTimeUntilBackupNextOccurence(fullBackupFrequency, DateTime.UtcNow);
                if(timeUntilNextOccurence < TimeSpan.FromSeconds(20))
                    await Task.Delay(timeUntilNextOccurence);

                // setup config with full and incremental freq and do first full backup
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: fullBackupFrequency, incrementalBackupFrequency: incrementalFrequency);
                var taskId = await Backup.CreateAndRunBackupInClusterAsync(config, store, new List<RavenServer>() { nodes[0] }, isFullBackup: false);

                var originalNode = Backup.GetBackupResponsibleNode(leader, taskId, store.Database);
                var originalNodeServer = nodes.Single(x => x.ServerStore.NodeTag == originalNode);
                var otherNodeServer = nodes.First(x => x.ServerStore.NodeTag != originalNode);
                var db = await Databases.GetDocumentDatabaseInstanceFor(originalNodeServer, store);

                var status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                Assert.NotNull(status);
                Assert.Equal(originalNodeServer.ServerStore.NodeTag, status.NodeTag);
                Assert.True(status.IsFull);

                // delete cx
                var delRes1 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/1", cmpxchngRes1.Index));
                Assert.True(delRes1.Successful);

                config.TaskId = taskId;
                config.MentorNode = otherNodeServer.ServerStore.NodeTag;
                await Backup.UpdateConfigAsync(leader, config, store);

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId, differentThan: originalNode);
                await Backup.RunBackupAsync(otherNodeServer, taskId, store, isFullBackup: false); // don't force full. should happen by itself

                // check cluster backup status is as expected
                status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                Assert.NotNull(status);
                Assert.Equal(otherNodeServer.ServerStore.NodeTag, status.NodeTag);
                Assert.True(status.IsFull);
                
                // run cleaner
                await db.TombstoneCleaner.ExecuteCleanup();
                await Cluster.RunCompareExchangeTombstoneCleaner(leader, simulateClusterTransactionIndex: false);

                // check tombstones not deleted
                var stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(1, stats.CountOfCompareExchangeTombstones);
                Assert.Equal(1, stats.CountOfCompareExchange);

                // delete another cx
                var delRes2 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/2", cmpxchngRes2.Index));
                Assert.True(delRes2.Successful);

                // run the incremental
                await Backup.RunBackupAsync(otherNodeServer, taskId, store, isFullBackup: false);
                
                var res = await WaitForValueAsync(async () =>
                {
                    status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    return status.LastIncrementalBackup != null;
                }, true);
                Assert.True(res, $"Incremental hasn't happened");
                Assert.False(status.IsFull);

                // run the cleaner - this should not delete anything
                await db.TombstoneCleaner.ExecuteCleanup();
                await Cluster.RunCompareExchangeTombstoneCleaner(leader, simulateClusterTransactionIndex: false);

                // check local status not null and tombstones not deleted
                var localStatus = originalNodeServer.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(store.Database, taskId);
                Assert.NotNull(localStatus);
                Assert.NotNull(localStatus.LastFullBackupInternal);

                stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(2, stats.CountOfCompareExchangeTombstones);
                Assert.Equal(0, stats.CountOfCompareExchange);

                // wait until full is overdue on not-responsible
                await Task.Delay(GetTimeUntilBackupNextOccurence(fullBackupFrequency, localStatus.LastFullBackupInternal ?? DateTime.UtcNow));

                // run cleaner
                await db.TombstoneCleaner.ExecuteCleanup();
                await Cluster.RunCompareExchangeTombstoneCleaner(leader, simulateClusterTransactionIndex: false);

                // wait for local backup status to be deleted
                await WaitForValueAsync(() =>
                {
                    localStatus = originalNodeServer.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(store.Database, taskId);
                    return localStatus == null;
                }, true);
                Assert.Null(localStatus);
                
                await WaitForValueAsync(async () =>
                {
                    await Cluster.RunCompareExchangeTombstoneCleaner(leader, simulateClusterTransactionIndex: false);
                    stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                    return stats.CountOfCompareExchangeTombstones == 0;
                }, true);
                Assert.Equal(0, stats.CountOfCompareExchangeTombstones);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster | RavenTestCategory.CompareExchange | RavenTestCategory.Sharding)]
        public async Task CompareExchangeTombstonesShouldNotGatherIndefinitelyAndShouldBeDeletedAfterNextFullSharded()
        {
            // This test checks tombstones don't gather indefinitely because a non-responsible node is stuck with a backup status of low index that won't increase.
            // Once the non-responsible node is overdue on its full backup, the local status is deleted and tombstones will be deleted.

            var fullBackupFrequency = "*/2 * * * *";
            var incrementalFrequency = "* * * * *";

            var settings = new Dictionary<string, string>
            {
                { RavenConfiguration.GetKey(x => x.Cluster.MaxClusterTransactionCompareExchangeTombstoneCheckInterval), "0" },
                { RavenConfiguration.GetKey(x => x.Tombstones.CleanupInterval), 1.ToString()},
                { RavenConfiguration.GetKey(x => x.Cluster.WorkerSamplePeriod), 1000.ToString()}
            };
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0, watcherCluster: true, customSettings: settings);
            var options = Sharding.GetOptionsForCluster(leader, shards: 1, shardReplicationFactor: 3, orchestratorReplicationFactor: 3);

            using (var store = GetDocumentStore(options))
            {
                // create cx
                var cmpxchngRes1 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("users/1", new User { Name = "Stav1" }, 0));
                var cmpxchngRes2 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("users/2", new User { Name = "Stav2" }, 0));
                Assert.True(cmpxchngRes1.Successful);
                Assert.True(cmpxchngRes2.Successful);

                // stabilize test - if next full is very close, wait it out
                var timeUntilNextOccurence = GetTimeUntilBackupNextOccurence(fullBackupFrequency, DateTime.UtcNow);
                if (timeUntilNextOccurence < TimeSpan.FromSeconds(20))
                    await Task.Delay(timeUntilNextOccurence);

                var shardName = ShardHelper.ToShardName(store.Database, 0);

                // setup config with full and incremental freq and do first full backup
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: fullBackupFrequency, incrementalBackupFrequency: incrementalFrequency);
                (long taskId, _) = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(nodes, store, config, isFullBackup: false);

                var originalNode = Backup.GetBackupResponsibleNode(leader, taskId, shardName);
                var originalNodeServer = nodes.Single(x => x.ServerStore.NodeTag == originalNode);
                var otherNodeServer = nodes.First(x => x.ServerStore.NodeTag != originalNode);
                var db = await Databases.GetDocumentDatabaseInstanceFor(originalNodeServer, store, shardName);

                await WaitForAssertionAsync(async () =>
                {
                    var status = (await store.Maintenance.ForShard(0).SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    Assert.NotNull(status);
                    Assert.Equal(originalNodeServer.ServerStore.NodeTag, status.NodeTag);
                    Assert.True(status.IsFull);
                });

                // delete cx
                var delRes1 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/1", cmpxchngRes1.Index));
                Assert.True(delRes1.Successful);

                // change responsible node
                var command = new UpdateResponsibleNodeForTasksCommand(
                    new UpdateResponsibleNodeForTasksCommand.Parameters()
                    {
                        ResponsibleNodePerDatabase = new Dictionary<string, List<ResponsibleNodeInfo>>()
                        {
                            {
                                shardName,
                                new List<ResponsibleNodeInfo>()
                                {
                                    new ResponsibleNodeInfo() { TaskId = taskId, ResponsibleNode = otherNodeServer.ServerStore.NodeTag }
                                }
                            }
                        }
                    }, RaftIdGenerator.NewId());
                var result = await leader.ServerStore.SendToLeaderAsync(command);
                await leader.ServerStore.Cluster.WaitForIndexNotification(result.Index);

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId, databaseName: shardName, differentThan: originalNode);
                await Sharding.Backup.RunBackupAsync(store, taskId, isFullBackup: false); // don't force full. should happen by itself

                // check cluster backup status is as expected
                await WaitForAssertionAsync(async () =>
                {
                    var status = (await store.Maintenance.ForShard(0).SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    Assert.NotNull(status);
                    Assert.Equal(otherNodeServer.ServerStore.NodeTag, status.NodeTag);
                    Assert.True(status.IsFull);
                });

                // run cleaner
                await db.TombstoneCleaner.ExecuteCleanup();
                await Cluster.RunCompareExchangeTombstoneCleaner(leader, simulateClusterTransactionIndex: false);

                // check tombstones not deleted
                var stats = store.Maintenance.ForDatabase(store.Database).ForShard(0).ForNode("A").Send(new GetDetailedStatisticsOperation());
                Assert.Equal(1, stats.CountOfCompareExchangeTombstones);
                Assert.Equal(1, stats.CountOfCompareExchange);

                // delete another cx
                var delRes2 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/2", cmpxchngRes2.Index));
                Assert.True(delRes2.Successful);

                // run the incremental
                await Sharding.Backup.RunBackupAsync(store, taskId, isFullBackup: false);

                await WaitForAssertionAsync(async () =>
                {
                    var status = (await store.Maintenance.ForShard(0).SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    Assert.NotNull(status.LastIncrementalBackup);
                    Assert.False(status.IsFull);
                });
                
                // run the cleaner - this should not delete anything
                await db.TombstoneCleaner.ExecuteCleanup();
                await Cluster.RunCompareExchangeTombstoneCleaner(leader, simulateClusterTransactionIndex: false);

                // check local status not null and tombstones not deleted
                var localStatus = originalNodeServer.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(shardName, taskId);
                Assert.NotNull(localStatus);
                Assert.NotNull(localStatus.LastFullBackupInternal);

                stats = store.Maintenance.ForDatabase(store.Database).ForShard(0).ForNode("A").Send(new GetDetailedStatisticsOperation());
                Assert.Equal(2, stats.CountOfCompareExchangeTombstones);
                Assert.Equal(0, stats.CountOfCompareExchange);

                // wait until full is overdue on not-responsible
                await Task.Delay(GetTimeUntilBackupNextOccurence(fullBackupFrequency, localStatus.LastFullBackupInternal ?? DateTime.UtcNow));

                // run cleaner
                await db.TombstoneCleaner.ExecuteCleanup();
                await Cluster.RunCompareExchangeTombstoneCleaner(leader, simulateClusterTransactionIndex: false);

                // wait for local backup status to be deleted
                await WaitForValueAsync(() =>
                {
                    localStatus = originalNodeServer.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(shardName, taskId);
                    return localStatus == null;
                }, true);
                Assert.Null(localStatus);

                await WaitForValueAsync(async () =>
                {
                    await Cluster.RunCompareExchangeTombstoneCleaner(leader, simulateClusterTransactionIndex: false);
                    stats = store.Maintenance.ForDatabase(store.Database).ForShard(0).ForNode("A").Send(new GetDetailedStatisticsOperation());
                    return stats.CountOfCompareExchangeTombstones == 0;
                }, true);
                Assert.Equal(0, stats.CountOfCompareExchangeTombstones);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
        public async Task TombstonesShouldNotGatherIndefinitelyAndShouldBeDeletedAfterNextFull()
        {
            // This test checks tombstones don't gather indefinitely because on a node because the backup moved from it to a different node.
            // Tombstones on the non-responsible node can't be deleted until it is guaranteed the next backup will be full.
            // Once the non-responsible node is overdue on its full backup, the local status is deleted and tombstones will be deleted.

            var fullBackupFrequency = "*/2 * * * *";
            var incrementalFrequency = "* * * * *";

            var settings = new Dictionary<string, string>
            {
                { RavenConfiguration.GetKey(x => x.Tombstones.CleanupInterval), 1.ToString()}
            };
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0, watcherCluster: true, customSettings: settings);

            using (var store = GetDocumentStore(new Options { ReplicationFactor = 3, Server = leader }))
            {
                // create docs
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.Store(new User(), "users/2");
                    session.SaveChanges();
                }

                // stabilize test - if next full is very close, wait it out
                var timeUntilNextOccurence = GetTimeUntilBackupNextOccurence(fullBackupFrequency, DateTime.UtcNow);
                await Task.Delay(timeUntilNextOccurence);

                // setup config with full and incremental freq and do first full backup
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: fullBackupFrequency, incrementalBackupFrequency: incrementalFrequency);
                var taskId = await Backup.CreateAndRunBackupInClusterAsync(config, store, new List<RavenServer>() { nodes[0] }, isFullBackup: false);

                var originalNode = Backup.GetBackupResponsibleNode(leader, taskId, store.Database);
                var originalNodeServer = nodes.Single(x => x.ServerStore.NodeTag == originalNode);
                var otherNodeServer = nodes.First(x => x.ServerStore.NodeTag != originalNode);
                var dbOriginal = await Databases.GetDocumentDatabaseInstanceFor(originalNodeServer, store);
                var dbOther = await Databases.GetDocumentDatabaseInstanceFor(otherNodeServer, store);

                var status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                Assert.NotNull(status);
                Assert.Equal(originalNodeServer.ServerStore.NodeTag, status.NodeTag);
                Assert.True(status.IsFull);

                // delete doc - create tombstone
                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                // move backup to another node and wait to finish
                config.TaskId = taskId;
                config.MentorNode = otherNodeServer.ServerStore.NodeTag;
                await Backup.UpdateConfigAsync(leader, config, store);

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId, differentThan: originalNode);
                await Backup.RunBackupAsync(otherNodeServer, taskId, store, isFullBackup: false); // don't force full. should happen by itself

                // check cluster backup status is as expected
                status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                Assert.NotNull(status);
                Assert.Equal(otherNodeServer.ServerStore.NodeTag, status.NodeTag);
                Assert.True(status.IsFull);

                // run cleaner
                await dbOriginal.TombstoneCleaner.ExecuteCleanup();
                await dbOther.TombstoneCleaner.ExecuteCleanup();

                // check tombstone not deleted - we are not yet overdue
                using (dbOriginal.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstonesCount = dbOriginal.DocumentsStorage.GetNumberOfTombstones(context);
                    var docCount = dbOriginal.DocumentsStorage.GetNumberOfDocuments(context);
                    Assert.Equal(1, docCount);
                    Assert.Equal(1, tombstonesCount);
                }

                // delete another doc
                using (var session = store.OpenSession())
                {
                    session.Delete("users/2");
                    session.SaveChanges();
                }

                // run the incremental
                await Task.Delay(GetTimeUntilBackupNextOccurence(incrementalFrequency, status.LastFullBackupInternal ?? DateTime.UtcNow));
                var res = await WaitForValueAsync(async () =>
                {
                    status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    return status.LastIncrementalBackup != null;
                }, true, timeout: 70_000);
                Assert.True(res, $"Incremental hasn't happened");
                Assert.False(status.IsFull);

                // run cleaner
                await dbOriginal.TombstoneCleaner.ExecuteCleanup();
                await dbOther.TombstoneCleaner.ExecuteCleanup();

                // check local status not null and tombstones not deleted
                var localStatus = originalNodeServer.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(store.Database, taskId);
                Assert.NotNull(localStatus);
                Assert.NotNull(localStatus.LastFullBackupInternal);

                // on original server not deleted
                using (dbOriginal.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var count = dbOriginal.DocumentsStorage.GetNumberOfTombstones(context);
                    Assert.Equal(2, count);
                }

                // on responsible node already deleted by incremental
                using (dbOther.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var count = dbOther.DocumentsStorage.GetNumberOfTombstones(context);
                    Assert.Equal(0, count);
                }

                // wait until full is overdue on not-responsible
                await Task.Delay(GetTimeUntilBackupNextOccurence(fullBackupFrequency, localStatus.LastFullBackupInternal ?? DateTime.UtcNow));

                // run cleaner
                await dbOriginal.TombstoneCleaner.ExecuteCleanup();
                await dbOther.TombstoneCleaner.ExecuteCleanup();

                // wait for local backup status to be deleted
                await WaitForValueAsync(() =>
                {
                    localStatus = originalNodeServer.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(store.Database, taskId);
                    return localStatus == null;
                }, true);
                Assert.Null(localStatus);

                // wait for tombstones to be deleted
                await WaitForValueAsync(() =>
                {
                    using (dbOriginal.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var count = dbOriginal.DocumentsStorage.GetNumberOfTombstones(context);
                        return count;
                    }
                }, 0);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task TombstonesShouldNotGatherIndefinitelyAndShouldBeDeletedAfterNextFullSharded()
        {
            // This test checks tombstones don't gather indefinitely because on a node because the backup moved from it to a different node.
            // Tombstones on the non-responsible node can't be deleted until it is guaranteed the next backup will be full.
            // Once the non-responsible node is overdue on its full backup, the local status is deleted and tombstones will be deleted.

            var fullBackupFrequency = "*/2 * * * *";
            var incrementalFrequency = "* * * * *";

            var settings = new Dictionary<string, string>
            {
                { RavenConfiguration.GetKey(x => x.Tombstones.CleanupInterval), 1.ToString()}
            };
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0, watcherCluster: true, customSettings: settings);
            var options = Sharding.GetOptionsForCluster(leader, shards: 1, shardReplicationFactor: 3, orchestratorReplicationFactor: 3);

            using (var store = GetDocumentStore(options))
            {
                // create docs
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.Store(new User(), "users/2");
                    session.SaveChanges();
                }

                // stabilize test - if next full is very close, wait it out
                var timeUntilNextOccurence = GetTimeUntilBackupNextOccurence(fullBackupFrequency, DateTime.UtcNow);
                await Task.Delay(timeUntilNextOccurence);

                // setup config with full and incremental freq and do first full backup
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: fullBackupFrequency, incrementalBackupFrequency: incrementalFrequency);
                (long taskId, _) = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(nodes[0], store, config, isFullBackup: false);

                var shardName = ShardHelper.ToShardName(store.Database, 0);
                var originalNode = Backup.GetBackupResponsibleNode(leader, taskId, shardName);
                var originalNodeServer = nodes.Single(x => x.ServerStore.NodeTag == originalNode);
                var otherNodeServer = nodes.First(x => x.ServerStore.NodeTag != originalNode);
                var dbOriginal = await Databases.GetDocumentDatabaseInstanceFor(originalNodeServer, store, shardName);
                var dbOther = await Databases.GetDocumentDatabaseInstanceFor(otherNodeServer, store, shardName);

                await WaitForAssertionAsync(async () =>
                {
                    var status = (await store.Maintenance.ForShard(0).SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    Assert.NotNull(status);
                    Assert.Equal(originalNodeServer.ServerStore.NodeTag, status.NodeTag);
                    Assert.True(status.IsFull);
                });

                // delete doc - create tombstone
                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                // move backup to another node and wait to finish
                var command = new UpdateResponsibleNodeForTasksCommand(
                    new UpdateResponsibleNodeForTasksCommand.Parameters()
                    {
                        ResponsibleNodePerDatabase = new Dictionary<string, List<ResponsibleNodeInfo>>()
                        {
                            {
                                shardName,
                                new List<ResponsibleNodeInfo>()
                                {
                                    new ResponsibleNodeInfo() { TaskId = taskId, ResponsibleNode = otherNodeServer.ServerStore.NodeTag }
                                }
                            }
                        }
                    }, RaftIdGenerator.NewId());
                var result = await leader.ServerStore.SendToLeaderAsync(command);
                await leader.ServerStore.Cluster.WaitForIndexNotification(result.Index);

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId, databaseName: shardName, differentThan: originalNode);
                await Sharding.Backup.RunBackupAsync(store, taskId, isFullBackup: false); // don't force full. should happen by itself

                // check cluster backup status is as expected
                PeriodicBackupStatus status = null;
                await WaitForAssertionAsync(async () =>
                {
                    status = (await store.Maintenance.ForShard(0).SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    Assert.NotNull(status);
                    Assert.Equal(otherNodeServer.ServerStore.NodeTag, status.NodeTag);
                    Assert.True(status.IsFull);
                });

                // run cleaner
                await dbOriginal.TombstoneCleaner.ExecuteCleanup();
                await dbOther.TombstoneCleaner.ExecuteCleanup();

                // check tombstone not deleted - we are not yet overdue
                using (dbOriginal.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstonesCount = dbOriginal.DocumentsStorage.GetNumberOfTombstones(context);
                    var docCount = dbOriginal.DocumentsStorage.GetNumberOfDocuments(context);
                    Assert.Equal(1, docCount);
                    Assert.Equal(1, tombstonesCount);
                }

                // delete another doc
                using (var session = store.OpenSession())
                {
                    session.Delete("users/2");
                    session.SaveChanges();
                }

                // run the incremental
                await Task.Delay(GetTimeUntilBackupNextOccurence(incrementalFrequency, status.LastFullBackupInternal ?? DateTime.UtcNow));
                var res = await WaitForValueAsync(async () =>
                {
                    status = (await store.Maintenance.ForShard(0).SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    return status.LastIncrementalBackup != null;
                }, true, timeout: 70_000);
                Assert.True(res, $"Incremental hasn't happened");
                Assert.False(status.IsFull);

                // run cleaner
                await dbOriginal.TombstoneCleaner.ExecuteCleanup();
                await dbOther.TombstoneCleaner.ExecuteCleanup();

                // check local status not null and tombstones not deleted
                var localStatus = originalNodeServer.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(shardName, taskId);
                Assert.NotNull(localStatus);
                Assert.NotNull(localStatus.LastFullBackupInternal);

                // on original server not deleted
                using (dbOriginal.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var count = dbOriginal.DocumentsStorage.GetNumberOfTombstones(context);
                    Assert.Equal(2, count);
                }

                // on responsible node already deleted by incremental
                using (dbOther.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var count = dbOther.DocumentsStorage.GetNumberOfTombstones(context);
                    Assert.Equal(0, count);
                }

                // wait until full is overdue on not-responsible
                await Task.Delay(GetTimeUntilBackupNextOccurence(fullBackupFrequency, localStatus.LastFullBackupInternal ?? DateTime.UtcNow));

                // run cleaner
                await dbOriginal.TombstoneCleaner.ExecuteCleanup();
                await dbOther.TombstoneCleaner.ExecuteCleanup();

                // wait for local backup status to be deleted
                await WaitForValueAsync(() =>
                {
                    localStatus = originalNodeServer.ServerStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(shardName, taskId);
                    return localStatus == null;
                }, true);
                Assert.Null(localStatus);

                // wait for tombstones to be deleted
                await WaitForValueAsync(() =>
                {
                    using (dbOriginal.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var count = dbOriginal.DocumentsStorage.GetNumberOfTombstones(context);
                        return count;
                    }
                }, 0);
            }
        }

        private static TimeSpan GetTimeUntilBackupNextOccurence(string frequency, DateTime fromUtc)
        {
            var fromUtcSpecified = DateTime.SpecifyKind(fromUtc, DateTimeKind.Utc);
            var fromLocal = fromUtcSpecified.ToLocalTime();
            var nowLocal = DateTime.Now.ToLocalTime();

            var backupParser = CrontabSchedule.Parse(frequency);
            var nextLocal = backupParser.GetNextOccurrence(fromLocal);
            var timeUntilNextOccurence = nextLocal - nowLocal + TimeSpan.FromSeconds(2);
            return timeUntilNextOccurence > TimeSpan.Zero ? timeUntilNextOccurence : TimeSpan.FromSeconds(1);
        }
    }
}
