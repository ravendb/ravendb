using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NCrontab.Advanced;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    internal class ClusterBackupTests : ClusterTestBase
    {
        public ClusterBackupTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
        public async Task DontRunFullBackupAgainIfComingBackFromAnotherNode()
        {
            var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "0" }, };
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0, watcherCluster: true, customSettings: settings);

            using (var store = GetDocumentStore(new Options { ReplicationFactor = 3, Server = leader }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Stav" }, "users/1");
                    await session.SaveChangesAsync();
                    var exists = await WaitForDocumentInClusterAsync<User>(nodes, store.Database, "users/1", x => x.Name == "Stav", TimeSpan.FromSeconds(10));
                    Assert.True(exists);
                }

                var nextBackup = GetTimeUntilBackupNextOccurence("0 0 * * *", DateTime.UtcNow);
                if (nextBackup < TimeSpan.FromSeconds(5))
                    await Task.Delay(nextBackup);

                // put backup config
                var config = new ServerWideBackupConfiguration
                {
                    Disabled = false,
                    FullBackupFrequency = "0 0 1 1 *",
                    IncrementalBackupFrequency = "0 0 * * *",
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                };
                var taskId = await Backup.UpdateServerWideConfigAsync(leader, config, store);
                var originalNode = Backup.GetBackupResponsibleNode(leader, taskId, store.Database);
                var originalNodeServer = nodes.Single(x => x.ServerStore.NodeTag == originalNode);
                await Backup.RunBackupAsync(originalNodeServer, taskId, store, isFullBackup: false);

                var otherNodeServer = nodes.First(x => x.ServerStore.NodeTag != originalNode);

                // check the backup status
                var backupStatus = await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId));
                Assert.NotNull(backupStatus.Status);
                Assert.Equal(originalNode, backupStatus.Status.NodeTag);

                // assert number of directories
                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(1, dirs.Length);
                var nodesBackupDir = dirs[0];
                var innerDirs = Directory.GetDirectories(nodesBackupDir);
                Assert.Equal(1, innerDirs.Length);
                Assert.True(innerDirs[0].Contains(originalNode));

                // change responsible node to other node
                var command = new UpdateResponsibleNodeForTasksCommand(
                    new UpdateResponsibleNodeForTasksCommand.Parameters
                    {
                        ResponsibleNodePerDatabase = new Dictionary<string, List<ResponsibleNodeInfo>>()
                        {
                            { store.Database, new List<ResponsibleNodeInfo>() { new() { TaskId = taskId, ResponsibleNode = otherNodeServer.ServerStore.NodeTag } } }
                        }
                    }, RaftIdGenerator.NewId());
                await leader.ServerStore.Engine.SendToLeaderAsync(command);

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId, differentThan: originalNode);

                // run full backup on new node
                await Backup.RunBackupAsync(otherNodeServer, taskId, store, isFullBackup: false); // is full false is to not force the backup

                // wait until status is updated with new node
                PeriodicBackupStatus status = null;
                await WaitForValueAsync(async () =>
                {
                    status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    return status?.NodeTag == otherNodeServer.ServerStore.NodeTag;
                }, true, timeout: 70_000);
                Assert.NotNull(status);
                Assert.Equal(otherNodeServer.ServerStore.NodeTag, status.NodeTag);
                Assert.True(status.IsFull);
                Assert.NotNull(status.LastFullBackup);

                // assert number of directories grew
                var otherNodeDir = Directory.GetDirectories(nodesBackupDir).Except(innerDirs).ToArray();
                Assert.Equal(1, otherNodeDir.Length);
                Assert.True(otherNodeDir[0].Contains(otherNodeServer.ServerStore.NodeTag));

                //change responsible node back to original node
                command = new UpdateResponsibleNodeForTasksCommand(
                    new UpdateResponsibleNodeForTasksCommand.Parameters
                    {
                        ResponsibleNodePerDatabase = new Dictionary<string, List<ResponsibleNodeInfo>>()
                        {
                            { store.Database, new List<ResponsibleNodeInfo>() { new() { TaskId = taskId, ResponsibleNode = originalNode } } }
                        }
                    }, RaftIdGenerator.NewId());
                await leader.ServerStore.Engine.SendToLeaderAsync(command);

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId, differentThan: otherNodeServer.ServerStore.NodeTag);

                // run the backup again - should be running incremental this time
                await Backup.RunBackupAsync(originalNodeServer, taskId, store, isFullBackup: false);

                // wait until backup finishes and saves the backup statuses - should be incremental backup and not a full one
                status = null;
                await WaitForValueAsync(async () =>
                {
                    status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    return status?.NodeTag == originalNode;
                }, true, timeout: 70_000);
                Assert.NotNull(status);
                Assert.Equal(originalNode, status.NodeTag);
                Assert.False(status.IsFull, "Backup should be incremental but is full");
                Assert.NotNull(status.LastIncrementalBackup);

                // assert no new dirs of full backup
                var dirsAfterSwitchBack = Directory.GetDirectories(nodesBackupDir);
                Assert.Equal(2, dirsAfterSwitchBack.Length);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster | RavenTestCategory.CompareExchange)]
        public async Task CompareExchangeTombstonesShouldNotBeCleanedIfNotBackedUpForLocalNode()
        {
            // A does full backup on compare exchanges, backup moves to B, cx deleted, B does full backup on their tombstones, backup moves back to A, A should still have the tombstones to back up
            // they should not have been deleted by the observer because of A's low index should prevent it
            var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Cluster.MaxClusterTransactionCompareExchangeTombstoneCheckInterval), "0" }, };
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var incrementalFrequency = "0 0 * * *";
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0, watcherCluster: true, customSettings: settings);

            using (var store = GetDocumentStore(new Options { ReplicationFactor = 3, Server = leader }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Stav" }, "users/1");
                    await session.SaveChangesAsync();
                    var exists = await WaitForDocumentInClusterAsync<User>(nodes, store.Database, "users/1", x => x.Name == "Stav", TimeSpan.FromSeconds(10));
                    Assert.True(exists);
                }

                var nextBackup = GetTimeUntilBackupNextOccurence(incrementalFrequency, DateTime.UtcNow);
                if (nextBackup < TimeSpan.FromSeconds(8))
                    await Task.Delay(nextBackup);

                // put compare exchanges
                var cmpxchngRes1 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("users/1", new User { Name = "Stav1" }, 0));
                var cmpxchngRes2 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("users/2", new User { Name = "Stav2" }, 0));
                var cmpxchngRes3 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("users/3", new User { Name = "Stav3" }, 0));
                Assert.True(cmpxchngRes1.Successful);
                Assert.True(cmpxchngRes2.Successful);
                Assert.True(cmpxchngRes3.Successful);

                var stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(3, stats.CountOfCompareExchange);

                // node backs up compare exchanges
                var config = new ServerWideBackupConfiguration
                {
                    Disabled = false,
                    FullBackupFrequency = "0 0 1 1 *",
                    IncrementalBackupFrequency = incrementalFrequency,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                };
                var taskId = await Backup.UpdateServerWideConfigAsync(leader, config, store);
                var originalNode = Backup.GetBackupResponsibleNode(leader, taskId, store.Database);
                var originalNodeServer = nodes.Single(x => x.ServerStore.NodeTag == originalNode);
                var otherNodeServer = nodes.First(x => x.ServerStore.NodeTag != originalNode);

                // first backup will be full
                await Backup.RunBackupAsync(originalNodeServer, taskId, store, isFullBackup: false);

                // check the backup status
                var backupStatus = await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId));
                Assert.NotNull(backupStatus.Status);
                Assert.Equal(originalNode, backupStatus.Status.NodeTag);
                Assert.True(backupStatus.Status.IsFull);

                // assert number of directories
                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(1, dirs.Length);
                var nodesBackupDir = dirs[0];
                var innerDirs = Directory.GetDirectories(nodesBackupDir);
                Assert.Equal(1, innerDirs.Length);
                Assert.True(innerDirs[0].Contains(originalNode));

                // change responsible node to other node
                var command = new UpdateResponsibleNodeForTasksCommand(
                    new UpdateResponsibleNodeForTasksCommand.Parameters
                    {
                        ResponsibleNodePerDatabase = new Dictionary<string, List<ResponsibleNodeInfo>>()
                        {
                            { store.Database, new List<ResponsibleNodeInfo>() { new() { TaskId = taskId, ResponsibleNode = otherNodeServer.ServerStore.NodeTag } } }
                        }
                    }, RaftIdGenerator.NewId());
                await leader.ServerStore.Engine.SendToLeaderAsync(command);

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId, differentThan: originalNode);

                // delete compare exchanges -> tombstones created
                var delRes1 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/1", cmpxchngRes1.Index));
                var delRes2 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/2", cmpxchngRes2.Index));
                var delRes3 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/3", cmpxchngRes3.Index));
                Assert.True(delRes1.Successful);
                Assert.True(delRes2.Successful);
                Assert.True(delRes3.Successful);

                // first backup on new node will be full - this includes the tombstones
                await Backup.RunBackupAsync(otherNodeServer, taskId, store, isFullBackup: false);

                PeriodicBackupStatus status = null;
                // wait until status is updated with new node
                var res = await WaitForValueAsync(async () =>
                {
                    status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    return status.NodeTag == otherNodeServer.ServerStore.NodeTag;
                }, true, timeout: 70_000);
                Assert.Equal(otherNodeServer.ServerStore.NodeTag, status.NodeTag);
                Assert.True(status.IsFull, "Backup wasn't full");
                Assert.NotNull(status.LastFullBackup);
                Assert.True(status.LastRaftIndex.LastEtag >= delRes3.Index, "tombstones raft index is not included in the backup");

                // assert number of directories grew
                var otherNodeDir = Directory.GetDirectories(nodesBackupDir).Except(innerDirs).ToArray();
                Assert.Equal(1, otherNodeDir.Length);
                Assert.True(otherNodeDir[0].Contains(otherNodeServer.ServerStore.NodeTag));

                // run cx tombstone cleaner
                await Cluster.RunCompareExchangeTombstoneCleaner(leader, simulateClusterTransactionIndex: false);

                // tombstones should not have been deleted
                stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(3, stats.CountOfCompareExchangeTombstones);
                Assert.Equal(0, stats.CountOfCompareExchange);
            }
        }
        //TODO stav: change in all tests to change mentor node instead of cluster command directly
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
                var db = await Databases.GetDocumentDatabaseInstanceFor(originalNodeServer, store.Database);

                var status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                Assert.NotNull(status);
                Assert.Equal(originalNodeServer.ServerStore.NodeTag, status.NodeTag);
                Assert.True(status.IsFull);

                // delete cx
                var delRes1 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/1", cmpxchngRes1.Index));
                Assert.True(delRes1.Successful);

                // move backup to another node and wait to finish
                var command = new UpdateResponsibleNodeForTasksCommand(
                    new UpdateResponsibleNodeForTasksCommand.Parameters
                    {
                        ResponsibleNodePerDatabase = new Dictionary<string, List<ResponsibleNodeInfo>>()
                        {
                            { store.Database, new List<ResponsibleNodeInfo>() { new() { TaskId = taskId, ResponsibleNode = otherNodeServer.ServerStore.NodeTag } } }
                        }
                    }, RaftIdGenerator.NewId());
                await leader.ServerStore.Engine.SendToLeaderAsync(command);

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
                var localStatus = BackupUtils.GetLocalBackupStatus(originalNodeServer.ServerStore, store.Database, taskId);
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
                    localStatus = BackupUtils.GetLocalBackupStatus(originalNodeServer.ServerStore, store.Database, taskId);
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

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster)]
        public async Task TombstonesShouldNotGatherIndefinitelyAndShouldBeDeletedAfterNextFull()
        {
            // This test checks tombstones don't gather indefinitely because on a node because the backup moved from it to a different node.
            // Tombstones on the non-responsible node can't be deleted until it is guaranteed the next backup will be full.
            // Once the non-responsible node is overdue on its full backup, the local status is deleted and tombstones will be deleted.

            var fullBackupFrequency = "*/2 * * * *";//TODO stav: up the freq to stabilize
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
                var dbOriginal = await Databases.GetDocumentDatabaseInstanceFor(originalNodeServer, store.Database);
                var dbOther = await Databases.GetDocumentDatabaseInstanceFor(otherNodeServer, store.Database);

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
                var command = new UpdateResponsibleNodeForTasksCommand(
                    new UpdateResponsibleNodeForTasksCommand.Parameters
                    {
                        ResponsibleNodePerDatabase = new Dictionary<string, List<ResponsibleNodeInfo>>()
                        {
                            {
                                store.Database, new List<ResponsibleNodeInfo>() { new() { TaskId = taskId, ResponsibleNode = otherNodeServer.ServerStore.NodeTag } }
                            }
                        }
                    }, RaftIdGenerator.NewId());
                await leader.ServerStore.Engine.SendToLeaderAsync(command);

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
                var localStatus = BackupUtils.GetLocalBackupStatus(originalNodeServer.ServerStore, store.Database, taskId);
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
                    localStatus = BackupUtils.GetLocalBackupStatus(originalNodeServer.ServerStore, store.Database, taskId);
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

        public static TimeSpan GetTimeUntilBackupNextOccurence(string frequency, DateTime fromUtc)
        {
            var fromUtcSpecified = DateTime.SpecifyKind(fromUtc, DateTimeKind.Utc);
            var fromLocal = fromUtcSpecified.ToLocalTime();
            var nowLocal = DateTime.Now.ToLocalTime();

            var backupParser = CrontabSchedule.Parse(frequency);
            var nextLocal = backupParser.GetNextOccurrence(fromLocal);
            var timeUntilNextOccurence = nextLocal - nowLocal + TimeSpan.FromSeconds(2);
            return timeUntilNextOccurence > TimeSpan.Zero ? timeUntilNextOccurence : TimeSpan.FromSeconds(1);
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Smuggler)]
        public async Task ChangingBackupConfigurationToAlsoIncrementalShouldNotCauseTombstoneLoss()
        {
            // have a full backup with only full frequency configuration, change config to include incremental frequency, make sure that cleaner did not rely on only having full backups to delete tombstones freely
            var backupPath1 = NewDataPath(suffix: "BackupFolder1");
            
            using (var server = GetNewServer())
            using (var store = GetDocumentStore(new Options { Server = server }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                // full backup without incremental
                var config = Backup.CreateBackupConfiguration(backupPath1);

                var documentDatabase = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                Assert.NotNull(documentDatabase);
                
                // run full backup
                var taskId = await Backup.UpdateConfigAndRunBackupAsync(server, config, store);
                
                // create tombstone
                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                // wait for tombstone cleaner
                await documentDatabase.TombstoneCleaner.ExecuteCleanup();

                var beforeChangeConfig = DateTime.UtcNow;

                // change configuration to include incremental
                config.TaskId = taskId;
                config.IncrementalBackupFrequency = "* * * * *";
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                // check change applied
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record.PeriodicBackups.Count);
                Assert.Equal("* * * * *", record.PeriodicBackups.First().IncrementalBackupFrequency);

                // wait for next backup
                PeriodicBackupStatus status;
                var res = await WaitForValueAsync(async () =>
                {
                    status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    return status.LastIncrementalBackup > beforeChangeConfig;
                }, true, timeout: 70_000, interval: 1000);
                Assert.True(res, "Incremental backup didn't happen");

                // restore from backup
                var newDb = store.Database + "-restored";
                var backupDir = Directory.GetDirectories(backupPath1).First();
                var restoreConfig = new RestoreBackupConfiguration { BackupLocation = backupDir, DatabaseName = newDb };
                var restoreOperation = new RestoreBackupOperation(restoreConfig);
                var o = await store.Maintenance.Server.SendAsync(restoreOperation);
                await o.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                // check the original document is not included in the restored db
                using (var session = store.OpenSession(newDb))
                {
                    var user = session.Load<User>("users/1");
                    Assert.Null(user);
                }
            }
        }
    }
}
