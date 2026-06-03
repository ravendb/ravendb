using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NCrontab.Advanced;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Backups.Sharding;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class ClusterBackupTests : ClusterTestBase
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
                if (nextBackup < TimeSpan.FromSeconds(10))
                    await Task.Delay(nextBackup);

                // put backup config
                var config = new PeriodicBackupConfiguration()
                {
                    Disabled = false,
                    FullBackupFrequency = "0 0 1 1 *",
                    IncrementalBackupFrequency = "0 0 * * *",
                    LocalSettings = new LocalSettings { FolderPath = backupPath }
                };
                var taskId = await Backup.UpdateConfigAsync(leader, config, store);
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

                var innerDirs = Directory.GetFiles(nodesBackupDir);
                Assert.Equal(1, innerDirs.Length);
                Assert.True(innerDirs[0].Contains(originalNode));
                
                // change responsible node to other node
                config.TaskId = taskId;
                config.MentorNode = otherNodeServer.ServerStore.NodeTag;
                config.PinToMentorNode = true;
                await Backup.UpdateConfigAsync(leader, config, store);
                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId, differentThan: originalNode);

                // run full backup on new node
                await Backup.RunBackupAsync(otherNodeServer, taskId, store, isFullBackup: false);

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
                var otherNodeDir = Directory.GetDirectories(backupPath).Except(dirs).ToArray();
                Assert.Equal(1, otherNodeDir.Length);
                Assert.True(otherNodeDir[0].Contains(otherNodeServer.ServerStore.NodeTag));

                // change responsible node back to original
                config.TaskId = taskId;
                config.MentorNode = originalNode;
                config.PinToMentorNode = true;
                await Backup.UpdateConfigAsync(leader, config, store);
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

                // assert no new dirs of full backup - original node directory should have a file for full and a file for incremental
                var dirsAfterSwitchBack = Directory.GetFiles(nodesBackupDir);
                Assert.Equal(2, dirsAfterSwitchBack.Length);

                // other node directory should only have a file for full
                dirsAfterSwitchBack = Directory.GetFiles(otherNodeDir[0]);
                Assert.Equal(1, dirsAfterSwitchBack.Length);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task DontRunFullBackupAgainIfComingBackFromAnotherNodeSharded()
        {
            var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Cluster.CompareExchangeTombstonesCleanupInterval), "0" }, };
            var backupPath = NewDataPath(suffix: "BackupFolder");

            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0, watcherCluster: true, customSettings: settings);
            var options = Sharding.GetOptionsForCluster(leader, shards: 1, shardReplicationFactor: 3, orchestratorReplicationFactor: 3);

            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Stav" }, "users/1");
                    await session.SaveChangesAsync();
                    var exists = await WaitForDocumentInClusterAsync<User>(nodes, store.Database, "users/1", x => x.Name == "Stav", TimeSpan.FromSeconds(10));
                    Assert.True(exists);
                }

                var nextBackup = GetTimeUntilBackupNextOccurence("0 0 * * *", DateTime.UtcNow);
                if (nextBackup < TimeSpan.FromSeconds(10))
                    await Task.Delay(nextBackup);

                // put backup config
                var config = new PeriodicBackupConfiguration()
                {
                    Disabled = false,
                    FullBackupFrequency = "0 0 1 1 *",
                    IncrementalBackupFrequency = "0 0 * * *",
                    LocalSettings = new LocalSettings { FolderPath = backupPath }
                };

                var shardName = ShardHelper.ToShardName(store.Database, 0);
                var taskId = await Sharding.Backup.UpdateConfigAsync(leader, config, store);
                var originalNode = Backup.GetBackupResponsibleNode(leader, taskId, shardName);
                var originalNodeServer = nodes.Single(x => x.ServerStore.NodeTag == originalNode);

                await Sharding.Backup.RunBackupAsync(store, taskId, isFullBackup: false);

                var otherNodeServer = nodes.First(x => x.ServerStore.NodeTag != originalNode);

                // check the backup status
                await WaitForAssertionAsync(async () =>
                {
                    var backupStatus = await store.Maintenance.SendAsync(new GetShardedPeriodicBackupStatusOperation(taskId));
                    Assert.NotNull(backupStatus?.Statuses.SingleOrDefault().Value);
                    Assert.Equal(originalNode, backupStatus.Statuses.Single().Value.NodeTag);
                });

                // assert number of directories
                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(1, dirs.Length);
                var nodesBackupDir = dirs[0];

                var innerDirs = Directory.GetFiles(nodesBackupDir);
                Assert.Equal(1, innerDirs.Length);
                Assert.True(innerDirs[0].Contains(originalNode));

                // change responsible node to other node
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

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId, shardName, differentThan: originalNode);

                // run full backup on new node
                await Sharding.Backup.RunBackupAsync(store, taskId, isFullBackup: false);

                // wait until status is updated with new node
                PeriodicBackupStatus status = null;
                await WaitForValueAsync(async () =>
                {
                    status = (await store.Maintenance.SendAsync(new GetShardedPeriodicBackupStatusOperation(taskId))).Statuses?.SingleOrDefault().Value;
                    return status?.NodeTag == otherNodeServer.ServerStore.NodeTag;
                }, true, timeout: 70_000);
                Assert.NotNull(status);
                Assert.Equal(otherNodeServer.ServerStore.NodeTag, status.NodeTag);
                Assert.True(status.IsFull);
                Assert.NotNull(status.LastFullBackup);

                // assert number of directories grew
                var otherNodeDir = Directory.GetDirectories(backupPath).Except(dirs).ToArray();
                Assert.Equal(1, otherNodeDir.Length);
                Assert.True(otherNodeDir[0].Contains(otherNodeServer.ServerStore.NodeTag));

                // change responsible node back to original
                command = new UpdateResponsibleNodeForTasksCommand(
                    new UpdateResponsibleNodeForTasksCommand.Parameters()
                    {
                        ResponsibleNodePerDatabase = new Dictionary<string, List<ResponsibleNodeInfo>>()
                        {
                            {
                                shardName,
                                new List<ResponsibleNodeInfo>()
                                {
                                    new ResponsibleNodeInfo() { TaskId = taskId, ResponsibleNode = originalNode }
                                }
                            }
                        }
                    }, RaftIdGenerator.NewId());
                result = await leader.ServerStore.SendToLeaderAsync(command);
                await leader.ServerStore.Cluster.WaitForIndexNotification(result.Index);

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId, shardName, differentThan: otherNodeServer.ServerStore.NodeTag);

                // add doc so incremental has something to save
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Stav" }, "users/2");
                    await session.SaveChangesAsync();
                    var exists = await WaitForDocumentInClusterAsync<User>(nodes, store.Database, "users/2", x => x.Name == "Stav", TimeSpan.FromSeconds(10));
                    Assert.True(exists);
                }

                // run the backup again - should be running incremental this time
                await Sharding.Backup.RunBackupAsync(store, taskId, isFullBackup: false);
                
                // wait until backup finishes and saves the backup statuses - should be incremental backup and not a full one
                status = null;
                await WaitForValueAsync(async () =>
                {
                    status = (await store.Maintenance.SendAsync(new GetShardedPeriodicBackupStatusOperation(taskId))).Statuses?.SingleOrDefault().Value;
                    return status?.NodeTag == originalNode;
                }, true, timeout: 70_000);
                Assert.NotNull(status);
                Assert.Equal(originalNode, status.NodeTag);
                Assert.False(status.IsFull, "Backup should be incremental but is full");
                Assert.NotNull(status.LastIncrementalBackup);

                // assert no new dirs of full backup - original node directory should have a file for full and a file for incremental
                var dirsAfterSwitchBack = Directory.GetFiles(nodesBackupDir);
                Assert.Equal(2, dirsAfterSwitchBack.Length);

                // other node directory should only have a file for full
                dirsAfterSwitchBack = Directory.GetFiles(otherNodeDir[0]);
                Assert.Equal(1, dirsAfterSwitchBack.Length);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster | RavenTestCategory.CompareExchange)]
        public async Task CompareExchangeTombstonesShouldNotBeCleanedIfNotBackedUpForLocalNode()
        {
            var logBuilder = new StringBuilder();

            // Node A does full backup on compare exchanges, backup moves to Node B, cx deleted, Node B does full backup on their tombstones, backup moves back to Node A, Node A should still have the tombstones to back up
            // they should not have been deleted by the observer because of Node A's low index should prevent it
            var backupPath = NewDataPath(suffix: "BackupFolder");
            if (Directory.Exists(backupPath))
            {
                Log($"Backup path '{backupPath}' already exists, deleting it.");
                Directory.Delete(backupPath, recursive: true);
            }

            var incrementalFrequency = "0 0 * * *";
            var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Cluster.MaxClusterTransactionCompareExchangeTombstoneCheckInterval), "0" }, };
            var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 3, leaderIndex: 0, watcherCluster: true, customSettings: settings);
            Log($"Leader node tag: {leader.ServerStore.NodeTag}");

            using (var store = GetDocumentStore(new Options { ReplicationFactor = 3, Server = leader }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Stav" }, "users/1");
                    await session.SaveChangesAsync();
                    var exists = await WaitForDocumentInClusterAsync<User>(nodes, store.Database, "users/1", x => x.Name == "Stav", TimeSpan.FromSeconds(10));
                    Assert.True(exists, UserMessageWithLog("Document 'users/1' was not replicated to all nodes in the cluster"));
                }

                var nextBackup = GetTimeUntilBackupNextOccurence(incrementalFrequency, DateTime.UtcNow);
                Log($"Next backup in: {nextBackup}");
                if (nextBackup < TimeSpan.FromSeconds(8))
                    await Task.Delay(nextBackup);

                // put compare exchanges
                var cmpxchngRes1 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("users/1", new User { Name = "Stav1" }, 0));
                var cmpxchngRes2 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("users/2", new User { Name = "Stav2" }, 0));
                var cmpxchngRes3 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("users/3", new User { Name = "Stav3" }, 0));
                Log($"Compare exchanges created: cmpxchngRes1.Index = {cmpxchngRes1.Index}, cmpxchngRes2.Index = {cmpxchngRes2.Index}, cmpxchngRes3.Index = {cmpxchngRes3.Index}");
                Assert.True(cmpxchngRes1.Successful, UserMessageWithLog("PutCompareExchangeValueOperation for 'users/1' failed."));
                Assert.True(cmpxchngRes2.Successful, UserMessageWithLog("PutCompareExchangeValueOperation for 'users/2' failed."));
                Assert.True(cmpxchngRes3.Successful, UserMessageWithLog("PutCompareExchangeValueOperation for 'users/3' failed."));

                var stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Log($"CountOfCompareExchange: {stats.CountOfCompareExchange}, CountOfCompareExchangeTombstones: {stats.CountOfCompareExchangeTombstones}");
                Assert.True(stats.CountOfCompareExchange == 3, UserMessageWithLog($"CountOfCompareExchange should be 3 but was {stats.CountOfCompareExchange}."));

                // node backs up compare exchanges
                var config = new PeriodicBackupConfiguration()
                {
                    Disabled = false,
                    FullBackupFrequency = "0 0 1 1 *",
                    IncrementalBackupFrequency = incrementalFrequency,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                };
                var taskId = await Backup.UpdateConfigAsync(leader, config, store);
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(taskId);

                Log($"Backup task created with id: {taskId}");
                var originalNode = Backup.GetBackupResponsibleNode(leader, taskId, store.Database);
                Log($"Original node responsible for backup: {originalNode}");
                var originalNodeServer = nodes.Single(x => x.ServerStore.NodeTag == originalNode);
                Log($"Original node server: {originalNodeServer.ServerStore.NodeTag}");
                var otherNodeServer = nodes.First(x => x.ServerStore.NodeTag != originalNode);
                Log($"Other node server: {otherNodeServer.ServerStore.NodeTag}");

                // first backup will be full
                await Backup.RunBackupAsync(originalNodeServer, taskId, store, isFullBackup: false);

                // check the backup status
                var backupStatus = await WaitForNotNullAsync(async () =>
                {
                    var result = await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId));
                    return result?.Status;
                });
                Assert.True(backupStatus != null, UserMessageWithLog("Backup status is null."));
                Assert.True(originalNode == backupStatus.NodeTag, UserMessageWithLog($"Backup status node tag '{backupStatus.NodeTag}' does not match original node '{originalNode}'."));
                Assert.True(backupStatus.IsFull, UserMessageWithLog("Backup status is not full."));

                // assert number of directories
                Assert.NotNull(backupPath);
                var dirs = Directory.GetDirectories(backupPath);
                Log($"Number of backup directories: {dirs.Length}, dirs: {string.Join(", ", dirs)}");
                Assert.True(dirs.Length == 1, UserMessageWithLog($"Expected 1 backup directory but found {dirs.Length}."));

                var nodesBackupDir = dirs[0];
                var originalNodeFiles = Directory.GetFiles(nodesBackupDir);
                Log($"Number of files in backup directory: {originalNodeFiles.Length}, files: {string.Join(", ", originalNodeFiles)}");
                Assert.True(originalNodeFiles.Length == 1, UserMessageWithLog($"Expected 1 file in backup directory but found {originalNodeFiles.Length}."));
                Assert.True(originalNodeFiles[0].Contains($"-{originalNode}-backup"), UserMessageWithLog($"Backup file '{originalNodeFiles[0]}' does not contain original node tag '{originalNode}'."));

                // change responsible node to other node
                config.TaskId = taskId;
                config.MentorNode = otherNodeServer.ServerStore.NodeTag;
                var updatedTaskId = await Backup.UpdateConfigAsync(leader, config, store);
                Log($"Backup task updated with new mentor node: {otherNodeServer.ServerStore.NodeTag}, taskId: {updatedTaskId} (original: {taskId})");

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId, differentThan: originalNode);

                // delete compare exchanges -> tombstones created
                var delRes1 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/1", cmpxchngRes1.Index));
                var delRes2 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/2", cmpxchngRes2.Index));
                var delRes3 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/3", cmpxchngRes3.Index));
                Log($"Compare exchanges deleted: delRes1.Index = {delRes1.Index}, delRes2.Index = {delRes2.Index}, delRes3.Index = {delRes3.Index}");
                Assert.True(delRes1.Successful, UserMessageWithLog("DeleteCompareExchangeValueOperation for 'users/1' failed."));
                Assert.True(delRes2.Successful, UserMessageWithLog("DeleteCompareExchangeValueOperation for 'users/2' failed."));
                Assert.True(delRes3.Successful, UserMessageWithLog("DeleteCompareExchangeValueOperation for 'users/3' failed."));

                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(delRes3.Index);

                // first backup on new node will be full - this includes the tombstones
                await Backup.RunBackupAsync(otherNodeServer, taskId, store, isFullBackup: false);

                PeriodicBackupStatus status = null;
                // wait until status is updated with new node
                var res = await WaitForValueAsync(async () =>
                {
                    status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    return status.NodeTag == otherNodeServer.ServerStore.NodeTag;
                }, true, timeout: 70_000);
                Assert.True(res, UserMessageWithLog($"Backup status node tag '{status.NodeTag}' does not match other node '{otherNodeServer.ServerStore.NodeTag}'."));
                Assert.True(status.IsFull, UserMessageWithLog("Backup wasn't full."));
                Assert.True(status.LastFullBackup != null, UserMessageWithLog($"Last full backup is null."));
                Assert.True(status.LastRaftIndex.LastEtag >= delRes3.Index, UserMessageWithLog($"Tombstones raft index was not included in the backup. Expected: {delRes3.Index}, Actual: {status.LastRaftIndex.LastEtag}."));

                // assert number of directories grew
                var otherNodeDir = Directory.GetDirectories(backupPath).Except(dirs).ToArray();
                Assert.True(otherNodeDir.Length == 1, UserMessageWithLog($"Expected 1 new backup directory but found {otherNodeDir.Length}."));
                Assert.True(otherNodeDir[0].Contains($"-{otherNodeServer.ServerStore.NodeTag}-backup"), UserMessageWithLog($"New backup directory '{otherNodeDir[0]}' does not contain other node tag '{otherNodeServer.ServerStore.NodeTag}'."));
                var otherNodeFiles = Directory.GetFiles(otherNodeDir[0]);
                Assert.True(otherNodeFiles.Length == 1, UserMessageWithLog($"Expected 1 file in new backup directory but found {otherNodeFiles.Length}."));

                // run cx tombstone cleaner
                await Cluster.RunCompareExchangeTombstoneCleaner(leader, simulateClusterTransactionIndex: false);

                // tombstones should not have been deleted
                stats = store.Maintenance.ForDatabase(store.Database).Send(new GetDetailedStatisticsOperation());
                Assert.True(stats.CountOfCompareExchangeTombstones == 3, UserMessageWithLog($"CountOfCompareExchangeTombstones should be 3 but was {stats.CountOfCompareExchangeTombstones}."));
                Assert.True(stats.CountOfCompareExchange == 0, UserMessageWithLog($"CountOfCompareExchange should be 0 but was {stats.CountOfCompareExchange}."));
            }

            return;
            void Log(string textToLog) => logBuilder.AppendLine($"{DateTime.UtcNow:O}: {textToLog}");
            string UserMessageWithLog(string message) => $"{message}{Environment.NewLine}Log:{Environment.NewLine}{logBuilder}";
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster | RavenTestCategory.CompareExchange | RavenTestCategory.Sharding)]
        public async Task CompareExchangeTombstonesShouldNotBeCleanedIfNotBackedUpForLocalNodeSharded()
        {
            // A does full backup on compare exchanges, backup moves to B, cx deleted, B does full backup on their tombstones, backup moves back to A, A should still have the tombstones to back up
            // they should not have been deleted by the observer because of A's low index should prevent it
            var settings = new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Cluster.MaxClusterTransactionCompareExchangeTombstoneCheckInterval), "0" }, };
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var incrementalFrequency = "0 0 * * *";
            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0, watcherCluster: true, customSettings: settings);
            var options = Sharding.GetOptionsForCluster(leader, shards: 1, shardReplicationFactor: 3, orchestratorReplicationFactor: 3);

            using (var store = GetDocumentStore(options))
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

                var shardName = ShardHelper.ToShardName(store.Database, 0);
                var stats = store.Maintenance.ForDatabase(store.Database).ForNode("A").ForShard(0).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(3, stats.CountOfCompareExchange);
                
                // node backs up compare exchanges
                var config = new PeriodicBackupConfiguration()
                {
                    Disabled = false,
                    FullBackupFrequency = "0 0 1 1 *",
                    IncrementalBackupFrequency = incrementalFrequency,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                };
                var taskId = await Sharding.Backup.UpdateConfigAsync(leader, config, store);
                var originalNode = Backup.GetBackupResponsibleNode(leader, taskId, shardName);
                var originalNodeServer = nodes.Single(x => x.ServerStore.NodeTag == originalNode);
                var otherNodeServer = nodes.First(x => x.ServerStore.NodeTag != originalNode);

                // first backup will be full
                await Sharding.Backup.RunBackupAsync(store, taskId, isFullBackup: false);

                // check the backup status
                await WaitForAssertionAsync(async () =>
                {
                    var backupStatus = (await store.Maintenance.ForShard(0).SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    Assert.NotNull(backupStatus);
                    Assert.Equal(originalNode, backupStatus.NodeTag);
                    Assert.True(backupStatus.IsFull);
                });

                // assert number of directories
                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(1, dirs.Length);

                var nodesBackupDir = dirs[0];
                var originalNodeFiles = Directory.GetFiles(nodesBackupDir);
                Assert.Equal(1, originalNodeFiles.Length);
                Assert.True(originalNodeFiles[0].Contains(originalNode));

                // change responsible node to other node
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

                // delete compare exchanges -> tombstones created
                var delRes1 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/1", cmpxchngRes1.Index));
                var delRes2 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/2", cmpxchngRes2.Index));
                var delRes3 = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("users/3", cmpxchngRes3.Index));
                Assert.True(delRes1.Successful);
                Assert.True(delRes2.Successful);
                Assert.True(delRes3.Successful);

                // first backup on new node will be full - this includes the tombstones
                await Sharding.Backup.RunBackupAsync(store, taskId, isFullBackup: false);

                PeriodicBackupStatus status = null;
                // wait until status is updated with new node
                var res = await WaitForValueAsync(async () =>
                {
                    status = (await store.Maintenance.ForShard(0).SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    return status.NodeTag == otherNodeServer.ServerStore.NodeTag;
                }, true, timeout: 70_000);
                Assert.Equal(otherNodeServer.ServerStore.NodeTag, status.NodeTag);
                Assert.True(status.IsFull, "Backup wasn't full");
                Assert.NotNull(status.LastFullBackup);
                Assert.True(status.LastRaftIndex.LastEtag >= delRes3.Index, "tombstones raft index is not included in the backup");

                // assert number of directories grew
                var otherNodeDir = Directory.GetDirectories(backupPath).Except(dirs).ToArray();
                Assert.Equal(1, otherNodeDir.Length);
                Assert.True(otherNodeDir[0].Contains(otherNodeServer.ServerStore.NodeTag));
                var otherNodeFiles = Directory.GetFiles(otherNodeDir[0]);
                Assert.Equal(1, otherNodeFiles.Length);

                // run cx tombstone cleaner
                await Cluster.RunCompareExchangeTombstoneCleaner(leader, simulateClusterTransactionIndex: false);

                // tombstones should not have been deleted
                stats = store.Maintenance.ForDatabase(store.Database).ForNode("A").ForShard(0).Send(new GetDetailedStatisticsOperation());
                Assert.Equal(3, stats.CountOfCompareExchangeTombstones);
                Assert.Equal(0, stats.CountOfCompareExchange);
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

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Smuggler)]
        public async Task ChangingBackupConfigurationToAlsoIncrementalShouldNotCauseTombstoneLoss()
        {
            // have a full backup with only full frequency configuration, change config to include incremental frequency, make sure that cleaner did not rely on only having full backups to delete tombstones freely
            var backupPath = NewDataPath(suffix: "BackupFolder1");

            using var server = GetNewServer();
            string newDb;
            using (var store = GetDocumentStore(new Options { Server = server }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                // full backup without incremental
                var config = Backup.CreateBackupConfiguration(backupPath);

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
                var numberOfTombstonesDeleted = await documentDatabase.TombstoneCleaner.ExecuteCleanup();
                Assert.True(numberOfTombstonesDeleted == 0, $"There should be no tombstones deleted, but {numberOfTombstonesDeleted} were deleted");

                var beforeChangeConfig = DateTime.UtcNow;

                // change configuration to include incremental
                config.TaskId = taskId;
                config.IncrementalBackupFrequency = "* * * * *";
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                // check change applied
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record.PeriodicBackups.Count);
                Assert.Equal("* * * * *", record.PeriodicBackups.First().IncrementalBackupFrequency);

                // wait for the next backup
                PeriodicBackupStatus status;
                var res = await WaitForValueAsync(async () =>
                {
                    status = (await store.Maintenance.SendAsync(new GetPeriodicBackupStatusOperation(taskId))).Status;
                    return status.LastIncrementalBackup > beforeChangeConfig;
                }, true, timeout: 70_000, interval: 1000);
                Assert.True(res, "Incremental backup didn't happen");

                // restore from backup
                newDb = $"Restored-{store.Database}";
                var backupDir = Directory.GetDirectories(backupPath).First();
                var restoreConfig = new RestoreBackupConfiguration { BackupLocation = backupDir, DatabaseName = newDb };
                var restoreOperation = new RestoreBackupOperation(restoreConfig);
                var o = await store.Maintenance.Server.SendAsync(restoreOperation);
                await o.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
            }

            // check the original document is not included in the restored db
            using (var store = GetDocumentStore(new Options { Server = server, CreateDatabase = false, ModifyDatabaseName = _ => newDb }))
            using (var session = store.OpenSession())
            {
                var user = session.Load<User>("users/1");
                Assert.Null(user);
            }
        }

        [RavenFact(RavenTestCategory.ClusterTransactions | RavenTestCategory.BackupExportImport)]
        public async Task ClusterTransactionAfterSnapshotRestoreOnNewServer()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var db2Name = GetDatabaseName();

            const string userId = "users/1";
            var user1 = new User { Id = userId, Name = "SingleUser" };
            var db1Name = GetDatabaseName();
            using (var store1 = GetDocumentStore(new Options { ModifyDatabaseName = _ => db1Name }))
            {
                for (int i = 0; i < 16; i++)
                {
                    using (var session = store1.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        await session.StoreAsync(new User { Name = $"User{i}" }, $"users/{i}");
                        await session.SaveChangesAsync();
                    }
                }
            }

            using (var store2 = GetDocumentStore(new Options { ModifyDatabaseName = _ => db2Name }))
            {
                using (var session = store2.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    await session.StoreAsync(user1);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, backupType: BackupType.Snapshot);
                await Backup.UpdateConfigAndRunBackupAsync(Server, config, store2);
            }

            using var newServer = GetNewServer();

            var restoredDbName = GetDatabaseName();
            var backupLocation = Directory.GetDirectories(backupPath).First();

            using (var newStore = GetDocumentStore(new Options { Server = newServer, CreateDatabase = false, ModifyDatabaseName = _ => db2Name }))
            using (Backup.RestoreDatabase(newStore, new RestoreBackupConfiguration
            {
                BackupLocation = backupLocation,
                DatabaseName = restoredDbName
            }))

            using (var restoredStore = GetDocumentStore(new Options { Server = newServer, CreateDatabase = false, ModifyDatabaseName = _ => restoredDbName }))
            {
                using (var session = restoredStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    await session.StoreAsync(user1);
                    await Assert.ThrowsAsync<ClusterTransactionConcurrencyException>(async () => await session.SaveChangesAsync());
                }

                var user2 = new User { Id = "users/2", Name = "Grisha" };
                var user3 = new User { Id = "users/3", Name = "Igal" };

                using (var session = restoredStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    await session.StoreAsync(user2);
                    await session.SaveChangesAsync();
                }

                using (var session = restoredStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    await session.StoreAsync(user3);
                    await session.SaveChangesAsync();
                }

                using (var session = restoredStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var user = await session.LoadAsync<User>(user2.Id);
                    Assert.NotNull(user);
                    Assert.Equal(user2.Name, user.Name);

                    user = await session.LoadAsync<User>("users/3");
                    Assert.NotNull(user);
                    Assert.Equal(user3.Name, user.Name);
                }

                using (var session = restoredStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("SingleUser", user.Name);
                    user.Name = "Grisha";
                    await session.SaveChangesAsync();
                }

                using (var session = restoredStore.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var user = await session.LoadAsync<User>(userId);
                    Assert.NotNull(user);
                    Assert.Equal("Grisha", user.Name);
                }
            }
        }
    }
}
