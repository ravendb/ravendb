using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19922 : ClusterTestBase
    {
        public RavenDB_19922(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task ResponsibleNodeForBackup_MentorNode()
        {
            DoNotReuseServer();

            const int clusterSize = 3;

            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();

            var settings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "0",
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1"
            };

            var (nodes, leaderServer) = await CreateRaftCluster(clusterSize, customSettings: settings, watcherCluster: true);
            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);

            using (var store = new DocumentStore
            {
                Urls = new[] { leaderServer.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
            })
            {
                var mentorNode = nodes.First(s => s.ServerStore.NodeTag != leaderServer.ServerStore.NodeTag);
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", mentorNode: mentorNode.ServerStore.NodeTag, name:"backup");

                long taskId = await InitializeBackup(store, clusterSize, leaderServer, nodes, config);

                var database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                var tag1 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);

                CheckDecisionLog(leaderServer, new MentorNode(tag1, config.Name).ReasonForDecisionLog);
                
                var disposedServer = nodes.First(s => s.ServerStore.NodeTag == tag1);

                await DisposeServerAndWaitForFinishOfDisposalAsync(disposedServer);

                string tag2 = "";
                await WaitForValueAsync( async () =>
                {
                    database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                    tag2 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                    return tag1.Equals(tag2);
                },false);

                Assert.Equal(tag1, tag2);

                CheckDecisionLog(leaderServer, $"Node '{tag1}' is currently in rehab state. The backup task '{config.Name}' will be moved to another node");
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task ResponsibleNodeForBackup_CurrentResponsibleNodeNotResponding()
        {
            DoNotReuseServer();

            const int clusterSize = 3;
            string tag2 = "";
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();

            var settings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "0",
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Backup.MoveToNewResponsibleNodeGracePeriod)] = "0"
            };

            var (nodes, leaderServer) = await CreateRaftCluster(clusterSize, customSettings: settings);
            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);

            using (var store = new DocumentStore
            {
                Urls = new[] { leaderServer.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
            })
            {
                var mentorNode = nodes.First(s => s.ServerStore.NodeTag != leaderServer.ServerStore.NodeTag);
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", mentorNode: mentorNode.ServerStore.NodeTag, name: "backup");
                long taskId = await InitializeBackup(store, clusterSize, leaderServer, nodes, config);
                
                var database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                var tag1 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);

                CheckDecisionLog(leaderServer, new MentorNode(tag1, config.Name).ReasonForDecisionLog);

                var disposedServer = nodes.First(s => s.ServerStore.NodeTag == tag1);
                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(disposedServer);
                nodes.Remove(disposedServer);
                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId);
                //Wait for new responsible node
                await WaitForValueAsync(async () =>
                {
                    database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                    tag2 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                    return tag1.Equals(tag2);
                }, false);
                Assert.NotEqual(tag1, tag2);

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId);

                CheckDecisionLog(leaderServer, new CurrentResponsibleNodeNotResponding(tag2, config.Name, tag1,TimeSpan.FromMinutes(0)).ReasonForDecisionLog);

                settings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url;
                using var server = GetNewServer(new ServerCreationOptions
                {
                    DeletePrevious = false, RunInMemory = false, DataDirectory = result.DataDirectory, CustomSettings = settings
                });
                nodes.Add(server);
                var val = await WaitForValueAsync(async () => await GetMembersCount(store), clusterSize);
                Assert.Equal(clusterSize, val);

                await WaitForValueAsync(async () =>
                {
                    database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                    tag2 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                    return tag1.Equals(tag2);
                }, true);

                Assert.Equal(tag1, tag2);

                CheckDecisionLog(leaderServer, new MentorNode(tag1, config.Name).ReasonForDecisionLog);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task ResponsibleNodeForBackup_PinnedMentorNode()
        {
            DoNotReuseServer();

            const int clusterSize = 3;

            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();
            string info = "";
            var settings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "0",
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Backup.MoveToNewResponsibleNodeGracePeriod)] = "0"
            };

            var (nodes, leaderServer) = await CreateRaftCluster(clusterSize, customSettings: settings, watcherCluster: true);
            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);

            using (var store = new DocumentStore
            {
                Urls = new[] { leaderServer.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
            })
            {
                var mentorNode = nodes.First(s => s.ServerStore.NodeTag != leaderServer.ServerStore.NodeTag);
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", mentorNode: mentorNode.ServerStore.NodeTag, name: "backup", pinToMentorNode:true);
                long taskId = await InitializeBackup(store, clusterSize, leaderServer, nodes, config);

                info += $"leader = {leaderServer.ServerStore.NodeTag}, mentorNode = {mentorNode.ServerStore.NodeTag}";

                var database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                var tag1 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);

                Assert.Equal(mentorNode.ServerStore.NodeTag, tag1);

                var op = new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup);
                var pinToMentorNode = store.Maintenance.Send(op).PinToMentorNode;

                Assert.True(pinToMentorNode);
                CheckDecisionLog(leaderServer, new PinnedMentorNode(tag1, config.Name).ReasonForDecisionLog, info);

                var disposedServer = nodes.First(s => s.ServerStore.NodeTag == tag1);
                nodes.Remove(disposedServer);
                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(disposedServer);

                string tag2 = "";
                await WaitForValueAsync(async () =>
                {
                    database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                    tag2 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                    return tag1.Equals(tag2);
                }, false);
                
                Assert.Equal(tag1, tag2);
                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId);

                settings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url;
                using var server = GetNewServer(new ServerCreationOptions
                {
                    DeletePrevious = false, RunInMemory = false, DataDirectory = result.DataDirectory, CustomSettings = settings
                });
                nodes.Add(server);
                var val = await WaitForValueAsync(async () => await GetMembersCount(store), clusterSize);
                Assert.Equal(clusterSize, val);

                await WaitForValueAsync(async () =>
                {
                    database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                    tag2 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                    return tag1.Equals(tag2);
                }, true);

                Assert.Equal(tag1, tag2);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task ResponsibleNodeForBackup_CurrentResponsibleNodeRemovedFromTopology()
        {
            DoNotReuseServer();

            const int clusterSize = 3;

            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();

            var settings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "0",
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Backup.MoveToNewResponsibleNodeGracePeriod)] = "0"
            };

            var (nodes, leaderServer) = await CreateRaftCluster(clusterSize, customSettings: settings);
            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);

            using (var store = new DocumentStore
            {
                Urls = new[] { leaderServer.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
            })
            {
                var mentorNode = nodes.First(s => s.ServerStore.NodeTag != leaderServer.ServerStore.NodeTag);
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", mentorNode: mentorNode.ServerStore.NodeTag, name: "backup");
                long taskId = await InitializeBackup(store, clusterSize, leaderServer, nodes, config);

                var database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                var tag1 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                CheckDecisionLog(leaderServer, new MentorNode(tag1, config.Name).ReasonForDecisionLog);

                var removedNode = nodes.First(s => s.ServerStore.NodeTag == tag1);

                await ActionWithLeader(l => l.ServerStore.RemoveFromClusterAsync(removedNode.ServerStore.NodeTag));

                string tag2 = "";
                await WaitForValueAsync(async () =>
                {
                    database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                    tag2 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                    return tag1.Equals(tag2);
                }, false);

                Assert.NotEqual(tag1, tag2);

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId);

                CheckDecisionLog(leaderServer, new CurrentResponsibleNodeRemovedFromTopology(tag2, config.Name,tag1).ReasonForDecisionLog);

            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task ResponsibleNodeForBackup_NonExistingResponsibleNode()
        {
            DoNotReuseServer();

            const int clusterSize = 3;

            var backupPath = NewDataPath(suffix: "BackupFolder");
            var databaseName = GetDatabaseName();

            var settings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "0",
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1"
            };

            var (nodes, leaderServer) = await CreateRaftCluster(clusterSize, customSettings: settings, watcherCluster: true);
            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);

            using (var store = new DocumentStore
            {
                Urls = new[] { leaderServer.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
            })
            {
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", name: "backup");

                long taskId = await InitializeBackup(store, clusterSize, leaderServer, nodes, config);

                var database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                var tag1 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);

                CheckDecisionLog(leaderServer, new NonExistingResponsibleNode(tag1, config.Name).ReasonForDecisionLog);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Delete_Backup_Task_Values_After_Task_Deletion()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "oren" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

                store.Maintenance.Send(new DeleteOngoingTaskOperation(backupTaskId, OngoingTaskType.Backup));

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var responsibleNodeInfo = Raven.Server.Utils.BackupUtils.GetResponsibleNodeInfoFromCluster(Server.ServerStore, context, store.Database, backupTaskId);
                    Assert.Null(responsibleNodeInfo);

                    var backupStatus = Raven.Server.Utils.BackupUtils.GetBackupStatusFromCluster(Server.ServerStore, context, store.Database, backupTaskId);
                    Assert.Null(backupStatus);
                }
            }
        }

        protected static async Task<int> GetMembersCount(IDocumentStore store, string databaseName = null)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName ?? store.Database));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Members.Count;
        }

        private static void CheckDecisionLog(RavenServer leaderServer, string reasonForDecisionLog, string info = "")
        {
            (ClusterObserverLogEntry[] List, long Iteration) dbDecisions = leaderServer.ServerStore.Observer.ReadDecisionsForDatabase();
            bool equals = false;
            WaitForValue(() =>
            {
                dbDecisions = leaderServer.ServerStore.Observer.ReadDecisionsForDatabase();
                equals = dbDecisions.List.Any(x => x.ToString().Contains(reasonForDecisionLog, StringComparison.OrdinalIgnoreCase));
                return equals;
            }, true);
            info += $", Iteration = {dbDecisions.Iteration}";
            Assert.True(equals, info + $"looking for :{reasonForDecisionLog}\n\nContents of dbDecisions:\n{string.Join("\n", dbDecisions.List.Select(entry => entry.ToString()))}");
        }

        private async Task<long> InitializeBackup(DocumentStore store, int clusterSize, RavenServer leaderServer, List<RavenServer> nodes, PeriodicBackupConfiguration config)
        {
            store.Initialize();
            await Backup.FillClusterDatabaseWithRandomDataAsync(databaseSizeInMb: 10, store, clusterSize);

            var database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
            Assert.NotNull(database);

            var taskId = await Backup.UpdateConfigAndRunBackupAsync(leaderServer, config, store, opStatus: OperationStatus.InProgress);

            Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId);

            await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
            return taskId;
        }
    }
}
