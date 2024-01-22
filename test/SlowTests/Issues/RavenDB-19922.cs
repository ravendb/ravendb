using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents.PeriodicBackup;
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
        [InlineData(BackupType.Backup)]
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
                var mentorNode = Servers.First(s => s.ServerStore.NodeTag != leaderServer.ServerStore.NodeTag);
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", mentorNode: mentorNode.ServerStore.NodeTag, name:"backup");

                long taskId = await InitializeBackup(store, clusterSize, leaderServer, nodes, config);

                var database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                var tag1 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);

                CheckDecisionLog(leaderServer, new MentorNode(tag1, config.Name).ReasonForDecisionLog);
                
                var disposedServer = Servers.First(s => s.ServerStore.NodeTag == tag1);

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
        [InlineData(BackupType.Backup)]
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
                [RavenConfiguration.GetKey(x => x.Backup.MoveToNewResponsibleNodeGracePeriod)] = "1"
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
                var mentorNode = Servers.First(s => s.ServerStore.NodeTag != leaderServer.ServerStore.NodeTag);
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", mentorNode: mentorNode.ServerStore.NodeTag, name: "backup");
                long taskId = await InitializeBackup(store, clusterSize, leaderServer, nodes, config);
                
                var database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                var tag1 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);

                CheckDecisionLog(leaderServer, new MentorNode(tag1, config.Name).ReasonForDecisionLog);

                var disposedServer = Servers.First(s => s.ServerStore.NodeTag == tag1);
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

                CheckDecisionLog(leaderServer, new CurrentResponsibleNodeNotResponding(tag2, config.Name, tag1,TimeSpan.FromSeconds(1)).ReasonForDecisionLog);

                settings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url;
                nodes.Add(GetNewServer(new ServerCreationOptions
                {
                    DeletePrevious = false,
                    RunInMemory = false,
                    DataDirectory = result.DataDirectory,
                    CustomSettings = settings
                }));
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
        [InlineData(BackupType.Backup)]
        public async Task ResponsibleNodeForBackup_PinnedMentorNode()
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
                [RavenConfiguration.GetKey(x => x.Backup.MoveToNewResponsibleNodeGracePeriod)] = "1"
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
                var mentorNode = Servers.First(s => s.ServerStore.NodeTag != leaderServer.ServerStore.NodeTag);
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", mentorNode: mentorNode.ServerStore.NodeTag, name: "backup", pinToMentorNode:true);
                long taskId = await InitializeBackup(store, clusterSize, leaderServer, nodes, config);

                var database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                var tag1 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);

                CheckDecisionLog(leaderServer, new PinnedMentorNode(tag1, config.Name).ReasonForDecisionLog);

                var disposedServer = Servers.First(s => s.ServerStore.NodeTag == tag1);
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
                nodes.Add(GetNewServer(new ServerCreationOptions
                {
                    DeletePrevious = false,
                    RunInMemory = false,
                    DataDirectory = result.DataDirectory,
                    CustomSettings = settings
                }));
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
        [InlineData(BackupType.Backup)]
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
                [RavenConfiguration.GetKey(x => x.Backup.MoveToNewResponsibleNodeGracePeriod)] = "1"
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
                var mentorNode = Servers.First(s => s.ServerStore.NodeTag != leaderServer.ServerStore.NodeTag);
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", mentorNode: mentorNode.ServerStore.NodeTag, name: "backup");
                long taskId = await InitializeBackup(store, clusterSize, leaderServer, nodes, config);

                var database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                var tag1 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                CheckDecisionLog(leaderServer, new MentorNode(tag1, config.Name).ReasonForDecisionLog);

                var removedNode = Servers.First(s => s.ServerStore.NodeTag == tag1);

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
        [InlineData(BackupType.Backup)]
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

        protected static async Task<int> GetMembersCount(IDocumentStore store, string databaseName = null)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName ?? store.Database));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Members.Count;
        }

        private static void CheckDecisionLog(RavenServer leaderServer, string reasonForDecisionLog)
        {
            var dbDecisions = leaderServer.ServerStore.Observer.ReadDecisionsForDatabase();
            bool equals = dbDecisions.List.Any(x => x.ToString().Contains(reasonForDecisionLog, StringComparison.OrdinalIgnoreCase));
            Assert.True(equals, reasonForDecisionLog);
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
