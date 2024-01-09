using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
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
        public async Task DoNotChangeResponsibleNode()
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

            var (nodes, leaderServer) = await CreateRaftCluster(clusterSize, customSettings: settings);
            await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);


            using (var store = new DocumentStore
            {
                Urls = new[] { leaderServer.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true },
                Database = databaseName
            })
            {
                store.Initialize();
                await Backup.FillClusterDatabaseWithRandomDataAsync(databaseSizeInMb: 10, store, clusterSize);

                var database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                Assert.NotNull(database);

                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", mentorNode: leaderServer.ServerStore.NodeTag);
                var taskId = await Backup.UpdateConfigAndRunBackupAsync(leaderServer, config, store, opStatus: OperationStatus.InProgress);

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId);

                var tag1 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);

                var disposedServer = Servers.Single(s => s.ServerStore.NodeTag == tag1);

                await DisposeServerAndWaitForFinishOfDisposalAsync(disposedServer);

                string tag2 = "";
                var sever = Servers.First(s => s.ServerStore.NodeTag != disposedServer.ServerStore.NodeTag);
                await WaitForValueAsync( async () =>
                {
                    database = await sever.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);

                    tag2 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                    return tag1.Equals(tag2);
                },false);

                Assert.Equal(tag1, tag2);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        [InlineData(BackupType.Backup)]
        public async Task ChangeResponsibleNode()
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
                store.Initialize();
                await Backup.FillClusterDatabaseWithRandomDataAsync(databaseSizeInMb: 10, store, clusterSize);

                var database = await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                Assert.NotNull(database);

                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* * * * *", mentorNode: leaderServer.ServerStore.NodeTag);
                var taskId = await Backup.UpdateConfigAndRunBackupAsync(leaderServer, config, store, opStatus: OperationStatus.InProgress);

                Backup.WaitForResponsibleNodeUpdateInCluster(store, nodes, taskId);

                var tag1 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                await leaderServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);

                var disposedServer = Servers.Single(s => s.ServerStore.NodeTag == tag1);

                var index = nodes.FindIndex(n => n.ServerStore.NodeTag == disposedServer.ServerStore.NodeTag);

                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(disposedServer);

                string tag2 = "";
                var sever = Servers.First(s => s.ServerStore.NodeTag != disposedServer.ServerStore.NodeTag);
                await WaitForValueAsync(async () =>
                {
                    database = await sever.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                    tag2 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                    return tag1.Equals(tag2);
                }, false);


                Assert.NotEqual(tag1, tag2);

                settings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url;
                nodes[index]= GetNewServer(new ServerCreationOptions
                {
                    DeletePrevious = false,
                    RunInMemory = false,
                    DataDirectory = result.DataDirectory,
                    CustomSettings = settings
                });
                var val = await WaitForValueAsync(async () => await GetMembersCount(store), clusterSize);
                Assert.Equal(clusterSize, val);

                await WaitForValueAsync(async () =>
                {
                    database = await sever.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).ConfigureAwait(false);
                    tag2 = database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                    return tag1.Equals(tag2);
                }, true);


                Assert.Equal(tag1, tag2);
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
    }
}
