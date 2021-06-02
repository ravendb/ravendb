using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Replication;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Rachis.DatabaseCluster
{
    public class ReplicationTestsStress : ReplicationTestBase
    {
        public ReplicationTestsStress(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task WaitForReplicationShouldWaitOnlyForInternalNodes()
        {
            var clusterSize = 5;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
            var mainTopology = leader.ServerStore.GetClusterTopology();

            var secondLeader = await CreateRaftClusterAndGetLeader(1);
            var secondTopology = secondLeader.ServerStore.GetClusterTopology();

            var watchers = new List<ExternalReplication>();

            var watcherUrls = new Dictionary<string, string[]>();

            using (var store = new DocumentStore()
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            using (var secondStore = new DocumentStore()
            {
                Urls = new[] { secondLeader.WebUrl },
                Database = databaseName,
                Conventions =
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.AllNodes.Count());
                foreach (var node in mainTopology.AllNodes)
                {
                    var server = Servers.First(x => x.WebUrl == node.Value);
                    await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
                }

                for (var i = 0; i < 5; i++)
                {
                    var dbName = $"Watcher{i}";
                    doc = new DatabaseRecord(dbName);
                    var res = await secondStore.Maintenance.Server.SendAsync(
                        new CreateDatabaseOperation(doc));
                    watcherUrls.Add(dbName, res.NodesAddedTo.ToArray());
                    var server = Servers.Single(x => x.WebUrl == res.NodesAddedTo[0]);
                    await server.ServerStore.Cluster.WaitForIndexNotification(res.RaftCommandIndex);
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbName);

                    var watcher = new ExternalReplication(dbName, $"{dbName}-Connection");
                    await AddWatcherToReplicationTopology((DocumentStore)store, watcher, secondStore.Urls);
                    watchers.Add(watcher);
                }

                var notLeadingNode = mainTopology.AllNodes.Select(x => Servers.First(y => y.WebUrl == x.Value)).First(x => x.ServerStore.IsLeader() == false);
                notLeadingNode.Dispose();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    session.Advanced.WaitForReplicationAfterSaveChanges(TimeSpan.FromSeconds(clusterSize + 15), true, clusterSize - 1);
                    Task saveChangesTask = session.SaveChangesAsync();
                    WaitForDocumentInExternalReplication(watchers, watcherUrls);
                    await Assert.ThrowsAsync<RavenException>(() => saveChangesTask);
                    Assert.IsType<TimeoutException>(saveChangesTask.Exception?.InnerException?.InnerException);
                }
            }
        }


        [Fact]
        public async Task RavenDB_14435()
        {
            using (var src = GetDocumentStore())
            using (var dst = GetDocumentStore())
            {
                var database = await GetDocumentDatabaseInstanceFor(src);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "foo/bar");
                    session.SaveChanges();
                }

                using (var controller = new ReplicationController(database))
                {
                    var databaseWatcher1 = new ExternalReplication(dst.Database, $"ConnectionString-{src.Identifier}_1");
                    await AddWatcherToReplicationTopology(src, databaseWatcher1, src.Urls);
                    controller.ReplicateOnce();

                    Assert.NotNull(WaitForDocumentToReplicate<User>(dst, "foo/bar", 10_000));
                    await Task.Delay(ReplicationLoader.MaxInactiveTime.Add(TimeSpan.FromSeconds(10)));

                    var databaseWatcher2 = new ExternalReplication(dst.Database, $"ConnectionString-{src.Identifier}_2");
                    await AddWatcherToReplicationTopology(src, databaseWatcher2, src.Urls);

                    await Task.Delay(TimeSpan.FromSeconds(5));
                }

                await EnsureReplicatingAsync(src, dst);
            }
        }

        private bool WaitForDocument(string[] urls, string database)
        {
            using (var store = new DocumentStore
            {
                Urls = urls,
                Database = database
            }.Initialize())
            {
                return WaitForDocument<User>(store, "users/1", u => u.Name == "Karmel", timeout: 15_000);
            }
        }

        private void WaitForDocumentInExternalReplication(List<ExternalReplication> watchers, Dictionary<string, string[]> watcherUrls)
        {
            var reached = 0;
            foreach (var watcher in watchers)
            {
                if (WaitForDocument(watcherUrls[watcher.Database], watcher.Database))
                    reached++;
            }

            Assert.True(reached == watchers.Count, $"reached only {reached} out of {watchers.Count}");
        }
    }
}
