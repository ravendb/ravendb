using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Server;
using Raven.Server.Rachis;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests
{
    public class AddNodeToClusterTests : ReplicationBasicTests
    {
        [Fact]
        public async Task FailOnAddingNonPassiveNode()
        {
            var raft1 = await CreateRaftClusterAndGetLeader(1);
            var raft2 = await CreateRaftClusterAndGetLeader(1);
            
            var url = raft2.WebUrls[0];
            await raft1.ServerStore.AddNodeToClusterAsync(url);
            Assert.True(await WaitForValueAsync(() => raft1.ServerStore.GetClusterErrors().Count > 0,true));
        }


        [Fact]
        public async Task RemoveNodeWithDb()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            var db = await CreateDatabaseInCluster("MainDB", 3, leader.WebUrls[0]);
            var watcherDb = await CreateDatabaseInCluster("WatcherDB", 1, leader.WebUrls[0]);
            
            var leaderStore = new DocumentStore
            {
                Database = "MainDB",
                Urls = leader.WebUrls
            }.Initialize();
            leaderStore.Conventions.DisableTopologyUpdates = true;

            var watcherStore = new DocumentStore
            {
                Database = "WatcherDB",
                Urls = watcherDb.Item2.Single().WebUrls
            }.Initialize();
            watcherStore.Conventions.DisableTopologyUpdates = true;

            var watcher = new DatabaseWatcher
            {
                Database = "WatcherDB",
                Url = watcherDb.Item2.Single().WebUrls[0]
            };
            var res = await AddWatcherToReplicationTopology((DocumentStore)leaderStore, watcher);
            var responsibleServer = Servers.Single(s => s.ServerStore.NodeTag == res.ResponsibleNode);

            Console.WriteLine("LeaderNode = " + leader.ServerStore.NodeTag);
            Console.WriteLine("WathcerNode = " + watcherDb.Item2.Single().ServerStore.NodeTag);
            Console.WriteLine("ResponsibleNode = " + res.ResponsibleNode);

            var responsibleStore = new DocumentStore
            {
                Database = "MainDB",
                Urls = responsibleServer.WebUrls
            }.Initialize();
            responsibleStore.Conventions.DisableTopologyUpdates = true;

            using (var session = leaderStore.OpenSession())
            {
                 session.Store(new User
                {
                    Name = "Karmel"
                }, "users/1");
                 session.SaveChanges();
                await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "usres/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(5));
            }
            await leader.ServerStore.RemoveFromClusterAsync(res.ResponsibleNode);
            await responsibleServer.ServerStore.WaitForState(RachisConsensus.State.Passive);

            using (var session = responsibleStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Name = "Karmel2"
                }, "users/2");
                await session.SaveChangesAsync();
            }
            Assert.Null(WaitForDocumentToReplicate<User>(watcherStore, "users/2", 1000));

            leaderStore.Dispose();
            watcherStore.Dispose();
            responsibleStore.Dispose();
        }
    }
}
