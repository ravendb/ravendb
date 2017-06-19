using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Http;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace RachisTests
{
    public class DisableNodeOnClusterTest : ReplicationTestsBase
    {
        [Fact]
        public async Task BackToFirstNodeAfterRevive()
        {
            var leader = await CreateRaftClusterAndGetLeader(3, shouldRunInMemory: false);
            await CreateDatabaseInCluster("MainDB", 3, leader.WebUrls[0]);

            var leaderStore = new DocumentStore
            {
                Database = "MainDB",
                Urls = leader.WebUrls
            }.Initialize();

            await WaitForDatabaseTopology(leaderStore, leaderStore.Database, 3);

            var re = leaderStore.GetRequestExecutor();
            using (var session = leaderStore.OpenSession())
            {
                session.Store(new User
                {
                    Name = "Idan"
                });
                session.SaveChanges();
            }

            var firstNodeUrl = re.Url;
            var firstNode = Servers.Single(s => s.WebUrls[0] == firstNodeUrl);
            var nodePath = firstNode.Configuration.Core.DataDirectory;

            firstNode.Dispose();

            // check that replication works.
            using (var session = leaderStore.OpenSession())
            {
                session.Store(new User
                {
                    Name = "Karmel"
                }, "users/1");
                session.SaveChanges();
            }

            Assert.NotEqual(re.Url, firstNodeUrl);
            var customSettings = new Dictionary<string, string>
            {
                { "Raven/ServerUrl", firstNodeUrl },
                {"Raven/DataDir",nodePath.FullPath }
            };
            GetNewServer(customSettings, runInMemory: false);

            Assert.True(SpinWait.SpinUntil(() => firstNodeUrl == re.Url, TimeSpan.FromSeconds(15)));
            Assert.Equal(firstNodeUrl, re.Url);
            leaderStore.Dispose();
        }

        private static async Task WaitForDatabaseTopology(IDocumentStore store, string databaseName, int replicationFactor)
        {
            do
            {
                await store.GetRequestExecutor()
                    .UpdateTopologyAsync(new ServerNode
                    {
                        Url = store.Urls[0],
                        Database = databaseName,
                    }, Timeout.Infinite);
            } while (store.GetRequestExecutor().TopologyNodes.Count != replicationFactor);
        }
    }
}
