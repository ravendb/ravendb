using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Http;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests
{
    public class DisableNodeOnClusterTest : ReplicationTestBase
    {
        [NightlyBuildFact]
        public async Task BackToFirstNodeAfterRevive()
        {
            var leader = await CreateRaftClusterAndGetLeader(3, shouldRunInMemory: false);
            await CreateDatabaseInCluster("MainDB", 3, leader.WebUrl);

            var leaderStore = new DocumentStore
            {
                Database = "MainDB",
                Urls = new[] {leader.WebUrl}
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
            var firstNode = Servers.Single(s => s.WebUrl == firstNodeUrl);
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
                { RavenConfiguration.GetKey(x => x.Core.ServerUrl), firstNodeUrl },
                { RavenConfiguration.GetKey(x => x.Core.DataDirectory), nodePath.FullPath }
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
                    },  Timeout.Infinite);
            } while (store.GetRequestExecutor().TopologyNodes.Count != replicationFactor);
        }
    }
}
