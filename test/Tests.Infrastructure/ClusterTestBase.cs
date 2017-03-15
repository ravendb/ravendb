using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Rachis;
using Xunit;

namespace Tests.Infrastructure
{
    [Trait("Category", "Cluster")]
    public abstract class ClusterTestBase : RavenTestBase
    {
        private const int PortRangeStart = 9000;
        private const int ElectionTimeoutInMs = 300;
        private static int numberOfPortRequests;

        internal static int GetPort()
        {
            var portRequest = Interlocked.Increment(ref numberOfPortRequests);
            return PortRangeStart - (portRequest % 500);
        }

        protected List<RavenServer> Servers = new List<RavenServer>();
        private Random _random = new Random();

        protected void NoTimeouts()
        {
            TimeoutEvent.Disable = true;
        }
        protected async Task<RavenServer> CreateRaftClusterAndGetLeader(int numberOfNodes)
        {
            var leaderIndex = _random.Next(0, numberOfNodes);
            RavenServer leader = null;
            var serversToPorts = new Dictionary<RavenServer,string>();
            for (var i = 0; i < numberOfNodes; i++)
            {
                var serverUrl = $"http://localhost:{GetPort()}";
                var server = GetNewServer(new Dictionary<string, string>()
                {                    
                    {"Raven/ServerUrl", serverUrl}
                });
                serversToPorts.Add(server, serverUrl);
                Servers.Add(server);
                if (i == leaderIndex)
                {
                    server.ServerStore.EnsureNotPassive();
                    leader = server;
                }
            }
            for (var i = 0; i < numberOfNodes; i++)
            {
                if (i == leaderIndex)
                {
                    continue;
                }
                var follower = Servers[i];
                // ReSharper disable once PossibleNullReferenceException
                await leader.ServerStore.AddNodeToClusterAsync(serversToPorts[follower]);
                await follower.ServerStore.WaitForTopology(Leader.TopologyModification.Voter);
            }
            // ReSharper disable once PossibleNullReferenceException
            Assert.True(leader.ServerStore.WaitForState(RachisConsensus.State.Leader).Wait(numberOfNodes* ElectionTimeoutInMs),
                "The leader has changed while waiting for cluster to become stable");
            return leader;
        }


        public override void Dispose()
        {
            foreach (var server in Servers)
            {
                server.Dispose();
            }
            base.Dispose();
        }
    }
}
