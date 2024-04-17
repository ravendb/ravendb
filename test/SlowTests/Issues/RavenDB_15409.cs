using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15409 : ClusterTestBase
    {
        public RavenDB_15409(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task DoNotCallUpdateLicenseLimitsCommandOnEveryLeaderChange()
        {
            const int numberOfNodes = 3;
            (List<RavenServer> servers, _) = await CreateRaftCluster(numberOfNodes);

            // Verifying that each node in the cluster has issued the same number of `UpdateLicenseLimitsCommand` commands,
            // as each node is expected to independently send its own details.
            Cluster.AssertNumberOfCommandsPerNode(expectedNumberOfCommands: numberOfNodes, servers, nameof(UpdateLicenseLimitsCommand));

            // 10 leader changes
            for (int i = 0; i < 10; i++)
                await ActionWithLeader(leader =>
                {
                    leader.ServerStore.Engine.CurrentLeader.StepDown();
                    return leader.ServerStore.Engine.WaitForLeaderChange(leader.ServerStore.ServerShutdown);
                });

            // Nothing should have changed
            Cluster.AssertNumberOfCommandsPerNode(expectedNumberOfCommands: numberOfNodes, servers, nameof(UpdateLicenseLimitsCommand));
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task CanWaitForTopologyModification()
        {
            var (servers, leader) = await CreateRaftCluster(3);
            var follower = servers.First(x => x != leader);

            // demote node to watcher
            await leader.ServerStore.Engine.ModifyTopologyAsync(follower.ServerStore.NodeTag, follower.WebUrl, Leader.TopologyModification.NonVoter);
            await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(10, TimeSpan.FromSeconds(15));
        }
    }
}
