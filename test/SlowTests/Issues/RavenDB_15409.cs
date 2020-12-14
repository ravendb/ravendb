using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15409 : ClusterTestBase
    {
        [Fact]
        public async Task DoNotCallUpdateLicenseLimitsCommandOnEveryLeaderChange()
        {
            var (servers, leader) = await CreateRaftCluster(3);
            await WaitForRaftIndexToBeAppliedInCluster(9, TimeSpan.FromSeconds(15));
            var expected = new HashSet<long>();
            foreach (var server in servers)
            {
                expected.Add(CountOfRaftCommandByType(server, nameof(UpdateLicenseLimitsCommand)));
            }

            Assert.Single(expected);

            for (int i = 0; i < 10; i++)
            {
                await ActionWithLeader(l =>
                {
                    l.ServerStore.Engine.CurrentLeader.StepDown();
                    return l.ServerStore.Engine.WaitForLeaderChange(l.ServerStore.ServerShutdown);
                });
            }

            foreach (var server in servers)
            {
                Assert.Equal(expected.Single(), CountOfRaftCommandByType(server, nameof(UpdateLicenseLimitsCommand)));
            }
        }

        [Fact]
        public async Task CanWaitForTopologyModification()
        {
            var (servers, leader) = await CreateRaftCluster(3);
            var follower = servers.First(x => x != leader);

            // demote node to watcher
            await leader.ServerStore.Engine.ModifyTopologyAsync(follower.ServerStore.NodeTag, follower.WebUrl, Leader.TopologyModification.NonVoter);
            await WaitForRaftIndexToBeAppliedInCluster(10, TimeSpan.FromSeconds(15));
        }

        public RavenDB_15409(ITestOutputHelper output) : base(output)
        {
        }
    }
}
