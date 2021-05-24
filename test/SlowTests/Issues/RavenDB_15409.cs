using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json;
using Sparrow.Logging;
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
            using var socket = new DummyWebSocket();

            var (servers, leader) = await CreateRaftCluster(3);
            var _ = LoggingSource.Instance.Register(socket, new LoggingSource.WebSocketContext(), CancellationToken.None);

            await WaitForRaftIndexToBeAppliedInCluster(9, TimeSpan.FromSeconds(15));
            var expected = new HashSet<long>();
            foreach (var server in servers)
            {
                expected.Add(GetRaftCommands(server, nameof(UpdateLicenseLimitsCommand)).Count());
            }

            if (expected.Count != 1)
            {
                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    var massageBuilder = new StringBuilder();
                    foreach (var server in servers)
                    {
                        var serverRaftCommands = GetRaftCommands(server).Select(c => ctx.ReadObject(c, "raftCommand").ToString());

                        massageBuilder
                            .AppendFormat("**** Node {0} ****", server.ServerStore.NodeTag).AppendLine()
                            .AppendLine(string.Join('\n', serverRaftCommands));
                    }

                    massageBuilder.Append(await socket.CloseAndGetLogsAsync());
                    Assert.False(true, massageBuilder.ToString());
                }
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

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                await RavenTestHelper.AssertAllAsync(async () => await socket.CloseAndGetLogsAsync(), servers.Select(s => (Action)(() =>
                {
                    var actual = GetRaftCommands(s, nameof(UpdateLicenseLimitsCommand)).Count();
                    Assert.True(expected.Single() == actual, 
                        $"{s.ServerStore.NodeTag} expect {expected.Single()} actual {actual} " +
                                $" {string.Join($"{Environment.NewLine}\t", GetRaftCommands(s).Select(c => ctx.ReadObject(c, "raftCommand").ToString()))}");
                })).ToArray());
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
