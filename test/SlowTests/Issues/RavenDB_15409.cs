using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json;
using Sparrow.Logging;
using Tests.Infrastructure;
using Voron.Util;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15409 : ClusterTestBase
    {
        [Fact]
        public async Task DoNotCallUpdateLicenseLimitsCommandOnEveryLeaderChange()
        {
            await using var logStream = new MemoryStream();
            using var dispose = new DisposableAction(() => LoggingSource.Instance.DetachPipeSink());
            LoggingSource.Instance.AttachPipeSink(logStream);

            var (servers, leader) = await CreateRaftCluster(3);
            await WaitForRaftIndexToBeAppliedInCluster(9, TimeSpan.FromSeconds(15));
            var expected = new HashSet<long>();
            foreach (var server in servers)
            {
                expected.Add(GetRaftCommandByType(server, nameof(UpdateLicenseLimitsCommand)).Count());
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
                async Task<string> MassageFactory()
                {
                    LoggingSource.Instance.DetachPipeSink();
                    logStream.Seek(0, SeekOrigin.Begin);
                    using StreamReader reader = new(logStream);
                    return await reader.ReadToEndAsync();
                }
                await RavenTestHelper.AssertAllAsync(MassageFactory, servers.Select(s => (Action)(() =>
                {
                    var actual = GetRaftCommandByType(s, nameof(UpdateLicenseLimitsCommand)).Count();
                    Assert.True(expected.Single() == actual, 
                        $"{s.ServerStore.NodeTag} expect {expected.Single()} actual {actual} " +
                                $" {string.Join($"{Environment.NewLine}\t", GetRaftCommandByType(s).Select(c => ctx.ReadObject(c, "raftCommand").ToString()))}");
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
