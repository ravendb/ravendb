using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide;
using Raven.Server;
using Sparrow.Server.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17231 : ClusterTestBase
    {
        public RavenDB_17231(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public async Task FailuresDuringEmptyQueueShouldCauseReelection()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            var leaderTag = leader.ServerStore.NodeTag;
            var dbName = GetDatabaseName();
            var db = await CreateDatabaseInCluster(dbName, 3, leader.WebUrl);

            var cmdCount = 0;
            var exceptionThrown = new ManualResetEventSlim();
            leader.ServerStore.Engine.BeforeAppendToRaftLog += (context, cmd) =>
            {
                if (++cmdCount > 10)
                {
                    exceptionThrown.Set();
                    throw new DiskFullException("no more space");
                }
            };

            var stateChanged = new ManualResetEventSlim();
            leader.ServerStore.Engine.StateChanged += (_, transition) =>
            {
                if (transition.From == RachisState.Leader && transition.To == RachisState.Candidate)
                    stateChanged.Set();
            };

            var followers = nodes.Where(s => s != leader).ToList();
            var leaderSteppedDown = leader.ServerStore.Engine.WaitForLeaveState(RachisState.Leader, CancellationToken.None);
            var newLeaderElected = Task.WhenAny(followers.Select(s => s.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None)));
            var putConnectionStrings = Task.Run(async () =>
            {
                using (var store = new DocumentStore
                {
                    Database = dbName,
                    Urls = db.Servers.Select(s => s.WebUrl).ToArray()
                }.Initialize())
                {
                    var urls = nodes.Select(s => s.WebUrl).ToArray();
                    for (int i = 0; i < 20; i++)
                    {
                        var cs = new RavenConnectionString
                        {
                            Database = $"db/{i}",
                            Name = $"cs/{i}",
                            TopologyDiscoveryUrls = urls
                        };

                        await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(cs));
                    }
                }
            });

            Assert.True(exceptionThrown.Wait(TimeSpan.FromSeconds(10)), $"no exception thrown. commands count : {cmdCount}");

            Assert.True(stateChanged.Wait(TimeSpan.FromSeconds(15)),
                await AddErrorMessageAndClusterDebugLogs(nodes, 
                    new StringBuilder().AppendLine($"leader {leaderTag} did not have a state transition from 'Leader' to 'Candidate' after 15 seconds.")));

            Assert.True(leaderSteppedDown.Wait(TimeSpan.FromSeconds(15)), 
                await AddErrorMessageAndClusterDebugLogs(nodes, new StringBuilder().AppendLine($"leader {leaderTag} did not step down after 15 seconds")));

            Assert.True(newLeaderElected.Wait(TimeSpan.FromSeconds(15)), 
                await AddErrorMessageAndClusterDebugLogs(nodes, new StringBuilder().AppendLine($"old leader {leaderTag} stepped down, but no new leader was elected after 15 seconds")));
            
            await putConnectionStrings;
        }

        private async Task<string> AddErrorMessageAndClusterDebugLogs(IEnumerable<RavenServer> nodes, StringBuilder sb)
        {
            try
            {
                await GetClusterDebugLogsAsync(sb);
            }
            catch (Exception e)
            {
                sb.AppendLine("Failed to get cluster debug logs " + e);
            }

            foreach (var node in nodes)
            {
                GetStateTransitionsForNode(node, sb);
            }

            return sb.ToString();
        }

        private void GetStateTransitionsForNode(RavenServer node, StringBuilder sb)
        {
            var prevStates = node.ServerStore.Engine.PrevStates.Select(s => s.ToString()).ToList();
            sb.AppendLine($"{Environment.NewLine}State transitions for node {node.ServerStore.NodeTag}:{Environment.NewLine}-----------------------");

            foreach (var state in prevStates)
            {
                sb.AppendLine($"{state}{Environment.NewLine}");
            }

            sb.AppendLine();
        }
    }
}
