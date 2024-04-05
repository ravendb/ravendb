using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server;
using Sparrow.Server;
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
            var exceptionThrown = new AsyncManualResetEvent();
            leader.ServerStore.Engine.BeforeAppendToRaftLog += (context, cmd) =>
            {
                if (++cmdCount > 10)
                {
                    exceptionThrown.Set();
                    throw new DiskFullException("no more space");
                }
            };

            var stateChanged = new AsyncManualResetEvent();
            leader.ServerStore.Engine.StateChanged += (_, transition) =>
            {
                if (transition.From == RachisState.Leader && transition.To == RachisState.Candidate)
                    stateChanged.Set();
            };

            var followers = nodes.Where(s => s != leader).ToList();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var leaderSteppedDown = leader.ServerStore.Engine.WaitForLeaveState(RachisState.Leader, cts.Token);
            var newLeaderElected = Task.WhenAny(followers.Select(s => s.ServerStore.WaitForState(RachisState.Leader, cts.Token)));
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


            Assert.True(await exceptionThrown.WaitAsync(TimeSpan.FromSeconds(10)), $"no exception thrown. commands count : {cmdCount}");

            Assert.True(await stateChanged.WaitAsync(TimeSpan.FromSeconds(15)),
                await AddErrorMessageAndClusterDebugLogs(nodes, 
                    new StringBuilder().AppendLine($"leader {leaderTag} did not have a state transition from 'Leader' to 'Candidate' after 15 seconds.")));

            Assert.True(await leaderSteppedDown.WaitWithTimeout(TimeSpan.FromSeconds(15)), 
                await AddErrorMessageAndClusterDebugLogs(nodes, new StringBuilder().AppendLine($"leader {leaderTag} did not step down after 15 seconds")));

            var r = await newLeaderElected;
            // this task is already completed
            Assert.True(r.IsCompleted,$"newLeaderElected was not completed");
#pragma warning disable xUnit1031
            Assert.True(r.Result, 
#pragma warning restore xUnit1031
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
