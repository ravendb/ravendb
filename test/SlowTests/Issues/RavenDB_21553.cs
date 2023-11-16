using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Database;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide.Commands;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21553 : ClusterTestBase
    {
        public RavenDB_21553(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi, Skip = "fix me")]
        public async Task Topology_Change_Shouldnt_Trigger_SpeedTest()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);

            using (var leaderStore = GetDocumentStore(new Options
            {
                ReplicationFactor = 3,
                ModifyDocumentStore = s => s.Conventions.ReadBalanceBehavior = ReadBalanceBehavior.FastestNode,
                Server = leader
            }))
            {
                List<string> originalNodesOrder = new() { "A", "B", "C" };
                originalNodesOrder.Shuffle();
                
                var speedTestsCount = 0;
                int scheduledSpeedTestsBeforeStopping;
                var re = leaderStore.GetRequestExecutor();

                await leaderStore.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(leaderStore.Database, originalNodesOrder));

                re.ForTestingPurposesOnly().ExecuteOnAllToFigureOutTheFastestOnBeforeWait = t => Interlocked.Increment(ref speedTestsCount);
               
                using (var session = leaderStore.OpenAsyncSession())
                {
                    // first node to reach 10 winning speed tests is the fastest. at that point we stop scheduling any more
                    for (var i = 0; i <= 10 * 3; i++)
                    {
                        await session.Query<object>().ToListAsync();
                        if (re._nodeSelector?._state?.FastestRecords.Any(x => x >= 10) == true)
                        {
                            break;
                        }
                    }
                    
                    Assert.True(speedTestsCount >= 10);
                    scheduledSpeedTestsBeforeStopping = speedTestsCount;
                }
                
                // speed tests should no longer happen
                using (var session = leaderStore.OpenAsyncSession())
                {
                    await session.Query<object>().ToListAsync();
                    Assert.Equal(scheduledSpeedTestsBeforeStopping, speedTestsCount);
                }
                
                //reordering should not trigger new speed tests any more since we check the fastest node still exists in the topology
                originalNodesOrder.Shuffle();
                await leaderStore.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(leaderStore.Database, originalNodesOrder));
                
                using (var session = leaderStore.OpenAsyncSession())
                {
                    await session.Query<object>().ToListAsync();
                    Assert.Equal(scheduledSpeedTestsBeforeStopping, speedTestsCount);
                }
                
                originalNodesOrder.Shuffle();
                await leaderStore.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(leaderStore.Database, originalNodesOrder));
                
                using (var session = leaderStore.OpenAsyncSession())
                {
                    await session.Query<object>().ToListAsync();
                    Assert.Equal(scheduledSpeedTestsBeforeStopping, speedTestsCount);
                }

                var state = re._nodeSelector._state;
                var fastestNode = state.Nodes[state.Fastest];
                
                //will trigger new speed tests
                await leaderStore.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(leaderStore.Database, true, fromNode: fastestNode.ClusterTag));

                //wait until db is fully removed from node
                var removedServer = nodes.Single(x => x.ServerStore.NodeTag == fastestNode.ClusterTag);
                await AssertWaitForTrueAsync(async () =>
                {
                    try
                    {
                        await Databases.GetDocumentDatabaseInstanceFor(removedServer, leaderStore);
                    }
                    catch (DatabaseNotRelevantException)
                    {
                        return true;
                    }
                    return false;
                });
                
                using (var session = leaderStore.OpenAsyncSession())
                {
                    await session.Query<object>().ToListAsync();
                    await session.Query<object>().ToListAsync();
                    
                    await AssertWaitForValueAsync(() => Task.FromResult(re._nodeSelector._state.Nodes.Count), 2);

                    await AssertWaitForTrueAsync(() => Task.FromResult(speedTestsCount > scheduledSpeedTestsBeforeStopping));
                }
            }
        }
    }
}
