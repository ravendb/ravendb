using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Http;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20783 : ReplicationTestBase
    {
        public RavenDB_20783(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task NullTimerInRecordFatest()
        {
            var (servers, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var mre = new ManualResetEvent(false);
            using (var store1 = GetDocumentStore(new Options
                   {
                       Server = servers[0],
                       ModifyDatabaseName = s => $"{s}_1",
                       ReplicationFactor = 3,
                       ModifyDocumentStore = s => s.Conventions.ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior.FastestNode
                   }))
            {
                var e = store1.GetRequestExecutor();
                NodeSelector node = null;
                e.ForTestingPurposesOnly().WaitForRecoredfastest = async (n) =>
                {
                    node = n;
                    mre.WaitOne();
                };
                _ = e.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(new ServerNode { Url = servers[0].WebUrl, Database = store1.Database })
                {
                    TimeoutInMs = Timeout.Infinite,
                    ForceUpdate = true,
                    DebugTag = "first-topology-update",
                });
                await Task.Delay(TimeSpan.FromSeconds(2)).ConfigureAwait(false);

                for (int i = 0; i < 10; i++)
                {
                    try
                    {
                        node.RecordFastest(0, node.Topology.Nodes[0]);
                    }
                    catch (Exception exception)
                    {
                        Assert.Fail(exception.Message + "Stack Track : " + exception.StackTrace);
                    }
                }

                mre.Set();
            }
        }
    }
}
