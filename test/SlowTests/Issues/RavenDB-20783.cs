using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20783 : ReplicationTestBase
    {
        public RavenDB_20783(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(30 * 10) : TimeSpan.FromSeconds(3);

        [Fact]
        public async Task NullTimerInRecordFastest()
        {
            var (servers, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var mre = new ManualResetEventSlim(false);
            var mre2 = new ManualResetEventSlim(false);
            using (var store = GetDocumentStore(new Options
            {
                Server = servers[0],
                ModifyDatabaseName = s => $"{s}_1",
                ReplicationFactor = 3,
                ModifyDocumentStore = s => s.Conventions.ReadBalanceBehavior = Raven.Client.Http.ReadBalanceBehavior.FastestNode
            }))
            {
                var requestExecutor = store.GetRequestExecutor();
                NodeSelector node = null;
                requestExecutor.ForTestingPurposesOnly().OnBeforeScheduleSpeedTest = (n) =>
                {
                    node = n;
                    mre2.Set();
                    Assert.True(mre.Wait(_reasonableWaitTime), "Waited too long");
                };
                _ = requestExecutor.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(new ServerNode { Url = servers[0].WebUrl, Database = store.Database })
                {
                    TimeoutInMs = Timeout.Infinite,
                    ForceUpdate = true,
                    DebugTag = "first-topology-update",
                });
                Assert.True(mre2.Wait(_reasonableWaitTime), "Waited too long");
                for (int i = 0; i < 10; i++)
                {
                    node.RecordFastest(0, node.Topology.Nodes[0]);
                }

                mre.Set();
            }
        }
    }
}
