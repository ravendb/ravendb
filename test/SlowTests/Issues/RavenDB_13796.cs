using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13796 : ClusterTestBase
    {
        public RavenDB_13796(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task TopologyUpdateDuringFailoverShouldntFaileCommand()
        {
            const int nodesAmount = 5;
            var (_, leader) = await CreateRaftCluster(nodesAmount);

            var defaultDatabase = GetDatabaseName();

            await CreateDatabaseInCluster(defaultDatabase, nodesAmount, leader.WebUrl).ConfigureAwait(false);

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = defaultDatabase
            }.Initialize())
            {
                var reqEx = store.GetRequestExecutor();

                Topology topology = null;

                while (reqEx.Topology == null)
                    await Task.Delay(100);
                topology = reqEx.Topology;
                var serverNode1 = topology.Nodes[0];
                await reqEx.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(serverNode1) { TimeoutInMs = 10_000 });
                var node1 = Servers.First(x => x.WebUrl.Equals(serverNode1.Url, StringComparison.InvariantCultureIgnoreCase));
                await DisposeServerAndWaitForFinishOfDisposalAsync(node1);
                var serverNode2 = topology.Nodes[1];
                var node2 = Servers.First(x => x.WebUrl.Equals(serverNode2.Url, StringComparison.InvariantCultureIgnoreCase));
                await DisposeServerAndWaitForFinishOfDisposalAsync(node2);

                using (reqEx.ContextPool.AllocateOperationContext(out var context))
                {
                    var command = new GetDocumentsCommand(0, 1);
                    var task = reqEx.ExecuteAsync(command, context);

                    var mre = new ManualResetEvent(false);

                    reqEx.OnFailedRequest += (sender, args) =>
                    {
                        mre.WaitOne();
                    };

                    while (command.FailedNodes == null || command.FailedNodes.Count == 0)
                        await Task.Delay(100);
                    while (await reqEx.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(topology.Nodes[3]) { TimeoutInMs = 10_000, ForceUpdate = true }) == false ||
                        reqEx.Topology.Etag == topology.Etag)
                    {
                        await Task.Delay(100);
                    }

                    mre.Set();
                    await task;
                }
            }
        }
    }
}
