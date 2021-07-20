using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12921 : ClusterTestBase
    {
        public RavenDB_12921(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        public async Task Can_failover_after_consecutive_failures(int nodes)
        {
            var (_, leader) = await CreateRaftCluster(nodes);

            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = nodes
            }))
            {
                const string id = "orders/1";

                using (var session = (DocumentSession)store.OpenSession())
                {
                    session.Store(new Order { Company = "Hibernating Rhinos" }, id);
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<Order>(
                        session,
                        "orders/1",
                        u => u.Company.Equals("Hibernating Rhinos"),
                        TimeSpan.FromSeconds(10)));
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(id);
                    Assert.NotNull(order);
                }

                // dispose the first topology nodes, forcing the requestExecutor to failover to the last one
                var requestExecutor = store.GetRequestExecutor();
                for (var i = 0; i < requestExecutor.TopologyNodes.Count - 1; i++)
                {
                    var serverToDispose = Servers.FirstOrDefault(
                        srv => srv.ServerStore.NodeTag.Equals(requestExecutor.TopologyNodes[i].ClusterTag, StringComparison.OrdinalIgnoreCase));
                    Assert.NotNull(serverToDispose);

                    await DisposeServerAndWaitForFinishOfDisposalAsync(serverToDispose);
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(id);
                    Assert.NotNull(order);
                }
            }
        }

        [Theory]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        public async Task Will_throw_when_all_nodes_are_down(int nodes)
        {
            var (_, leader) = await CreateRaftCluster(nodes);

            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = nodes
            }))
            {
                const string id = "orders/1";

                using (var session = (DocumentSession)store.OpenSession())
                {
                    session.Store(new Order { Company = "Hibernating Rhinos" }, id);
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<Order>(
                        session,
                        "orders/1",
                        u => u.Company.Equals("Hibernating Rhinos"),
                        TimeSpan.FromSeconds(10)));
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(id);
                    Assert.NotNull(order);
                }

                // dispose all topology nodes
                var requestExecutor = store.GetRequestExecutor();
                foreach (var node in requestExecutor.TopologyNodes)
                {
                    var serverToDispose = Servers.FirstOrDefault(
                        srv => srv.ServerStore.NodeTag.Equals(node.ClusterTag, StringComparison.OrdinalIgnoreCase));
                    Assert.NotNull(serverToDispose);

                    await DisposeServerAndWaitForFinishOfDisposalAsync(serverToDispose);
                }

                using (var session = store.OpenSession())
                {
                    var exception = Assert.Throws<AllTopologyNodesDownException>(() => session.Load<Order>(id));
                    Assert.Contains("to all configured nodes in the topology, none of the attempt succeeded", exception.Message);
                }
            }
        }
    }
}
