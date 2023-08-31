using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19887 : ClusterTestBase
    {
        public RavenDB_19887(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ClusterWideTransactions2Test()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            using var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = nodes.Count,
                RunInMemory = false
            });

            using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var doc = new Doc { Id = "doc-1", NumVal = 1 };
                session.Store(doc);
                session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(15), replicas: 2);
                session.SaveChanges();
            }

            using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var doc1 = session.Load<Doc>("doc-1");
                doc1.NumVal++;

                // if the node used for document loading is brought down here, the session.SaveChanges() below fails:
                // Raven.Client.Exceptions.ClusterTransactionConcurrencyException:
                // 'Failed to execute cluster transaction due to the following issues: Guard compare exchange value
                // 'rvn-atomic/doc-1' index does not match the transaction index's 0 change vector on doc-1
                // Concurrency check failed for putting the key 'rvn-atomic/doc-1'.Requested index: 0, actual index: 342'
                var responsibleNodeTag = store.GetRequestExecutor(store.Database).Topology.Nodes[0].ClusterTag;
                var responsibleNode = nodes.Single(n => n.ServerStore.NodeTag == responsibleNodeTag);
                var result0 = await DisposeServerAndWaitForFinishOfDisposalAsync(responsibleNode);

                session.SaveChanges();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var doc1 = await session.LoadAsync<Doc>("doc-1");
                Assert.Equal(doc1.NumVal, 2);
            }
        }

        private class Doc
        {
            public string Id { get; set; }
            public int NumVal { get; set; }
        }
    }
}
