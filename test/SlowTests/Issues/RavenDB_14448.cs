using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14448 : ClusterTestBase
    {
        public RavenDB_14448(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Validation()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = true
            }))
            {
                Exception e = Assert.Throws<ArgumentException>(() => store.Maintenance.Server.ForNode(null));
                Assert.Contains("Value cannot be null or whitespace", e.Message);

                e = Assert.Throws<InvalidOperationException>(() => store.Maintenance.Server.ForNode("A"));
                Assert.Contains("Cannot switch server operation executor", e.Message);
            }

            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<InvalidOperationException>(() => store.Maintenance.Server.ForNode("B"));
                Assert.Contains("Could not find node", e.Message);

                var re1 = store.Maintenance.Server.ForNode("A");
                var re2 = store.Maintenance.Server.ForNode("A");

                Assert.Equal(re1, re2);

                re2 = re2.ForNode("A");

                Assert.Equal(re1, re2);

                // should not remove from cache
                re1.Dispose();

                re1 = store.Maintenance.Server.ForNode("A");

                Assert.Equal(re1, re2);
            }
        }

        [Fact]
        public async Task CanSwitch()
        {
            var (_, leader) = await CreateRaftCluster(3);
            var databaseName = GetDatabaseName();

            using (var store = new DocumentStore()
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var createRes = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc));

                var nodeA = store.Maintenance.Server.ForNode("A");
                var nodeB = store.Maintenance.Server.ForNode("B");

                Assert.NotEqual(nodeA, nodeB);

                var operation = new GetNodeInfoOperation();

                var nodeInfoA = await nodeA.SendAsync(operation);
                var nodeInfoB = await nodeB.SendAsync(operation);

                Assert.Equal("A", nodeInfoA.NodeTag);
                Assert.Equal("B", nodeInfoB.NodeTag);

                var nodeA2 = nodeB.ForNode("A");
                var nodeB2 = nodeA.ForNode("B");
                var nodeC = nodeB.ForNode("C");

                Assert.Equal(nodeA, nodeA2);
                Assert.Equal(nodeB, nodeB2);
                Assert.NotEqual(nodeA, nodeC);

                var nodeInfoC = await nodeC.SendAsync(operation);

                Assert.Equal("C", nodeInfoC.NodeTag);
            }
        }

        private class GetNodeInfoOperation : IServerOperation<NodeInfo>
        {
            public RavenCommand<NodeInfo> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new GetNodeInfoCommand(TimeSpan.FromSeconds(30));
            }

            private class GetNodeInfoCommand : RavenCommand<NodeInfo>
            {
                public GetNodeInfoCommand() { }

                public GetNodeInfoCommand(TimeSpan timeout)
                {
                    Timeout = timeout;
                }

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/cluster/node-info";

                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Get
                    };
                }

                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
                {
                    if (response == null)
                        return;

                    Result = JsonDeserializationClient.NodeInfo(response);
                }

                public override bool IsReadRequest => true;
            }
        }
    }
}
