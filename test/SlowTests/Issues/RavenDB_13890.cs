using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Http;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13890 : RavenTestBase
    {
        public RavenDB_13890(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CreateRequest()
        {
            using (var store = GetDocumentStore())
            {
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var dummy = new ServerNode
                    {
                        ClusterTag = "A",
                        Database = "dummy",
                        ServerRole = ServerNode.Role.Member,
                        Url = "http://dummy:1234"
                    };

                    var batchCommand = new SingleNodeBatchCommand(store.Conventions, context,new List<ICommandData>());
                    var uri = requestExecutor.CreateRequest(context, dummy, batchCommand, out _);
                    Assert.DoesNotContain("raft", uri.RequestUri.ToString());

                    var clusterBatchCommand = new ClusterWideBatchCommand(store.Conventions, new List<ICommandData>());
                    uri = requestExecutor.CreateRequest(context, dummy, clusterBatchCommand, out _);
                    Assert.Contains("raft", uri.RequestUri.ToString());
                }
            }
        }
    }
}
