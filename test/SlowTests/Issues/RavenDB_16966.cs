using System;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16966 : ReplicationTestBase
    {
        public RavenDB_16966(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task AvoidContextDisposalForRaftBlittableResult()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var size = 100;
            using var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3
            });

            foreach (var server in Servers)
            {
                server.ServerStore.ForTestingPurposesOnly().ModifyCompareExchangeTimeout = (cmd) => cmd.Timeout = TimeSpan.FromMilliseconds(1);
            }

            var tasks = new Task[size];
            for (int i = 0; i < size; i++)
            {
                tasks[i] = store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("test/" + i, "Karmel/" + i, 0));
            }


            for (int i = 0; i < size; i++)
            {
                try
                {
                    await tasks[i];
                }
                catch (RavenException e) when (e.InnerException is TimeoutException)
                {

                }
            }

            for (int i = 0; i < size; i++)
            {
                var res = await AssertWaitForNotNullAsync(() => store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>("test/" + i)));
                Assert.Equal("Karmel/" + i, res.Value);
            }
        }
    }
}
