using System;
using System.Threading.Tasks;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests
{
    public class InfrastructureTests : ClusterTestBase
    {
        public InfrastructureTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanPropagateException()
        {
            var ae = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                var cluster = await CreateRaftCluster(2, leaderIndex: 0);
                using (var store = GetDocumentStore(new Options{Server = cluster.Leader}))
                {
                    cluster.Nodes[1].Dispose();
                    throw new InvalidOperationException(); // this is the real exception
                }
            });
        }
    }
}
