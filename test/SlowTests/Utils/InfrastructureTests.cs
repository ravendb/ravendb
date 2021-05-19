using System;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Utils
{
    public class InfrastructureTests : ClusterTestBase
    {
        public InfrastructureTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanPropagateException()
        {
            var ioe = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                var cluster = await CreateRaftCluster(2, leaderIndex: 0);
                using (var store = GetDocumentStore(new Options { Server = cluster.Leader }))
                {
                    cluster.Nodes[1].Dispose();
                    throw new InvalidOperationException("Cows can fly!"); // this is the real exception
                }
            });

            Assert.Equal("Cows can fly!", ioe.Message);
        }

        [Fact]
        public async Task CanCatchException()
        {
            DoNotReuseServer();

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
            {
                using (var store = GetDocumentStore())
                {
                    throw new InvalidOperationException("Cows can fly!"); // this is the real exception
                }
            });
        }
    }
}
