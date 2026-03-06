using System;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Utils
{
    public class InfrastructureTests : ClusterTestBase
    {
        public InfrastructureTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public async Task CanPropagateException()
        {
            var ioe = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                var cluster = await CreateRaftCluster(2, leaderIndex: 0);
                using (var store = GetDocumentStore(new Options { Server = cluster.Leader }))
                {
                    try
                    {
                        cluster.Nodes[1].Dispose();
                    }
                    catch
                    {
                       // we don't care if it throws here,
                       // the important thing is to test the exception below will be thrown before the expected exception in the dispose of the document store
                    }

                    throw new InvalidOperationException("Cows can fly!"); // this is the real exception
                }
            });

            Assert.Equal("Cows can fly!", ioe.Message);
        }

        [RavenFact(RavenTestCategory.Core)]
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

