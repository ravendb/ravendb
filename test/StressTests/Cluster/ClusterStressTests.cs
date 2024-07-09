using System.Threading.Tasks;
using FastTests;
using SlowTests.Cluster;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace StressTests.Cluster
{
    public class ClusterStressTests : NoDisposalNeeded
    {
        public ClusterStressTests(ITestOutputHelper output) : base(output)
        {
        }

        [MultiplatformFact(RavenArchitecture.AllX64, Skip = "RavenDB-22199")]
        public async Task ParallelClusterTransactions()
        {
            using (var test = new ParallelClusterTransactionsTests(Output))
            {
                await test.ParallelClusterTransactions(7);
            }
        }
    }
}
