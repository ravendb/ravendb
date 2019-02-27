using System.Threading.Tasks;
using FastTests;
using SlowTests.Server.Replication;
using Xunit;

namespace StressTests.Issues
{
    public class RavenDB_5763 : NoDisposalNeeded
    {
        [Fact]
        public void Should_not_throw_timeout_and_out_of_memory()
        {
            Parallel.For(0, 3, RavenTestHelper.DefaultParallelOptions, i =>
            {
                using (var store = new ReplicationTombstoneTests())
                {
                    store.Two_tombstones_should_replicate_in_master_master().Wait();
                }
            });
        }
    }
}
