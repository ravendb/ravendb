using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using SlowTests.Server.Replication;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues
{
    public class RavenDB_5763 : NoDisposalNoOutputNeeded
    {
        public RavenDB_5763(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Should_not_throw_timeout_and_out_of_memory()
        {
            await Parallel.ForEachAsync(Enumerable.Range(0, 3), RavenTestHelper.DefaultParallelOptions, async (_, __) =>
            {
                using (var store = new ReplicationTombstoneTests(Output))
                {
                    await store.Two_tombstones_should_replicate_in_master_master(RavenTestBase.Options.ForMode(RavenDatabaseMode.Single)).WaitAsync(TimeSpan.FromMinutes(10));
                }
            });
        }
    }
}
