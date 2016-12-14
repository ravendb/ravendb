using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5763 : RavenTestBase
    {
        [Fact]
        public void Should_not_throw_timeout_and_out_of_memory()
        {
            Parallel.For(0, 50, RavenTestHelper.DefaultParallelOptions, i =>
            {
                using (var store = new FastTests.Server.Documents.Replication.ReplicationTombstoneTests())
                {
                    store.Two_tombstones_should_replicate_in_master_master().Wait();
                }
            });
        }
    }
}
