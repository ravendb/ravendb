using FastTests;
using FastTests.Server.Documents.Indexing.MapReduce;
using Xunit;

namespace SlowTests.Server.Documents.Indexing.MapReduce
{
    public class RavenDB_5383_Slow : NoDisposalNeeded
    {
        [Theory]
        [InlineData(1000)]
        public void When_map_results_do_not_change_then_we_skip_the_reduce_phase(int numberOfDocs)
        {
            using (var a = new RavenDB_5383())
            {
                a.When_map_results_do_not_change_then_we_skip_the_reduce_phase(numberOfDocs);
            }
        }
    }
}