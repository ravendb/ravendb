using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static SlowTests.Issues.RavenDB_14986;

namespace StressTests.Issues
{
    public class RavenDB_14986_Stress : RavenTestBase
    {
        public RavenDB_14986_Stress(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
        public void CanSetFieldStorageNoAndFieldIndexingNoInMapReduceCorax(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                CanSetFieldStorageNoAndFieldIndexingNoInMapReduce(store, Indexes, simpleMapReduceErrors =>
                   {
                       Assert.Equal(25, simpleMapReduceErrors.Errors.Length);
                       Assert.True(simpleMapReduceErrors.Errors.All(x => x.Error.Contains("that is neither indexed nor stored is useless because it cannot be searched or retrieved.")));
                   });
            }
        }
    }
}
