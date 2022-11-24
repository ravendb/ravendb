using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Corax.Bugs;

public class GrowableBuffersTests : RavenTestBase
{
    public GrowableBuffersTests(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void GrowableBufferInBoostingMatchWillCreateBufferWithProperSize(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < 16 * 1024; ++i)
                {
                    bulkInsert.Store(new Item() {Name = $"Name{i}", Number = i});
                }
            }

            {
                using var session = store.OpenSession();
                var list = session.Advanced.RawQuery<Item>("from Items where boost(Number > -1, 1.0) and startsWith(Name, 'n')").NoTracking().NoCaching();
                Indexes.WaitForIndexing(store);
            }
            
            Parallel.ForEach(Enumerable.Range(0, 128), RavenTestHelper.DefaultParallelOptions, i =>
            {
                using var _ = store.SetRequestTimeout(TimeSpan.FromMinutes(2));
                using var session = store.OpenSession();
                var list = session.Advanced.RawQuery<Item>("from Items where boost(Number > -1, 1.0) and startsWith(Name, 'n')").NoCaching().NoTracking().ToList();
                Assert.NotEmpty(list);
            });
        }
    }
    
    private class Item
    {
        public string Name { get; set; }
        public long Number { get; set; }
    }
    
}
