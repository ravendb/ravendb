using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21525 : RavenTestBase
{
    public RavenDB_21525(ITestOutputHelper output) : base(output)
    {
    }


    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task OrderByWithPaginationVsUnordered(Options options, int size = 10000)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < size; ++i)
                {
                    bulkInsert.Store(new Person() { Name = $"ItemNo{i}", Age = i % 100, Height = i % 200 });
                }
            }

            for (int i = 0; i < size; i += 70)
            {
                // sync API
                using (var session = store.OpenSession())
                {
                    var resultsOrdered = session.Query<Person>()
                        .OrderBy(y => y.Age)
                        .Statistics(out var orderedStats)
                        .Skip(i).Take(70)
                        .Customize(i => i.WaitForNonStaleResults().NoCaching())
                        .ToList();

                    var resultsUnordered = session.Query<Person>()
                        .Statistics(out var unorderedStats)
                        .Skip(i).Take(70)
                        .Customize(i => i.WaitForNonStaleResults().NoCaching())
                        .ToList();

                    Assert.Equal(resultsOrdered.Count, resultsUnordered.Count);

                    long totalOrdered = orderedStats.TotalResults;
                    long totalUnordered = unorderedStats.TotalResults;
                    Assert.Equal(totalUnordered, totalOrdered);
                }

                // async API
                using (var session = store.OpenAsyncSession())
                {
                    var resultsOrdered = await session.Query<Person>()
                        .OrderBy(y => y.Age)
                        .Statistics(out var orderedStats)
                        .Skip(i).Take(70)
                        .Customize(i => i.WaitForNonStaleResults().NoCaching())
                        .ToListAsync();

                    var resultsUnordered = await session.Query<Person>()
                        .Statistics(out var unorderedStats)
                        .Skip(i).Take(70)
                        .Customize(i => i.WaitForNonStaleResults().NoCaching())
                        .ToListAsync();

                    Assert.Equal(resultsOrdered.Count, resultsUnordered.Count);

                    long totalOrdered = orderedStats.TotalResults;
                    long totalUnordered = unorderedStats.TotalResults;
                    Assert.Equal(totalUnordered, totalOrdered);
                }

            }
        }
    }

    private class Person
    {
        public string Name { get; set; }
        public int Age { get; set; }
        public int Height { get; set; }
    }
}
