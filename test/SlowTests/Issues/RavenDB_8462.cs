using System.Linq;
using FastTests;
using Orders;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8462 : RavenTestBase
    {
        public RavenDB_8462(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void Should_be_handled_by_the_same_index(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "big box"
                    });

                    session.SaveChanges();

                    var results = session.Advanced.DocumentQuery<Company>()
                        .Statistics(out var stats)
                        .WhereLucene("Name", "big box")
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.Equal("Auto/Companies/BySearch(Name)", stats.IndexName);
                    Assert.Equal(1, results.Count);


                    results = session.Advanced.DocumentQuery<Company>()
                        .Statistics(out stats)
                        .Search("Name", "big box")
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.Equal("Auto/Companies/BySearch(Name)", stats.IndexName);
                    Assert.Equal(1, results.Count);
                }
            }
        }
    }
}
