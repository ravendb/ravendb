using FastTests;
using Xunit;
using System.Linq;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Indexing
{
    public class DynamicQueriesCanSort : RavenTestBase
    {
        public DynamicQueriesCanSort(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanSortOnDynamicIndexOnFieldWhichWeDontQuery(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        session.Store(new { Value = i });
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var array = session.Advanced.DocumentQuery<dynamic>()
                        .AddOrder("Value", true)
                        .ToArray();

                    Assert.Equal(4, (int)array[0].Value);
                    Assert.Equal(3, (int)array[1].Value);
                    Assert.Equal(2, (int)array[2].Value);
                    Assert.Equal(1, (int)array[3].Value);
                    Assert.Equal(0, (int)array[4].Value);
                }
            }
        }
    }
}
