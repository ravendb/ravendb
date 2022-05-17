using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Queries
{
    public class QueryingOnValueWithMinus : RavenTestBase
    {
        public QueryingOnValueWithMinus(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanQueryOnValuesContainingMinus(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Bruce-Lee" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var list = session.Advanced.DocumentQuery<dynamic>()
                        .WhereEquals("Name", "Bruce-Lee")
                        .ToList();

                    Assert.Equal(1, list.Count);
                }
            }
        }
    }
}
