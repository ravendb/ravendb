using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Queries
{
    public class DynamicQueriesOnMetadata : RavenTestBase
    {
        public DynamicQueriesOnMetadata(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanQueryOnMetadataUsingDynamicQueries(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    var g = new Glass();
                    s.Store(g);
                    s.Advanced.GetMetadataFor(g)["Is-Nice"] = true;
                    s.SaveChanges();
                }


                using (var s = store.OpenSession())
                {
                    var glasses = s.Advanced.DocumentQuery<Glass>()
                        .WhereEquals("@metadata.'Is-Nice'", true)
                        .ToArray();
                    Assert.NotEmpty(glasses);
                }
            }
        }

        private class Glass
        {
            public string Id { get; set; }
        }
    }
}
