using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Queries
{
    public class QueryByTypeOnly : RavenTestBase
    {
        public QueryByTypeOnly(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void QueryOnlyByType(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Query<Product>()
                        .Skip(5)
                        .Take(5)
                        .ToList();
                }
            }
        }

        private class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public decimal Cost { get; set; }
        }
    }


}
