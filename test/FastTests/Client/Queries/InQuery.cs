using System.Linq;
using FastTests.Server.Documents.Indexing;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace FastTests.Client.Queries
{
    public class InQuery : RavenTestBase
    {
        public InQuery(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void QueryingUsingInShouldYieldDistinctResults(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo{Name = "Bar"},"Foos/1");
                    session.SaveChanges();
                    session.Query<Foo>().Single(foo => foo.Id.In("Foos/1", "Foos/1", "Foos/1", "Foos/1"));
                }

            }
        }


        private class Foo
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

    }
}
