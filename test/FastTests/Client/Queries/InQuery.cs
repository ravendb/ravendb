using System.Linq;
using FastTests.Server.Documents.Indexing;
using Raven.Client.Documents.Linq;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Queries
{
    public class InQuery : RavenTestBase
    {
        public InQuery(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [MemberData(nameof(SearchEngineTypeValue.Data), MemberType= typeof(SearchEngineTypeValue))]
        public void QueryingUsingInShouldYieldDistinctResults(string searchEngineType)
        {
            using (var store = GetDocumentStore(new Options(){ModifyDatabaseRecord = d => d.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = searchEngineType}))
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
