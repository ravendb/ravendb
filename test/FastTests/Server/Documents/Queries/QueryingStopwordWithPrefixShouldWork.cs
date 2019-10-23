using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Queries
{
    public class QueryingStopwordWithPrefixShouldWork : RavenTestBase
    {
        public QueryingStopwordWithPrefixShouldWork(ITestOutputHelper output) : base(output)
        {
        }

        private class Foo
        {
            public string Bar { get; set; }
        }

        private class FooByBar : AbstractIndexCreationTask<Foo>
        {
            public FooByBar()
            {
                Map = foos => from foo in foos select new { foo.Bar };
                Index(x => x.Bar, FieldIndexing.Search);
            }
        }

        [Fact]
        public void CanQueryStopwordsWithPrefix()
        {
            using (var store = GetDocumentStore())
            {
                var index = new FooByBar();
                index.Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo { Bar = "Andrew" });
                    session.Store(new Foo { Bar = "boo" });
                    session.SaveChanges();
                    Assert.Single(session.Query<Foo>("FooByBar").Search(x => x.Bar, "And*").Customize(x => x.WaitForNonStaleResults()).ToList());
                }
            }
        }
    }
}
