using System.Linq;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Queries
{
    public class InQuery : RavenTestBase
    {
        public InQuery(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void QueryingUsingInShouldYieldDistinctResults()
        {
            using (var store = GetDocumentStore())
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
