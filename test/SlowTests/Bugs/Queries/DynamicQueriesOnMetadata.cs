using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Queries
{
    public class DynamicQueriesOnMetadata : RavenTestBase
    {
        public DynamicQueriesOnMetadata(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanQueryOnMetadataUsingDynamicQueries()
        {
            using (var store = GetDocumentStore())
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
