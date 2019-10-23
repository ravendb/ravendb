using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Queries
{
    public class QueryingOnValueWithMinus : RavenTestBase
    {
        public QueryingOnValueWithMinus(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanQueryOnValuesContainingMinus()
        {
            using (var store = GetDocumentStore())
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
