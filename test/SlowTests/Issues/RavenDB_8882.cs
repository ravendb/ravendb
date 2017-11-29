using System.Linq;
using FastTests;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8882 : RavenTestBase
    {
        [Fact]
        public void DuplicateSuggestShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced
                        .RawQuery<dynamic>("FROM Orders SELECT suggest(Name, 'John'), suggest(Company, 'HR')")
                        .ToList());

                    Assert.Contains("Suggestion query can only contain one suggest() in SELECT", e.Message);
                }
            }
        }
    }
}
