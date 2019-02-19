using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10475 : RavenTestBase
    {
        [Fact]
        public void CanQueryNegativeNumbers()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Advanced.RawQuery<dynamic>("from Users where Age > -1 and Size > -12.4")
                        .ToList();
                }
            }
        }
    }
}
