using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9276 : RavenTestBase
    {
        [Fact]
        public void Can_group_by_constant()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.RawQuery<dynamic>("from Orders group by 1 select count()").ToList();
                }
            }
        }
    }
}
