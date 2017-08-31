using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8382 : RavenTestBase
    {
        [Fact]
        public void ShouldThrowOnAttemptToGroupByCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var ex = Assert.Throws<InvalidQueryException>(() => session.Advanced.DocumentQuery<Order>().RawQuery("from Orders group by Lines[].ProductName").ToList());

                    Assert.Contains("Grouping by collections in auto map reduce indexes is not supported", ex.Message);
                }
            }
        }
    }
}
