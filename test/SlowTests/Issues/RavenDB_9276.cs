using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9276 : RavenTestBase
    {
        public RavenDB_9276(ITestOutputHelper output) : base(output)
        {
        }

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
