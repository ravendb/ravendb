using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9276 : RavenTestBase
    {
        public RavenDB_9276(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Querying)]
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
