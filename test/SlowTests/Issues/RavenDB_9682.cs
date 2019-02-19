using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9682 : RavenTestBase
    {
        [Fact]
        public void IdInInQueriesShouldNotBeCaseSensitive()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                using (var s = store.OpenSession())
                {
                    var count = s.Advanced.RawQuery<dynamic>("FROM @all_docs WHERE ID() = 'orders/1-A'")
                        .Count();
                    Assert.Equal(1, count);
                }
            }
        }
    }
}
