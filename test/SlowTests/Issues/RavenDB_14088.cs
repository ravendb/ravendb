using FastTests;
using Orders;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14088 : RavenTestBase
    {
        public RavenDB_14088(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void NotWithExistsShouldYieldProperRql()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .DocumentQuery<Company>()
                        .Not
                        .WhereExists("SomeField")
                        .ToString();

                    Assert.Equal("from 'Companies' where true and not exists(SomeField)", query);
                }
            }
        }
    }
}
