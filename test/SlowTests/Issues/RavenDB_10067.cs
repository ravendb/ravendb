using System.Linq;
using FastTests;
using Orders;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10067 : RavenTestBase
    {
        public RavenDB_10067(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void NodeTagWillNotBeEmpty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Query<Company>()
                        .Statistics(out var stats)
                        .ToList();

                    Assert.NotNull(stats.NodeTag);

                    session.Query<Company>()
                        .Statistics(out stats)
                        .Where(x => x.Name == "HR")
                        .ToList();

                    Assert.NotNull(stats.NodeTag);
                }
            }
        }
    }
}
