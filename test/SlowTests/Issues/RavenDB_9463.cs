using System.Linq;
using FastTests;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9463 : RavenTestBase
    {
        public RavenDB_9463(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() => session.Advanced
                        .RawQuery<dynamic>("from index 'games' select  facet(playerId, sum(goals), sum(errors), sum(assists))")
                        .ToList());

                    Assert.Contains("Detected duplicate facet aggregation", e.Message);
                }
            }
        }
    }
}
