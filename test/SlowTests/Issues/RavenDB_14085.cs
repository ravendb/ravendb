using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14085 : RavenTestBase
    {
        public RavenDB_14085(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void NegativeSkipAndTakeInQueryShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidQueryException>(() =>
                    {
                        session
                        .Query<Company>()
                        .Skip(-10)
                        .Take(-5)
                        .ToList();
                    });

                    Assert.Contains("cannot be negative", e.Message);

                    e = Assert.Throws<InvalidQueryException>(() =>
                    {
                        session.Advanced
                        .DocumentQuery<Company>()
                        .Skip(-10)
                        .Take(-5)
                        .ToList();
                    });

                    Assert.Contains("cannot be negative", e.Message);
                }
            }
        }
    }
}
