using System.Threading.Tasks;
using FastTests;
using Orders;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14109 : RavenTestBase
    {
        public RavenDB_14109(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public async Task QueryStatsShouldBeFilledBeforeCallingMoveNext()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company());
                    session.Store(new Company());

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Company>();

                    var enumerator = session
                        .Advanced
                        .Stream(query, out var stats);

                    Assert.Equal(2, stats.TotalResults);

                    var count = 0;
                    while (enumerator.MoveNext())
                        count++;

                    Assert.Equal(stats.TotalResults, count);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<Company>();

                    var enumerator = await session
                        .Advanced
                        .StreamAsync(query, out var stats);

                    Assert.Equal(2, stats.TotalResults);

                    var count = 0;
                    while (await enumerator.MoveNextAsync())
                        count++;

                    Assert.Equal(stats.TotalResults, count);
                }
            }
        }
    }
}
