using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Bugs.Async
{
    public class Querying : RavenNewTestBase
    {
        [Fact]
        public async Task Can_query_using_async_session()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenAsyncSession())
                {
                    await s.StoreAsync(new { Name = "Ayende" });
                    await s.SaveChangesAsync();
                }

                using (var s = store.OpenAsyncSession())
                {
                    var queryResultAsync = await s.Advanced.AsyncDocumentQuery<dynamic>()
                        .WhereEquals("Name", "Ayende")
                        .ToListAsync();

                    var result = queryResultAsync;
                    Assert.Equal("Ayende", (string)result[0].Name);
                }
            }
        }
    }
}
