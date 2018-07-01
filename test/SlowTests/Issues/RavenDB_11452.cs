using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11452 : RavenTestBase
    {
        [Fact]
        public async Task ShouldNotThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.LoadAsync<object>((string)null);
                }
            }
        }
    }
}
