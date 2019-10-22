using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11452 : RavenTestBase
    {
        public RavenDB_11452(ITestOutputHelper output) : base(output)
        {
        }

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
