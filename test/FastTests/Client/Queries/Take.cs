using System.Linq;
using System.Threading.Tasks;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Exceptions;
using Raven.NewClient.Operations.Databases.ApiKeys;
using Xunit;

namespace FastTests.Client.Queries
{
    public class Take : RavenNewTestBase
    {
        [Fact]
        public async Task ExplictTakeWhichIsGreaterThanMaxPageSizeShouldThrow()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenAsyncSession())
            {
                var exception = await Assert.ThrowsAsync<RavenException>(async () =>
                {
                    await session.Query<Item>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Take(2048)
                        .ToListAsync();
                });
                Assert.Contains("Your page size (2048) is more than the max page size which is 1024.", exception.Message);
                Assert.Contains("session.Advanced.Stream(query)", exception.Message);

                exception = await Assert.ThrowsAsync<RavenException>(async () =>
                {
                    await store.Admin.SendAsync(new GetApiKeysOperation(0, 2048));
                });
                Assert.Contains("Your page size (2048) is more than the max page size which is 1024.", exception.Message);
                Assert.DoesNotContain("Stream", exception.Message);
            }
        }

        private class Item
        {
            public string Id { get; set; }

            public int Index { get; set; }
        }
    }
}