using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Exceptions;
using Raven.NewClient.Operations.Databases.ApiKeys;
using Tests.Infrastructure;
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

        [Fact]
        public async Task ImplicitTakeWillBeConfigurableAndByDefault25()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 1; i <= 30; i++)
                {
                    await session.StoreAsync(new Item { Index = i });
                }
                await session.SaveChangesAsync();

                store.Conventions.ThrowIfImplicitTakeAmountExceeded = false;
                var items = await session.Query<Item>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .ToListAsync();
                Assert.Equal(25, items.Count);

                store.Conventions.ImplicitTakeAmount = 15;
                items = await session.Query<Item>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .ToListAsync();
                Assert.Equal(15, items.Count);

                store.Conventions.ImplicitTakeAmount = 29;
                items = await session.Query<Item>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .ToListAsync();
                Assert.Equal(29, items.Count);

                items = await session.Query<Item>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Take(28)
                        .ToListAsync();
                Assert.Equal(28, items.Count);
            }
        }

        [Fact]
        public async Task ThrowIfThereAreMoreResultsThanImplicitTake()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 1; i <= 30; i++)
                {
                    await session.StoreAsync(new Item {Index = i});
                }
                await session.SaveChangesAsync();

                var exception = await Assert.ThrowsAsync<RavenException>(async () =>
                {
                    await session.Query<Item>()
                    .Customize(customization => customization.WaitForNonStaleResults())
                    .ToListAsync();
                });
                Assert.Contains("The query has more results (30) than the implicity take ammount which is .Take(25).", exception.Message);
            }
        }

        private class Item
        {
            public string Id { get; set; }

            public int Index { get; set; }
        }
    }
}