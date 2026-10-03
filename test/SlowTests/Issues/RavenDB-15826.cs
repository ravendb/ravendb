using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15826 : RavenTestBase
    {
        public RavenDB_15826(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public string[] Refs;
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public async Task CanIncludeLazyLoadITemThatIsAlreadyOnSession()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item(), "items/a");
                await session.StoreAsync(new Item(), "items/b");
                await session.StoreAsync(new Item { Refs = new[] { "items/a", "items/b" } }, "items/c");
                await session.StoreAsync(new Item { Refs = new[] { "items/a", } }, "items/d");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.Include<Item>(x => x.Refs).LoadAsync("items/d"); // include, some loaded: Load DocIds [items/d], Includes[items/a]
                var a = await session.LoadAsync<Item>("items/c"); // include, some loaded // DocsIds: [items/c, items/d], Includes[items/a]
                var items = session.Advanced.Lazily.LoadAsync<Item>(a.Refs); // Load [items/a, items/b, items/c, items/d] Includes[items/a]
                await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();
                Dictionary<string, Item> itemsDic = await items.Value; //
                Assert.Equal(a.Refs.Length, itemsDic.Count);
                Assert.DoesNotContain(itemsDic, x => x.Value == null);
            }
        }
        
        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public async Task CanIncludeLoadItemThatIsAlreadyOnSession()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item(), "items/a");
                await session.StoreAsync(new Item(), "items/b");
                await session.StoreAsync(new Item { Refs = new[] { "items/a", "items/b" } }, "items/c");
                await session.StoreAsync(new Item { Refs = new[] { "items/a", } }, "items/d");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.Include<Item>(x => x.Refs).LoadAsync("items/d"); // include, some loaded: Load DocIds [items/d], Includes[items/a]
                var a = await session.LoadAsync<Item>("items/c"); // include, some loaded // DocsIds: [items/c, items/d], Includes[items/a]
                var items = session.LoadAsync<Item>(a.Refs); // Load [items/a, items/b, items/c, items/d] Includes[items/a]
                Dictionary<string, Item> itemsDic = await items;
                Assert.Equal(a.Refs.Length, itemsDic.Count);
                Assert.DoesNotContain(itemsDic, x => x.Value == null);
            }
        }
    }
}
