using QuillTests.E2E.Fixtures;
using Tests.Infrastructure;
using Xunit;
using static QuillTests.E2E.Fixtures.ConversationSeed;

namespace QuillTests;

public class CollectionsEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
{
    private sealed class Product
    {
        public string Name { get; set; } = "";
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Collections_returns_business_collections_with_document_counts()
    {
        await using var app = await NewAppAsync();

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            await session.StoreAsync(new Product { Name = "Widget" }, "products/1");
            await session.StoreAsync(new Product { Name = "Gadget" }, "products/2");
            await session.SaveChangesAsync();
        }
        // @conversations = system collection, must be filtered out
        await SeedConversationAsync(app.Store, app.Slug, "chats/a", "demo", DateTime.UtcNow);

        var collections = await app.GetCollectionsAsync();

        var products = collections.Single(c => c.Name == "Products");
        Assert.Equal(2, products.DocumentsCount);
        Assert.Empty(products.Fields);
        Assert.Equal(app.Slug, products.AppId);

        Assert.DoesNotContain(collections, c => c.Name.StartsWith('@'));
    }
}
