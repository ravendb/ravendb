using System.Net.Http.Json;
using System.Text.Json;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for <c>GET /api/apps/{slug}/collections</c> — backs the prototype's
/// <c>api.listCollections(appId)</c>. Phase-1 scope (egor): just the business
/// collections with current document counts; <c>fields[]</c> ships empty
/// (RavenDB is schemaless — field stats are a later enhancement). System
/// collections (<c>@conversations</c>, <c>@hilo</c>, …) are excluded.
/// </summary>
public class CollectionsEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    private sealed class Product
    {
        public string Name { get; set; } = "";
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Collections_returns_business_collections_with_document_counts()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using (var session = store.OpenAsyncSession(perAppDb))
        {
            await session.StoreAsync(new Product { Name = "Widget" }, "products/1");
            await session.StoreAsync(new Product { Name = "Gadget" }, "products/2");
            await session.SaveChangesAsync();
        }
        // A system collection that must be filtered out of the result.
        await SeedConversationAsync(store, perAppDb, "chats/a", "demo", DateTime.UtcNow);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/my-app/collections");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var collections = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(JsonValueKind.Array, collections.ValueKind);

        var byName = collections.EnumerateArray()
            .ToDictionary(c => c.GetProperty("name").GetString()!, c => c);

        Assert.True(byName.ContainsKey("Products"), "expected the Products collection");
        Assert.Equal(2, byName["Products"].GetProperty("documentsCount").GetInt64());
        Assert.Equal(0, byName["Products"].GetProperty("fields").GetArrayLength());
        Assert.Equal("my-app", byName["Products"].GetProperty("appId").GetString());

        // System collections (@-prefixed) are excluded.
        Assert.DoesNotContain(byName.Keys, k => k.StartsWith('@'));
    }
}
