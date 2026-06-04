using FastTests;
using Raven.AiAppliance.Channels;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// RavenDB-26700 (auth follow-up) — the conversation-binding service that
/// hides real <c>chats/</c> ids behind opaque client-held tokens (A2 fix).
/// Service-level coverage: persistence shape, resolve roundtrip, the 401
/// error codes, and the cluster-tx conflict branch that random iFrame keys
/// never hit (exercised here with a deterministic key, mirroring the C2
/// provision race test).
/// </summary>
public class ConversationBindingsTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private static readonly TimeSpan Ttl = TimeSpan.FromHours(24);

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task GetOrCreate_persists_binding_that_hides_the_conversation_id()
    {
        using var store = GetDocumentStore();
        var bindings = new ConversationBindings(store, TimeProvider.System);
        var token = RandomIds.NewId("cnv_");
        var bindingId = ConversationBinding.MakeId("wgt_test", token);

        var before = DateTime.UtcNow;
        var conversationId = await bindings.GetOrCreateAsync(
            store.Database, bindingId, widgetId: "wgt_test", () => "chats/hidden-1", Ttl, CancellationToken.None);

        Assert.Equal("chats/hidden-1", conversationId);

        using var session = store.OpenAsyncSession();
        var doc = await session.LoadAsync<ConversationBinding>(bindingId);
        Assert.NotNull(doc);
        Assert.Equal("chats/hidden-1", doc.ConversationId);
        Assert.Equal("wgt_test", doc.WidgetId);
        Assert.InRange(doc.ExpiresAt, before + Ttl, DateTime.UtcNow + Ttl);
        Assert.InRange(doc.CreatedAt, before, DateTime.UtcNow);

        // Session retention is deferred (user decision) — the binding must
        // NOT carry @expires; validity is enforced by the read-time check.
        var metadata = session.Advanced.GetMetadataFor(doc);
        Assert.False(metadata.ContainsKey("@expires"));
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Resolve_returns_the_hidden_id_for_a_live_binding()
    {
        using var store = GetDocumentStore();
        var bindings = new ConversationBindings(store, TimeProvider.System);
        var bindingId = ConversationBinding.MakeId("wgt_test", RandomIds.NewId("cnv_"));
        await bindings.GetOrCreateAsync(
            store.Database, bindingId, widgetId: "wgt_test", () => "chats/hidden-2", Ttl, CancellationToken.None);

        var (conversationId, errorCode) = await bindings.TryResolveAsync(store.Database, bindingId, CancellationToken.None);

        Assert.Null(errorCode);
        Assert.Equal("chats/hidden-2", conversationId);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Resolve_unknown_binding_returns_conversation_unknown()
    {
        using var store = GetDocumentStore();
        var bindings = new ConversationBindings(store, TimeProvider.System);

        var (conversationId, errorCode) = await bindings.TryResolveAsync(
            store.Database, ConversationBinding.MakeId("wgt_test", RandomIds.NewId("cnv_")), CancellationToken.None);

        Assert.Null(conversationId);
        Assert.Equal("conversation_unknown", errorCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Resolve_expired_binding_returns_conversation_expired()
    {
        using var store = GetDocumentStore();
        var bindings = new ConversationBindings(store, TimeProvider.System);
        var bindingId = ConversationBinding.MakeId("wgt_test", RandomIds.NewId("cnv_"));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new ConversationBinding
            {
                Id = bindingId,
                ConversationId = "chats/stale",
                WidgetId = "wgt_test",
                CreatedAt = DateTime.UtcNow.AddDays(-2),
                ExpiresAt = DateTime.UtcNow.AddDays(-1),
            });
            await session.SaveChangesAsync();
        }

        var (conversationId, errorCode) = await bindings.TryResolveAsync(store.Database, bindingId, CancellationToken.None);

        Assert.Null(conversationId);
        Assert.Equal("conversation_expired", errorCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task GetOrCreate_converges_concurrent_writers_on_one_binding()
    {
        using var store = GetDocumentStore();
        var bindings = new ConversationBindings(store, TimeProvider.System);

        // Deterministic key — the shape a future deterministic-key consumer
        // (platform identity) would use. Random iFrame keys can't collide, so
        // this is the only path that exercises the conflict -> read-winner branch.
        var bindingId = ConversationBinding.MakeId("wgt_test", "user-42");

        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
            bindings.GetOrCreateAsync(
                store.Database, bindingId, widgetId: "wgt_test", () => RandomIds.NewId("chats/"), Ttl, CancellationToken.None))));

        Assert.Single(results.Distinct(StringComparer.Ordinal));

        using var session = store.OpenAsyncSession();
        var docs = await session.Advanced.LoadStartingWithAsync<ConversationBinding>(ConversationBinding.IdPrefix);
        Assert.Single(docs);
        Assert.Equal(results[0], docs.Single().ConversationId);
    }
}
