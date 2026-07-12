using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.AiAppliance.Channels;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// Coverage for chat-history rendering on the public embed page
/// (<c>GET /embed/{token}</c>, RavenDB-26916): a returning visitor sees the prior
/// turns of the link's conversation, and a fresh link renders an empty feed.
/// </summary>
public class EmbedPageHistoryTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Embed_page_renders_prior_conversation_history()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/embed", "demo", now.AddMinutes(-5),
            turns: [("user", "hello there"), ("assistant", "hi, how can I help?")]);
        await SeedChannelAsync(store, perAppDb, channelId: "wgt1", enabled: true);

        var token = Guid.NewGuid().ToString("N");
        await SeedEmbedLinkAsync(store, perAppDb, token, widgetId: "wgt1", conversationId: "chats/embed", now: now);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var html = await client.GetStringAsync($"/embed/{token}");

        // The placeholder is replaced with a script-safe JSON array carrying the prior turns,
        // rendered into the feed on load.
        Assert.DoesNotContain("__HISTORY__", html);
        Assert.Contains("for (const turn of [{", html);
        Assert.Contains("hello there", html);
        Assert.Contains("hi, how can I help?", html);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Embed_page_for_a_fresh_link_renders_an_empty_feed()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await SeedChannelAsync(store, perAppDb, channelId: "wgt1", enabled: true);

        var token = Guid.NewGuid().ToString("N");
        // No conversation bound yet (fresh link) → empty history array.
        await SeedEmbedLinkAsync(store, perAppDb, token, widgetId: "wgt1", conversationId: null, now: DateTime.UtcNow);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var html = await client.GetStringAsync($"/embed/{token}");

        Assert.DoesNotContain("__HISTORY__", html);
        Assert.Contains("for (const turn of [])", html);
    }

    private static async Task SeedEmbedLinkAsync(
        IDocumentStore store, string database, string token, string widgetId, string? conversationId, DateTime now)
    {
        using (var appSession = store.OpenAsyncSession(database))
        {
            await appSession.StoreAsync(new EmbedLink
            {
                WidgetId = widgetId,
                AgentId = "demo",
                ExpiresAt = now.AddHours(1),
                MaxInvocations = 5,
                ConversationId = conversationId,
                CreatedAt = now,
            }, $"{EmbedLink.IdPrefix}{token}");
            await appSession.SaveChangesAsync();
        }

        using (var cfgSession = store.OpenAsyncSession())
        {
            await cfgSession.StoreAsync(new LinkIndex { Slug = "my-app" }, $"{LinkIndex.IdPrefix}{token}");
            await cfgSession.SaveChangesAsync();
        }
    }
}
