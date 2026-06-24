using System.Net.Http.Json;
using System.Text.Json;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// Coverage for <c>GET /api/apps/{slug}/conversations</c> (list) and
/// <c>/conversations/{*id}</c> (detail) — backs the prototype's
/// <c>listConversations</c> / <c>getConversation</c>. Shaped from <c>@conversations</c>
/// docs: agentName, derived agentInitials + state, last-exchange preview, and the
/// full chronological transcript on detail.
/// </summary>
public class ConversationsEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Conversations_list_and_detail_shape_transcript_state_and_agent()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/recent", "order-support", now.AddMinutes(-10),
            turns: [("user", "hello"), ("agent", "hi there")]);
        await SeedConversationAsync(store, perAppDb, "chats/old", "billing", now.AddDays(-3),
            turns: [("user", "where is my refund")]);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // List — newest first, last-exchange preview, derived state/initials.
        var list = await client.GetFromJsonAsync<JsonElement>("/api/apps/my-app/conversations");
        Assert.Equal(2, list.GetArrayLength());

        var first = list[0];
        Assert.Equal("chats/recent", first.GetProperty("id").GetString());
        Assert.Equal("order-support", first.GetProperty("agentName").GetString());
        Assert.Equal("OS", first.GetProperty("agentInitials").GetString());
        Assert.Equal("active", first.GetProperty("state").GetString());          // 10 min ago
        Assert.Equal(2, first.GetProperty("lastExchange").GetArrayLength());
        Assert.Equal("agent", first.GetProperty("lastExchange")[0].GetProperty("role").GetString()); // newest first
        Assert.Equal("", first.GetProperty("channelName").GetString());          // no channel link yet

        Assert.Equal("closed", list[1].GetProperty("state").GetString());        // 3 days ago

        // Detail — full transcript, chronological.
        var detail = await client.GetFromJsonAsync<JsonElement>("/api/apps/my-app/conversations/chats/recent");
        var transcript = detail.GetProperty("transcript");
        Assert.Equal(2, transcript.GetArrayLength());
        Assert.Equal("user", transcript[0].GetProperty("role").GetString());
        Assert.Equal("hello", transcript[0].GetProperty("text").GetString());
    }
}
