using System.Text.Json;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;
using static QuillTests.E2E.Fixtures.ConversationSeed;

namespace QuillTests;

public class AppUsageEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task AppUsage_aggregates_conversations_tokens_and_top_capabilities()
    {
        await using var app = await NewAppAsync();

        var now = DateTime.UtcNow;
        await SeedConversationAsync(app.Store, app.Slug, "chats/a", "support", now, messages: 3, tokens: 100_000);
        await SeedConversationAsync(app.Store, app.Slug, "chats/b", "support", now, messages: 4, tokens: 200_000);
        await SeedConversationAsync(app.Store, app.Slug, "chats/c", "sales", now, messages: 2, tokens: 50_000);

        var usage = await app.GetUsageAsync(now.Year, now.Month);

        Assert.Equal(3, usage.Metrics.Conversations.Value);
        Assert.Equal(350_000, usage.Metrics.Tokens.Value);

        var top = usage.TopCapabilities;
        Assert.Equal(2, top.Length);
        Assert.Equal("support", top[0].Name);
        Assert.Equal(300_000, top[0].TotalTokens);
        Assert.Equal(2, top[0].Invocations);
        Assert.Equal("sales", top[1].Name);
        Assert.Equal(50_000, top[1].TotalTokens);

        Assert.Equal(2, usage.TokensByCapability.Keys.Length);

        Assert.Empty(usage.ConversationsByChannel.Keys);
        // seeded agents aren't provisioned → "unknown" model
        var modelKey = Assert.Single(usage.TokensByModel.Keys);
        Assert.Equal("unknown", modelKey.Key);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task AppUsage_fills_model_and_channel_series_from_real_data()
    {
        await using var app = await NewAppAsync();

        var agentId = (await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.", ConnectionStringName = Host.ConnectionStringName,
        })).AgentId;

        var now = DateTime.UtcNow;
        var channel = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, agentId, Array.Empty<string>(), "Support Widget"));

        await SeedConversationAsync(app.Store, app.Slug, "chats/a", agentId, now, tokens: 100_000, channelId: channel.ChannelId);
        await SeedConversationAsync(app.Store, app.Slug, "chats/b", agentId, now, tokens: 200_000, channelId: channel.ChannelId);

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            for (var i = 0; i < 3; i++)
            {
                await session.StoreAsync(new EmbedLink
                {
                    ChannelId = channel.ChannelId,
                    AgentId = agentId,
                    ExpiresAt = now.AddHours(1),
                    MaxInvocations = 5,
                    ConversationId = $"chats/link{i}",
                    CreatedAt = now,
                }, $"{EmbedLink.IdPrefix}{Guid.NewGuid():N}");
            }
            await session.SaveChangesAsync();
        }

        var usage = await app.GetUsageAsync(now.Year, now.Month);

        var modelKey = Assert.Single(usage.TokensByModel.Keys);
        Assert.Equal("llama3.1", modelKey.Key);

        var channelData = usage.ConversationsByChannel;
        var channelKey = Assert.Single(channelData.Keys);
        Assert.Equal(channel.ChannelId, channelKey.Key);
        Assert.Equal("Support Widget", channelKey.Label);
        long channelTotal = 0;
        foreach (var point in channelData.Points)
            channelTotal += ((JsonElement)point[channel.ChannelId]).GetInt64();
        Assert.Equal(2, channelTotal);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task AppUsage_series_labels_use_agent_display_names()
    {
        await using var app = await NewAppAsync();
        var agentId = (await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "customer-support", Name = "Customer Support", SystemPrompt = "You help.", ConnectionStringName = Host.ConnectionStringName,
        })).AgentId;

        var now = DateTime.UtcNow;
        await SeedConversationAsync(app.Store, app.Slug, "chats/a", agentId, now, tokens: 100_000);

        var usage = await app.GetUsageAsync(now.Year, now.Month);

        var capKey = Assert.Single(usage.TokensByCapability.Keys);
        Assert.Equal(agentId, capKey.Key);
        Assert.Equal("Customer Support", capKey.Label);

        Assert.Equal("Customer Support", usage.TopCapabilities[0].Name);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task AppUsage_delta_excludes_previous_window_boundary()
    {
        await using var app = await NewAppAsync();

        var now = DateTime.UtcNow;
        var start = new DateTime(now.Year, now.Month, now.Day, 0, 0, 0, DateTimeKind.Utc);

        await SeedConversationAsync(app.Store, app.Slug, "chats/cur", "support", start.AddHours(Math.Min(now.Hour, 2)), messages: 1, tokens: 10);
        await SeedConversationAsync(app.Store, app.Slug, "chats/boundary", "support", start, messages: 1, tokens: 10);
        await SeedConversationAsync(app.Store, app.Slug, "chats/prev", "support", start.AddHours(-2), messages: 1, tokens: 10);

        var usage = await app.GetUsageAsync(now.Year, now.Month, now.Day);

        var conv = usage.Metrics.Conversations;
        Assert.Equal(2, conv.Value);
        // boundary bucket (==start) counts in current window only → prev=1, delta=100
        Assert.Equal(100.0, conv.Delta);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task AppUsage_uses_hour_buckets_for_the_ByHour_window()
    {
        await using var app = await NewAppAsync();

        var now = DateTime.UtcNow;
        await SeedConversationAsync(app.Store, app.Slug, "chats/a", "support", now, messages: 1, tokens: 1000);

        var usage = await app.GetUsageAsync(now.Year, now.Month, now.Day);

        Assert.Equal(1000, usage.Metrics.Tokens.Value);
        var points = usage.TokensByCapability.Points;
        Assert.Contains("T", ((JsonElement)points[0]["t"]).GetString());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task AppUsage_conversationsByChannel_survives_a_widget_keyed_like_the_time_axis()
    {
        await using var app = await NewAppAsync();

        var now = DateTime.UtcNow;
        // EP mints random guid ids; literal "t"/"alpha" ids must be seeded directly
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            foreach (var id in new[] { "t", "alpha" })
                await session.StoreAsync(
                    new Channel { Type = ChannelType.IFrame, DisplayName = id, AgentId = "demo", Enabled = true, CreatedAt = now },
                    $"{Channel.IdPrefix}{id}");
            await session.SaveChangesAsync();
        }
        await SeedConversationAsync(app.Store, app.Slug, "chats/x", "demo", now, channelId: "t");
        await SeedConversationAsync(app.Store, app.Slug, "chats/y", "demo", now, channelId: "alpha");

        // GetUsageAsync asserts 2xx → a 500 from the "t" collision fails here
        var byChannel = (await app.GetUsageAsync(now.Year, now.Month)).ConversationsByChannel;

        var keys = byChannel.Keys.Select(k => k.Key).ToArray();
        Assert.Contains("alpha", keys);
        Assert.DoesNotContain("t", keys);

        foreach (var p in byChannel.Points)
            Assert.Equal(JsonValueKind.String, ((JsonElement)p["t"]).ValueKind);
    }
}
