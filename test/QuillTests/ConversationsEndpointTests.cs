using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;
using static QuillTests.E2E.Fixtures.ConversationSeed;

namespace QuillTests;

public class ConversationsEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversations_list_and_detail_shape_transcript_state_and_agent()
    {
        await using var app = await NewAppAsync();
        var now = DateTime.UtcNow;
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "demo", Name = "Demo", SystemPrompt = "You help.", ConnectionStringName = Host.ConnectionStringName,
        });
        var channel = await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "demo", Array.Empty<string>(), "Support Widget"));
        await SeedConversationAsync(app.Store, app.Slug, "chats/recent", "order-support", now.AddMinutes(-10),
            turns: [("user", "hello"), ("assistant", "hi there")], channelId: channel.ChannelId);
        await SeedConversationAsync(app.Store, app.Slug, "chats/old", "billing", now.AddDays(-3));

        var list = (await app.GetConversationsAsync(year: now.Year)).Conversations;
        Assert.Equal(2, list.Count);

        var first = list[0];
        Assert.Equal("chats/recent", first.Id);
        Assert.Equal(app.Slug, first.AppId);
        Assert.Equal("order-support", first.AgentName);
        Assert.Equal("active", first.State);
        var firstExchange = first.LastExchange;
        Assert.Equal(2, firstExchange.Length);
        Assert.Equal(AiMessageRole.Assistant, firstExchange[0].Role);
        Assert.Equal("hi there", firstExchange[0].Content);
        Assert.Equal(AiMessageRole.User, firstExchange[1].Role);
        Assert.Equal("hello", firstExchange[1].Content);
        Assert.Null(first.Transcript);
        Assert.Equal("Support Widget", first.ChannelName);

        Assert.Equal("closed", list[1].State);
        Assert.Equal("", list[1].ChannelName);

        var detail = await app.GetConversationAsync("chats/recent");
        var transcript = detail.Transcript;
        Assert.Equal(2, transcript.Length);
        Assert.Equal(AiMessageRole.User, transcript[0].Role);
        Assert.Equal("hello", transcript[0].Content);

        Assert.Equal(DateTimeKind.Utc, detail.LastActivityAt.Kind);
        Assert.Equal(DateTimeKind.Utc, detail.StartedAt!.Value.Kind);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversations_list_folds_channels_into_the_query_with_no_extra_round_trips()
    {
        await using var app = await NewAppAsync();
        var now = DateTime.UtcNow;
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "demo", Name = "Demo", SystemPrompt = "You help.", ConnectionStringName = Host.ConnectionStringName,
        });
        for (var i = 0; i < 3; i++)
        {
            var channel = await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "demo", Array.Empty<string>(), $"Widget {i}"));
            await SeedConversationAsync(app.Store, app.Slug, $"chats/c{i}", "demo", now.AddMinutes(-(i + 1)), channelId: channel.ChannelId);
        }
        // Calls MetricsReadService directly to count round-trips, so it waits for indexing explicitly.
        await app.WaitForIndexingAsync();

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var result = await MetricsReadService.GetConversationsAsync(
            session, app.Slug, new UsagePeriod(now.Year, null, null, now), start: 0, pageSize: 50, now, CancellationToken.None);

        Assert.Equal(3, result.Conversations.Count);
        Assert.Equal("Widget 0", result.Conversations.Single(c => c.Id == "chats/c0").ChannelName);
        Assert.Equal("Widget 2", result.Conversations.Single(c => c.Id == "chats/c2").ChannelName);

        // Single round trip: the Include folded the channel docs, else N+1 (1 + 3).
        Assert.Equal(1, session.Advanced.NumberOfRequests);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversations_list_shows_the_stored_last_exchange_newest_first()
    {
        await using var app = await NewAppAsync();
        var now = DateTime.UtcNow;
        await SeedConversationAsync(app.Store, app.Slug, "chats/x", "agent-x", now.AddMinutes(-5),
            turns: [("user", "m13"), ("assistant", "m14")]);

        var list = (await app.GetConversationsAsync(year: now.Year)).Conversations;
        var conversation = Assert.Single(list);

        var exchange = conversation.LastExchange;
        Assert.Equal(2, exchange.Length);
        Assert.Equal(AiMessageRole.Assistant, exchange[0].Role);
        Assert.Equal("m14", exchange[0].Content);
        Assert.Equal(AiMessageRole.User, exchange[1].Role);
        Assert.Equal("m13", exchange[1].Content);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task GetConversation_resolves_a_percent_encoded_slash_in_the_id()
    {
        await using var app = await NewAppAsync();
        await SeedConversationAsync(app.Store, app.Slug, "chats/recent", "order-support", DateTime.UtcNow.AddMinutes(-5),
            turns: [("user", "hello"), ("assistant", "hi there")]);

        var detail = await app.GetConversationAsync("chats%2Frecent");
        var transcript = detail.Transcript;
        Assert.Equal(2, transcript.Length);
        Assert.Equal("hello", transcript[0].Content);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task GetConversation_returns_404_for_non_conversation_or_unknown_id()
    {
        await using var app = await NewAppAsync();
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "demo", Name = "Demo", SystemPrompt = "You help.", ConnectionStringName = Host.ConnectionStringName,
        });
        var channel = await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "demo", Array.Empty<string>()));

        var nonConversation = await Assert.ThrowsAsync<QuillHttpException>(() => app.GetConversationAsync($"channels/{channel.ChannelId}"));
        Assert.Equal(HttpStatusCode.NotFound, nonConversation.StatusCode);

        var unknown = await Assert.ThrowsAsync<QuillHttpException>(() => app.GetConversationAsync("chats/does-not-exist"));
        Assert.Equal(HttpStatusCode.NotFound, unknown.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversations_list_pages_by_recency_newest_first()
    {
        await using var app = await NewAppAsync();
        var now = DateTime.UtcNow;
        for (var i = 0; i < 5; i++)
            await SeedConversationAsync(app.Store, app.Slug, $"chats/c{i}", "demo", now.AddMinutes(-i));

        var page1 = await app.GetConversationsAsync(year: now.Year, pageSize: 2);
        Assert.Equal(new[] { "chats/c0", "chats/c1" }, page1.Conversations.Select(x => x.Id).ToArray());
        Assert.Equal(5, page1.TotalResults);

        var page2 = await app.GetConversationsAsync(year: now.Year, start: 2, pageSize: 2);
        Assert.Equal(new[] { "chats/c2", "chats/c3" }, page2.Conversations.Select(x => x.Id).ToArray());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversation_detail_filters_scaffolding_and_extracts_array_content()
    {
        await using var app = await NewAppAsync();

        await SeedRealisticConversationAsync(app.Store, app.Slug, "chats/real", "order-support", DateTime.UtcNow.AddMinutes(-5));

        var detail = await app.GetConversationAsync("chats/real");
        var transcript = detail.Transcript;

        Assert.Equal(4, transcript.Length);
        Assert.Equal(AiMessageRole.System, transcript[0].Role);
        Assert.Equal("You are a helpful assistant.", transcript[0].Content);
        Assert.Equal(AiMessageRole.User, transcript[1].Role);
        Assert.Equal("hello", transcript[1].Content);
        Assert.Equal(AiMessageRole.Assistant, transcript[2].Role);
        Assert.Equal("hi there", transcript[2].Content);
        Assert.Equal(AiMessageRole.Assistant, transcript[3].Role);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversation_detail_returns_the_full_transcript_of_a_long_conversation()
    {
        await using var app = await NewAppAsync();

        var messages = new List<(string Role, object? Content)> { ("system", "You are a helpful assistant.") };
        for (var i = 0; i < 15; i++)
        {
            messages.Add(("user", $"question {i}"));
            messages.Add(("assistant", $"answer {i}"));
        }
        await SeedConversationAsync(app.Store, app.Slug, "chats/long", "order-support", DateTime.UtcNow.AddMinutes(-5),
            richMessages: messages);

        var detail = await app.GetConversationAsync("chats/long");

        Assert.Equal(31, detail.Transcript.Length);
        Assert.Equal(AiMessageRole.System, detail.Transcript[0].Role);
        Assert.Equal("You are a helpful assistant.", detail.Transcript[0].Content);
        Assert.Equal("question 0", detail.Transcript[1].Content);
        Assert.Equal("answer 14", detail.Transcript[^1].Content);
    }
}
