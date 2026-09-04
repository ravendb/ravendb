using System.Text.Json;
using QuillTests.E2E.Fixtures;
using Raven.Quill.Agents;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class UsageRollupTests(ITestOutputHelper output) : QuillTestBase(output)
{
    private static readonly ConversationLifetime ChannelLifetime =
        new(TranscriptIdleWindow: TimeSpan.FromHours(24), PreviewRetention: TimeSpan.FromDays(30));

    private static AgentRequest Request(string database, string conversationId, ConversationLifetime? lifetime) =>
        new(database, "support", conversationId, "hello", "channels/abc123",
            new Dictionary<string, JsonElement>(), lifetime);

    private static Task<bool> TurnAsync(QuillApp app, string conversationId, DateTime at,
        long tokens = 0, ConversationLifetime? lifetime = null) =>
        AgentRouter.RecordTurnAsync(app.Store, Request(app.Slug, conversationId, lifetime ?? ChannelLifetime),
            "support", conversationId, "hi", tokens, at, CancellationToken.None);

    private static async Task<Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry<UsageIncrement>[]> EntriesAsync(QuillApp app)
    {
        using var session = app.Store.OpenAsyncSession(app.Slug);
        return await session.IncrementalTimeSeriesFor<UsageIncrement>(
            UsageMetrics.IdFor("support", "channels/abc123"), UsageMetrics.SeriesName).GetAsync();
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_turn_rolls_up_one_conversation_one_message_and_its_tokens_at_the_turn_hour()
    {
        await using var app = await NewAppAsync();
        var at = new DateTime(2026, 8, 17, 12, 41, 0, DateTimeKind.Utc);

        await TurnAsync(app, "chats/one", at, tokens: 350);

        var entry = Assert.Single(await EntriesAsync(app));
        Assert.Equal(new DateTime(2026, 8, 17, 12, 0, 0, DateTimeKind.Utc), entry.Timestamp);
        Assert.Equal(1, entry.Value.Conversations);
        Assert.Equal(1, entry.Value.Messages);
        Assert.Equal(350, entry.Value.Tokens);

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var doc = await session.LoadAsync<UsageMetrics>(UsageMetrics.IdFor("support", "channels/abc123"));
        Assert.Equal("support", doc.Agent);
        Assert.Equal("channels/abc123", doc.ChannelId);
        Assert.Equal(at, doc.LastTurnAt);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Turns_in_the_same_hour_merge_and_the_conversation_counts_once()
    {
        await using var app = await NewAppAsync();
        var at = new DateTime(2026, 8, 17, 12, 5, 0, DateTimeKind.Utc);

        await TurnAsync(app, "chats/one", at, tokens: 100);
        await TurnAsync(app, "chats/one", at.AddMinutes(20), tokens: 50);

        var entry = Assert.Single(await EntriesAsync(app));
        Assert.Equal(1, entry.Value.Conversations);
        Assert.Equal(2, entry.Value.Messages);
        Assert.Equal(150, entry.Value.Tokens);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_turn_past_the_idle_window_counts_a_fresh_conversation_and_resets_its_start()
    {
        await using var app = await NewAppAsync();
        var first = new DateTime(2026, 8, 17, 12, 0, 0, DateTimeKind.Utc);
        var later = first.AddHours(30);

        // rolled is false on first contact and true only when a returning chat idle-expired
        Assert.False(await TurnAsync(app, "chats/one", first));
        Assert.True(await TurnAsync(app, "chats/one", later));

        var entries = await EntriesAsync(app);
        Assert.Equal(2, entries.Sum(e => e.Value.Conversations));

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var preview = await session.LoadAsync<ConversationPreview>(ConversationPreview.IdFor("chats/one"));
        Assert.Equal(later, preview.CreatedAt);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Without_an_idle_window_a_living_preview_never_recounts_the_conversation()
    {
        await using var app = await NewAppAsync();
        var first = new DateTime(2026, 8, 17, 12, 0, 0, DateTimeKind.Utc);

        Assert.False(await TurnAsync(app, "chats/one", first, lifetime: new ConversationLifetime(null, null)));
        Assert.False(await TurnAsync(app, "chats/one", first.AddDays(90), lifetime: new ConversationLifetime(null, null)));

        var entries = await EntriesAsync(app);
        Assert.Equal(1, entries.Sum(e => e.Value.Conversations));
        Assert.Equal(2, entries.Sum(e => e.Value.Messages));

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var preview = await session.LoadAsync<ConversationPreview>(ConversationPreview.IdFor("chats/one"));
        Assert.Equal(first, preview.CreatedAt);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Concurrent_increments_on_the_same_bucket_both_land()
    {
        await using var app = await NewAppAsync();
        var at = new DateTime(2026, 8, 17, 12, 0, 0, DateTimeKind.Utc);

        await Task.WhenAll(
            TurnAsync(app, "chats/one", at, tokens: 10),
            TurnAsync(app, "chats/two", at, tokens: 20));

        var entry = Assert.Single(await EntriesAsync(app));
        Assert.Equal(2, entry.Value.Messages);
        Assert.Equal(30, entry.Value.Tokens);
    }
}
