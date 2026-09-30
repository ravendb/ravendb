using QuillTests.E2E.Fixtures;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;
using static QuillTests.E2E.Fixtures.ConversationSeed;

namespace QuillTests;

public class ConversationStatsEndpointsTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversation_stats_counts_conversations_in_selected_period()
    {
        await using var app = await NewAppAsync();

        var now = DateTime.UtcNow;
        var monthStart = new DateTime(now.Year, now.Month, 1, 0, 0, 0, DateTimeKind.Utc);
        await SeedConversationAsync(app.Store, app.Slug, "chats/a", "demo", PastInMonth(now, 2));
        await SeedConversationAsync(app.Store, app.Slug, "chats/b", "demo", PastInMonth(now, 1));
        await SeedConversationAsync(app.Store, app.Slug, "chats/c", "demo", PastInMonth(now, 0));
        await SeedConversationAsync(app.Store, app.Slug, "chats/prev", "demo", monthStart.AddDays(-3));

        var stats = await app.GetConversationStatsAsync(now.Year, now.Month);
        Assert.Equal(3, stats.Conversations);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversation_stats_sums_messages_and_tokens_in_selected_period()
    {
        await using var app = await NewAppAsync();

        var now = DateTime.UtcNow;
        var monthStart = new DateTime(now.Year, now.Month, 1, 0, 0, 0, DateTimeKind.Utc);
        await SeedConversationAsync(app.Store, app.Slug, "chats/a", "demo", PastInMonth(now, 2), messages: 3, tokens: 100);
        await SeedConversationAsync(app.Store, app.Slug, "chats/b", "demo", PastInMonth(now, 1), messages: 5, tokens: 250);
        await SeedConversationAsync(app.Store, app.Slug, "chats/prev", "demo", monthStart.AddDays(-3), messages: 9, tokens: 999);

        var stats = await app.GetConversationStatsAsync(now.Year, now.Month);
        Assert.Equal(2, stats.Conversations);
        Assert.Equal(8, stats.Messages);
        Assert.Equal(350, stats.Tokens);
    }
}
