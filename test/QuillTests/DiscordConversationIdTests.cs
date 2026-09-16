using FastTests;
using Raven.Quill.Discord;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class DiscordConversationIdTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Builds_the_daily_conversation_id_from_channel_and_sender()
    {
        var id = DiscordConversationId.ForUtcDay(
            "abc123", "800000000000000001", new DateTime(2026, 8, 17, 13, 45, 0, DateTimeKind.Utc));

        Assert.Equal("chats/discord/abc123/800000000000000001/2026-08-17", id);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Rolls_to_a_fresh_conversation_at_utc_midnight()
    {
        var beforeMidnight = DiscordConversationId.ForUtcDay(
            "abc123", "800000000000000001", new DateTime(2026, 8, 17, 23, 59, 59, DateTimeKind.Utc));
        var afterMidnight = DiscordConversationId.ForUtcDay(
            "abc123", "800000000000000001", new DateTime(2026, 8, 18, 0, 0, 1, DateTimeKind.Utc));

        Assert.NotEqual(beforeMidnight, afterMidnight);
        Assert.EndsWith("/2026-08-17", beforeMidnight);
        Assert.EndsWith("/2026-08-18", afterMidnight);
    }
}
