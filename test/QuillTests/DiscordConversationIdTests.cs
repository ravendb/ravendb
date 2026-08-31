using FastTests;
using Raven.Quill.Discord;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class DiscordConversationIdTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static readonly Dictionary<string, string> NoParameters = new();

    [RavenFact(RavenTestCategory.Quill)]
    public void Builds_the_daily_conversation_id_from_channel_sender_and_parameters()
    {
        var id = DiscordConversationId.ForUtcDay(
            "abc123", "800000000000000001", new DateTime(2026, 8, 17, 13, 45, 0, DateTimeKind.Utc), NoParameters);

        Assert.Matches("^chats/discord/abc123/800000000000000001/2026-08-17/[0-9a-f]{8}$", id);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Rolls_to_a_fresh_conversation_at_utc_midnight()
    {
        var beforeMidnight = DiscordConversationId.ForUtcDay(
            "abc123", "800000000000000001", new DateTime(2026, 8, 17, 23, 59, 59, DateTimeKind.Utc), NoParameters);
        var afterMidnight = DiscordConversationId.ForUtcDay(
            "abc123", "800000000000000001", new DateTime(2026, 8, 18, 0, 0, 1, DateTimeKind.Utc), NoParameters);

        Assert.NotEqual(beforeMidnight, afterMidnight);
        Assert.Contains("/2026-08-17/", beforeMidnight);
        Assert.Contains("/2026-08-18/", afterMidnight);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Changing_a_parameter_value_rolls_to_a_fresh_conversation()
    {
        var noon = new DateTime(2026, 8, 17, 12, 0, 0, DateTimeKind.Utc);
        var before = DiscordConversationId.ForUtcDay("abc123", "800000000000000001", noon,
            new Dictionary<string, string> { ["userId"] = "users/1" });
        var after = DiscordConversationId.ForUtcDay("abc123", "800000000000000001", noon,
            new Dictionary<string, string> { ["userId"] = "users/2" });

        Assert.NotEqual(before, after);
    }
}
