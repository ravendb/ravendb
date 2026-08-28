using FastTests;
using Raven.Quill.Channels;
using Raven.Quill.Slack;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class SlackConversationIdTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static readonly Dictionary<string, ChannelParameterBinding> NoBindings = new();

    [RavenFact(RavenTestCategory.Quill)]
    public void Builds_the_daily_conversation_id_from_channel_sender_and_bindings()
    {
        var id = SlackConversationId.ForUtcDay(
            "abc123", "U0SENDER01", new DateTime(2026, 8, 17, 13, 45, 0, DateTimeKind.Utc), NoBindings);

        Assert.Matches("^chats/slack/abc123/U0SENDER01/2026-08-17/[0-9a-f]{8}$", id);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Rolls_to_a_fresh_conversation_at_utc_midnight()
    {
        var beforeMidnight = SlackConversationId.ForUtcDay(
            "abc123", "U0SENDER01", new DateTime(2026, 8, 17, 23, 59, 59, DateTimeKind.Utc), NoBindings);
        var afterMidnight = SlackConversationId.ForUtcDay(
            "abc123", "U0SENDER01", new DateTime(2026, 8, 18, 0, 0, 1, DateTimeKind.Utc), NoBindings);

        Assert.NotEqual(beforeMidnight, afterMidnight);
        Assert.Contains("/2026-08-17/", beforeMidnight);
        Assert.Contains("/2026-08-18/", afterMidnight);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Changing_a_constant_binding_value_rolls_to_a_fresh_conversation()
    {
        var noon = new DateTime(2026, 8, 17, 12, 0, 0, DateTimeKind.Utc);
        var before = SlackConversationId.ForUtcDay("abc123", "U0SENDER01", noon,
            new Dictionary<string, ChannelParameterBinding>
            {
                ["userId"] = new() { Source = ChannelParameterSource.Constant, Value = "users/1" },
            });
        var after = SlackConversationId.ForUtcDay("abc123", "U0SENDER01", noon,
            new Dictionary<string, ChannelParameterBinding>
            {
                ["userId"] = new() { Source = ChannelParameterSource.Constant, Value = "Users/2" },
            });

        Assert.NotEqual(before, after);
    }
}
