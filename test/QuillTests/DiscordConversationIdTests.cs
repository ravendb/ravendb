using FastTests;
using Raven.Quill.Channels;
using Raven.Quill.Discord;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class DiscordConversationIdTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static readonly Dictionary<string, ChannelParameterBinding> NoBindings = new();

    [RavenFact(RavenTestCategory.Quill)]
    public void Builds_the_daily_conversation_id_from_channel_sender_and_bindings()
    {
        var id = DiscordConversationId.ForUtcDay(
            "abc123", "800000000000000001", new DateTime(2026, 8, 17, 13, 45, 0, DateTimeKind.Utc), NoBindings);

        Assert.Matches("^chats/discord/abc123/800000000000000001/2026-08-17/[0-9a-f]{8}$", id);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Rolls_to_a_fresh_conversation_at_utc_midnight()
    {
        var beforeMidnight = DiscordConversationId.ForUtcDay(
            "abc123", "800000000000000001", new DateTime(2026, 8, 17, 23, 59, 59, DateTimeKind.Utc), NoBindings);
        var afterMidnight = DiscordConversationId.ForUtcDay(
            "abc123", "800000000000000001", new DateTime(2026, 8, 18, 0, 0, 1, DateTimeKind.Utc), NoBindings);

        Assert.NotEqual(beforeMidnight, afterMidnight);
        Assert.Contains("/2026-08-17/", beforeMidnight);
        Assert.Contains("/2026-08-18/", afterMidnight);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Changing_a_constant_binding_value_rolls_to_a_fresh_conversation()
    {
        var noon = new DateTime(2026, 8, 17, 12, 0, 0, DateTimeKind.Utc);
        var before = DiscordConversationId.ForUtcDay("abc123", "800000000000000001", noon,
            new Dictionary<string, ChannelParameterBinding>
            {
                ["userId"] = new() { Source = ChannelParameterSource.Constant, Value = "users/1" },
            });
        var after = DiscordConversationId.ForUtcDay("abc123", "800000000000000001", noon,
            new Dictionary<string, ChannelParameterBinding>
            {
                ["userId"] = new() { Source = ChannelParameterSource.Constant, Value = "Users/2" },
            });

        Assert.NotEqual(before, after);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Constant_binding_values_differing_only_by_case_roll_to_a_fresh_conversation()
    {
        var noon = new DateTime(2026, 8, 17, 12, 0, 0, DateTimeKind.Utc);
        var before = DiscordConversationId.ForUtcDay("abc123", "800000000000000001", noon,
            new Dictionary<string, ChannelParameterBinding>
            {
                ["userId"] = new() { Source = ChannelParameterSource.Constant, Value = "users/1" },
            });
        var after = DiscordConversationId.ForUtcDay("abc123", "800000000000000001", noon,
            new Dictionary<string, ChannelParameterBinding>
            {
                ["userId"] = new() { Source = ChannelParameterSource.Constant, Value = "Users/1" },
            });

        Assert.NotEqual(before, after);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Changing_a_binding_source_rolls_to_a_fresh_conversation()
    {
        var noon = new DateTime(2026, 8, 17, 12, 0, 0, DateTimeKind.Utc);
        var constant = DiscordConversationId.ForUtcDay("abc123", "800000000000000001", noon,
            new Dictionary<string, ChannelParameterBinding>
            {
                ["userId"] = new() { Source = ChannelParameterSource.Constant, Value = "users/1" },
            });
        var senderBound = DiscordConversationId.ForUtcDay("abc123", "800000000000000001", noon,
            new Dictionary<string, ChannelParameterBinding>
            {
                ["userId"] = new() { Source = ChannelParameterSource.UserId },
            });

        Assert.NotEqual(constant, senderBound);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Unchanged_bindings_derive_the_same_id_regardless_of_entry_order_and_key_casing()
    {
        var noon = new DateTime(2026, 8, 17, 12, 0, 0, DateTimeKind.Utc);
        var first = DiscordConversationId.ForUtcDay("abc123", "800000000000000001", noon,
            new Dictionary<string, ChannelParameterBinding>
            {
                ["userId"] = new() { Source = ChannelParameterSource.Constant, Value = "users/1" },
                ["handle"] = new() { Source = ChannelParameterSource.Username },
            });
        var second = DiscordConversationId.ForUtcDay("abc123", "800000000000000001", noon,
            new Dictionary<string, ChannelParameterBinding>
            {
                ["HANDLE"] = new() { Source = ChannelParameterSource.Username },
                ["USERID"] = new() { Source = ChannelParameterSource.Constant, Value = "users/1" },
            });

        Assert.Equal(first, second);
    }
}
