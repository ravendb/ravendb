using FastTests;
using Raven.Quill.Channels;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class ChannelConversationIdTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static readonly DateTime Noon = new(2026, 8, 17, 12, 0, 0, DateTimeKind.Utc);

    [RavenFact(RavenTestCategory.Quill)]
    public void Fingerprint_canonical_form_is_pinned()
    {
        var id = ChannelConversationId.ForUtcDay("discord", "abc123", "800000000000000001", Noon,
            new Dictionary<string, string> { ["userId"] = "users/1", ["handle"] = "dana.dev" });

        Assert.Equal("chats/discord/abc123/800000000000000001/2026-08-17/e1b1ca337dd45908", id);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Changing_a_parameter_value_rolls_to_a_fresh_conversation()
    {
        var before = ChannelConversationId.ForUtcDay("discord", "abc123", "800000000000000001", Noon,
            new Dictionary<string, string> { ["userId"] = "users/1" });
        var after = ChannelConversationId.ForUtcDay("discord", "abc123", "800000000000000001", Noon,
            new Dictionary<string, string> { ["userId"] = "users/2" });

        Assert.NotEqual(before, after);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Parameter_values_differing_only_by_case_roll_to_a_fresh_conversation()
    {
        var before = ChannelConversationId.ForUtcDay("discord", "abc123", "800000000000000001", Noon,
            new Dictionary<string, string> { ["userId"] = "users/1" });
        var after = ChannelConversationId.ForUtcDay("discord", "abc123", "800000000000000001", Noon,
            new Dictionary<string, string> { ["userId"] = "Users/1" });

        Assert.NotEqual(before, after);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Unchanged_values_derive_the_same_id_regardless_of_entry_order_and_name_casing()
    {
        var first = ChannelConversationId.ForUtcDay("discord", "abc123", "800000000000000001", Noon,
            new Dictionary<string, string> { ["userId"] = "users/1", ["handle"] = "dana.dev" });
        var second = ChannelConversationId.ForUtcDay("discord", "abc123", "800000000000000001", Noon,
            new Dictionary<string, string> { ["HANDLE"] = "dana.dev", ["USERID"] = "users/1" });

        Assert.Equal(first, second);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Values_cannot_bleed_across_parameter_boundaries()
    {
        var joined = ChannelConversationId.ForUtcDay("discord", "abc123", "800000000000000001", Noon,
            new Dictionary<string, string> { ["a"] = "x", ["b"] = "y" });
        var shifted = ChannelConversationId.ForUtcDay("discord", "abc123", "800000000000000001", Noon,
            new Dictionary<string, string> { ["a"] = "xy", ["b"] = "" });

        Assert.NotEqual(joined, shifted);
    }
}
