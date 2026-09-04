using FastTests;
using Raven.Quill.Channels;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class ChannelConversationIdTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Fingerprint_canonical_form_is_pinned()
    {
        var id = ChannelConversationId.For("discord", "abc123", "800000000000000001",
            new Dictionary<string, string> { ["userId"] = "users/1", ["handle"] = "dana.dev" });

        Assert.Equal("chats/discord/abc123/800000000000000001/e1b1ca337dd45908", id);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Changing_a_parameter_value_rolls_to_a_fresh_conversation()
    {
        var before = ChannelConversationId.For("discord", "abc123", "800000000000000001",
            new Dictionary<string, string> { ["userId"] = "users/1" });
        var after = ChannelConversationId.For("discord", "abc123", "800000000000000001",
            new Dictionary<string, string> { ["userId"] = "users/2" });

        Assert.NotEqual(before, after);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Parameter_values_differing_only_by_case_roll_to_a_fresh_conversation()
    {
        var before = ChannelConversationId.For("discord", "abc123", "800000000000000001",
            new Dictionary<string, string> { ["userId"] = "users/1" });
        var after = ChannelConversationId.For("discord", "abc123", "800000000000000001",
            new Dictionary<string, string> { ["userId"] = "Users/1" });

        Assert.NotEqual(before, after);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Unchanged_values_derive_the_same_id_regardless_of_entry_order_and_name_casing()
    {
        var first = ChannelConversationId.For("discord", "abc123", "800000000000000001",
            new Dictionary<string, string> { ["userId"] = "users/1", ["handle"] = "dana.dev" });
        var second = ChannelConversationId.For("discord", "abc123", "800000000000000001",
            new Dictionary<string, string> { ["HANDLE"] = "dana.dev", ["USERID"] = "users/1" });

        Assert.Equal(first, second);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void The_id_extends_the_chat_prefix_with_the_fingerprint()
    {
        var prefix = ChannelConversationId.ChatPrefix("discord", "abc123", "800000000000000001");
        var id = ChannelConversationId.For("discord", "abc123", "800000000000000001",
            new Dictionary<string, string> { ["userId"] = "users/1" });

        Assert.Equal("chats/discord/abc123/800000000000000001/", prefix);
        Assert.StartsWith(prefix, id);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Values_cannot_bleed_across_parameter_boundaries()
    {
        var joined = ChannelConversationId.For("discord", "abc123", "800000000000000001",
            new Dictionary<string, string> { ["a"] = "x", ["b"] = "y" });
        var shifted = ChannelConversationId.For("discord", "abc123", "800000000000000001",
            new Dictionary<string, string> { ["a"] = "xy", ["b"] = "" });

        Assert.NotEqual(joined, shifted);
    }
}
