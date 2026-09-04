using FastTests;
using Raven.Quill.Discord;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class DiscordConversationIdTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static readonly Dictionary<string, string> NoParameters = new();

    [RavenFact(RavenTestCategory.Quill)]
    public void Builds_the_conversation_id_from_channel_sender_and_parameters()
    {
        var id = DiscordConversationId.For("abc123", "800000000000000001", NoParameters);

        Assert.Matches("^chats/discord/abc123/800000000000000001/[0-9a-f]{16}$", id);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Same_chat_and_parameters_derive_the_same_id()
    {
        var first = DiscordConversationId.For("abc123", "800000000000000001", NoParameters);
        var second = DiscordConversationId.For("abc123", "800000000000000001", NoParameters);

        Assert.Equal(first, second);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Changing_a_parameter_value_rolls_to_a_fresh_conversation()
    {
        var before = DiscordConversationId.For("abc123", "800000000000000001",
            new Dictionary<string, string> { ["userId"] = "users/1" });
        var after = DiscordConversationId.For("abc123", "800000000000000001",
            new Dictionary<string, string> { ["userId"] = "users/2" });

        Assert.NotEqual(before, after);
    }
}
