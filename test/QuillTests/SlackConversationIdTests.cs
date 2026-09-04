using FastTests;
using Raven.Quill.Slack;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class SlackConversationIdTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static readonly Dictionary<string, string> NoParameters = new();

    [RavenFact(RavenTestCategory.Quill)]
    public void Builds_the_conversation_id_from_channel_sender_and_parameters()
    {
        var id = SlackConversationId.For("abc123", "U0SENDER01", NoParameters);

        Assert.Matches("^chats/slack/abc123/U0SENDER01/[0-9a-f]{16}$", id);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Same_chat_and_parameters_derive_the_same_id()
    {
        var first = SlackConversationId.For("abc123", "U0SENDER01", NoParameters);
        var second = SlackConversationId.For("abc123", "U0SENDER01", NoParameters);

        Assert.Equal(first, second);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Changing_a_parameter_value_rolls_to_a_fresh_conversation()
    {
        var before = SlackConversationId.For("abc123", "U0SENDER01",
            new Dictionary<string, string> { ["userId"] = "users/1" });
        var after = SlackConversationId.For("abc123", "U0SENDER01",
            new Dictionary<string, string> { ["userId"] = "users/2" });

        Assert.NotEqual(before, after);
    }
}
