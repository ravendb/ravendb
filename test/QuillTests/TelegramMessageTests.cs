using FastTests;
using Raven.Quill.Channels;
using Raven.Quill.Telegram;
using Telegram.Bot.Types;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class TelegramMessageTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("/clear", true)]
    [InlineData("/clear now", true)]
    [InlineData("/clear\tnow", true)]
    [InlineData("/clear\r\n", true)]
    [InlineData("/clear@SupportBot", true)]
    [InlineData("/clearall", false)]
    [InlineData("please /clear", false)]
    public void A_command_ends_at_any_whitespace_or_a_bot_mention(string text, bool isCommand)
    {
        var message = new TelegramMessage(new Message { Text = text, Chat = new Chat { Id = 1 } });

        Assert.Equal(isCommand, message.IsCommand("/clear"));
        Assert.Equal(isCommand ? ChannelMessageKind.Command : ChannelMessageKind.Prompt, message.Kind);
    }
}
