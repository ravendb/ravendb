using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests.E2E;

/// Runs against the real api.telegram.org with a real bot token (no mock, no ApplianceOptions.Telegram.ApiUrl
/// override). Covers what MockTelegramBotApi cannot: real getMe validation, real long-poll wire format, and a
/// clean disable/delete against a live bot. The reply loop itself needs a human: message the bot from a real
/// Telegram account while the poller runs (see the comment in the test body).
public class TelegramManualE2ETests(ITestOutputHelper output) : QuillTestBase(output)
{
    private const string BotTokenVariable = "QUILL_TELEGRAM_E2E_BOT_TOKEN";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Real_bot_token_provisions_polls_and_cleans_up()
    {
        var botToken = Environment.GetEnvironmentVariable(BotTokenVariable);
        if (string.IsNullOrEmpty(botToken))
            Assert.Skip($"Set {BotTokenVariable} to a bot token from @BotFather to run the live Telegram E2E.");

        await using var app = await NewAppAsync();
        var agentId = "tg-live-agent";
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Live Telegram Agent",
            SystemPrompt = "You are a demo agent; answer briefly.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });

        // real getMe: a bad token fails here with telegram's reason
        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Telegram, agentId, null, Telegram: new(botToken)));

        var summary = Assert.Single(await app.GetChannelsAsync(), c => c.ChannelId == created.ChannelId);
        Assert.NotNull(summary.Telegram);
        Assert.NotEmpty(summary.Telegram!.BotUsername);

        // To exercise the full reply loop manually: put a breakpoint (or a long Task.Delay) here,
        // message the bot from a real Telegram account, and expect a streamed agent reply. The
        // seeded LLM connection string in QuillHost is unreachable, so a real reply additionally
        // needs the agent pointed at a live model.

        await app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, Enabled: false));

        await app.DeleteChannelAsync(created.ChannelId);
        Assert.Empty(await app.GetChannelsAsync());
    }
}
