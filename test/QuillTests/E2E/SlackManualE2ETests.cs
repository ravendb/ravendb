using Microsoft.Extensions.DependencyInjection;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests.E2E;

public class SlackManualE2ETests(ITestOutputHelper output) : QuillTestBase(output)
{
    private const string BotTokenVariable = "QUILL_SLACK_E2E_BOT_TOKEN";
    private const string SigningSecretVariable = "QUILL_SLACK_E2E_SIGNING_SECRET";
    private const string ChannelVariable = "QUILL_SLACK_E2E_CHANNEL";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Real_slack_validates_the_token_and_posts_then_edits_a_message()
    {
        var botToken = Environment.GetEnvironmentVariable(BotTokenVariable);
        var signingSecret = Environment.GetEnvironmentVariable(SigningSecretVariable);
        var channel = Environment.GetEnvironmentVariable(ChannelVariable);
        if (string.IsNullOrEmpty(botToken) || string.IsNullOrEmpty(signingSecret) || string.IsNullOrEmpty(channel))
            Assert.Skip($"Set {BotTokenVariable}, {SigningSecretVariable} and {ChannelVariable} " +
                        "to run the live Slack E2E.");

        await using var host = await NewHostAsync();
        await using var app = await NewAppAsync(host);

        var agentId = "slack-live-agent";
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Live Slack Agent",
            SystemPrompt = "You are a demo agent; answer briefly.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null,
            Slack: new(botToken, signingSecret)));

        var channels = await app.GetChannelsAsync();
        var summary = Assert.Single(channels, c => c.ChannelId == created.ChannelId);
        Assert.NotEmpty(summary.Slack!.TeamId);
        Assert.NotEmpty(summary.Slack.BotUserId);

        var client = host.Services.GetRequiredService<Raven.Quill.Slack.ISlackClient>();
        var ts = await client.PostMessageAsync(botToken, channel,
            "Quill Slack E2E: outbound post works.", CancellationToken.None);
        await client.UpdateMessageAsync(botToken, channel, ts,
            "Quill Slack E2E: outbound post and edit work.", CancellationToken.None);

        await app.DeleteChannelAsync(created.ChannelId);
    }
}
