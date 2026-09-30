using Microsoft.Extensions.DependencyInjection;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests.E2E;

public class DiscordManualE2ETests(ITestOutputHelper output) : QuillTestBase(output)
{
    private const string BotTokenVariable = "QUILL_DISCORD_E2E_BOT_TOKEN";
    private const string ChannelVariable = "QUILL_DISCORD_E2E_CHANNEL_ID";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Real_discord_validates_the_token_and_posts_then_edits_a_message()
    {
        var botToken = Environment.GetEnvironmentVariable(BotTokenVariable);
        var channel = Environment.GetEnvironmentVariable(ChannelVariable);
        if (string.IsNullOrEmpty(botToken) || string.IsNullOrEmpty(channel))
            Assert.Skip($"Set {BotTokenVariable} and {ChannelVariable} to run the live Discord E2E.");

        await using var host = await NewHostAsync();
        await using var app = await NewAppAsync(host);

        var agentId = "discord-live-agent";
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Live Discord Agent",
            SystemPrompt = "You are a demo agent; answer briefly.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Discord, agentId, null, Discord: new(botToken)));

        var channels = await app.GetChannelsAsync();
        var summary = Assert.Single(channels, c => c.ChannelId == created.ChannelId);
        Assert.NotEmpty(summary.Discord!.ApplicationId);
        Assert.NotEmpty(summary.Discord.BotUserId);

        var client = host.Services.GetRequiredService<Raven.Quill.Discord.IDiscordClient>();

        var gatewayUrl = await client.GetGatewayUrlAsync(botToken, CancellationToken.None);
        Assert.StartsWith("wss://", gatewayUrl);

        var messageId = await client.CreateMessageAsync(botToken, channel,
            "Quill Discord E2E: outbound post works.", CancellationToken.None);
        await client.EditMessageAsync(botToken, channel, messageId,
            "Quill Discord E2E: outbound post and edit work.", CancellationToken.None);

        await app.DeleteChannelAsync(created.ChannelId);
    }
}
