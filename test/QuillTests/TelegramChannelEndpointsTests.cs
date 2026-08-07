using System.Net;
using System.Net.Http.Json;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillTelegramCollection.Name)]
public class TelegramChannelEndpointsTests(ITestOutputHelper output, QuillTelegramFixture fixture)
    : QuillTelegramTestBase(output, fixture)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_requires_bot_token()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.Telegram, agentId, null)));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("botToken is required", e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_an_unknown_agent_id()
    {
        await using var app = await NewAppAsync();

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.Telegram, "no-such-agent", null, BotToken: NewBotToken())));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("unknown agentId 'no-such-agent'", e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_a_token_telegram_rejects_and_never_echoes_it()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        Mock.GetMeFailure = MockTelegramBotApi.Unauthorized;

        var token = NewBotToken();
        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, BotToken: token)));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("telegram rejected the bot token", e.Body);
        Assert.DoesNotContain(TokenSecret(token), e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_and_rotation_reject_a_malformed_token()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.Telegram, agentId, null, BotToken: "not-a-valid-token")));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("invalid bot token format", e.Body);

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, BotToken: NewBotToken()));
        var rotate = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, null, BotToken: "12noSecretPart")));
        Assert.Equal(HttpStatusCode.BadRequest, rotate.StatusCode);
        Assert.Contains("invalid bot token format", rotate.Body);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_defaults_display_name_to_bot_username_and_projects_it()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, BotToken: NewBotToken()));

        var channels = await app.GetChannelsAsync();
        var summary = Assert.Single(channels, c => c.ChannelId == created.ChannelId);
        Assert.Equal(ChannelType.Telegram, summary.Type);
        Assert.Equal("@quill_test_bot", summary.DisplayName);
        Assert.Equal("quill_test_bot", summary.BotUsername);
        Assert.True(summary.Enabled);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_a_second_channel_for_the_same_bot_in_one_app()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var token = NewBotToken();

        var first = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, BotToken: token));

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, BotToken: token)));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("already connected", e.Body);

        await app.DeleteChannelAsync(first.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_requires_values_for_declared_parameters_except_user_identifier()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app,
            new AiAgentParameter("customerId", "the customer to scope queries to"),
            new AiAgentParameter("UserIdentifier", "the telegram user id"));

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.Telegram, agentId, null, BotToken: NewBotToken())));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("missing agent parameter(s): customerId", e.Body);
        Assert.DoesNotContain("UserIdentifier", e.Body);

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Telegram, agentId, null, BotToken: NewBotToken(),
            Parameters: new Dictionary<string, string> { ["customerId"] = "customers/1" }));
        Assert.NotEmpty(created.ChannelId);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_exempts_the_telegram_username_parameter()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app,
            new AiAgentParameter("customerId", "the customer to scope queries to"),
            new AiAgentParameter("UserIdentifier", "the telegram user id"),
            new AiAgentParameter("TelegramUsername", "the telegram username"));

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.Telegram, agentId, null, BotToken: NewBotToken())));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("missing agent parameter(s): customerId", e.Body);
        Assert.DoesNotContain("TelegramUsername", e.Body);
        Assert.DoesNotContain("UserIdentifier", e.Body);

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Telegram, agentId, null, BotToken: NewBotToken(),
            Parameters: new Dictionary<string, string> { ["customerId"] = "customers/1" }));
        Assert.NotEmpty(created.ChannelId);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_and_list_never_contain_the_bot_token()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var token = NewBotToken();

        var resp = await Host.Client.PostAsJsonAsync(QuillRoutes.SetupChannel(app.Slug),
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, BotToken: token), QuillHttp.Json);
        var provisionBody = await resp.Content.ReadAsStringAsync();
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.DoesNotContain(TokenSecret(token), provisionBody);

        var listBody = await Host.Client.GetStringAsync(QuillRoutes.Channels(app.Slug));
        Assert.DoesNotContain(TokenSecret(token), listBody);

        var channels = await app.GetChannelsAsync();
        await app.DeleteChannelAsync(channels.Single().ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_and_update_reject_allowed_origins()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.Telegram, agentId, new[] { "http://localhost" }, BotToken: NewBotToken())));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("allowedOrigins does not apply", e.Body);

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, BotToken: NewBotToken()));
        var update = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, [], null)));
        Assert.Equal(HttpStatusCode.BadRequest, update.StatusCode);
        Assert.Contains("allowedOrigins does not apply", update.Body);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Token_rotation_revalidates_and_restarts_the_poller()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var original = NewBotToken();

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, BotToken: original));
        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(original) >= 1, "a poll with the original token");

        var rotated = NewBotToken();
        var summary = await app.UpdateChannelAsync(created.ChannelId,
            new UpdateChannelRequest(null, null, null, BotToken: rotated));
        Assert.Equal("quill_test_bot", summary.BotUsername);

        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(rotated) >= 1, "a poll with the rotated token");

        Mock.GetMeFailure = MockTelegramBotApi.Unauthorized;
        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, null, BotToken: NewBotToken())));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    private static async Task<string> SeedAgentAsync(QuillApp app, params AiAgentParameter[] parameters)
    {
        var agentId = "tg-agent-" + Guid.NewGuid().ToString("N")[..8];
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Telegram Demo Agent",
            SystemPrompt = "You are a placeholder demo agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Parameters = parameters.ToList(),
        });
        return agentId;
    }

    private static string TokenSecret(string token) => token[(token.IndexOf(':') + 1)..];
}
