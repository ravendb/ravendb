using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Slack;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillSlackCollection.Name)]
public class SlackChannelEndpointsTests(ITestOutputHelper output, QuillSlackFixture fixture)
    : QuillSlackTestBase(output, fixture)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_projects_workspace_metadata_and_serializes_the_type_as_a_string_name()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        var teamId = NewTeamId();
        var botUserId = NewBotUserId();
        Slack.AddBot(botToken, teamId, "Acme Coffee", botUserId);

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null,
            DisplayName: "Support bot", Slack: new(botToken, NewAppToken())));

        var channels = await app.GetChannelsAsync();
        var summary = Assert.Single(channels, c => c.ChannelId == created.ChannelId);
        Assert.Equal(ChannelType.Slack, summary.Type);
        Assert.NotNull(summary.Slack);
        Assert.Equal(teamId, summary.Slack!.TeamId);
        Assert.Equal("Acme Coffee", summary.Slack.TeamName);
        Assert.Equal(botUserId, summary.Slack.BotUserId);
        Assert.Equal("Support bot", summary.DisplayName);
        Assert.Contains(botToken, Slack.AuthTestCalls);

        var raw = await (await Host.Client.GetAsync(QuillRoutes.Channels(app.Slug))).EnsureSuccessAsync();
        Assert.Contains("\"Slack\"", await raw.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_responses_never_leak_credentials()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        var appToken = NewAppToken();
        Slack.AddBot(botToken, NewTeamId(), "Leaky Inc", NewBotUserId());

        await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null,
            DisplayName: "Support bot", Slack: new(botToken, appToken)));

        var listResponse = await (await Host.Client.GetAsync(QuillRoutes.Channels(app.Slug))).EnsureSuccessAsync();
        var body = await listResponse.Content.ReadAsStringAsync();
        Assert.DoesNotContain(botToken, body);
        Assert.DoesNotContain(appToken, body);

        var health = await (await Host.Client.GetAsync(QuillRoutes.SlackHealth(app.Slug))).EnsureSuccessAsync();
        var healthBody = await health.Content.ReadAsStringAsync();
        Assert.DoesNotContain(botToken, healthBody);
        Assert.DoesNotContain(appToken, healthBody);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_requires_a_bot_token_and_an_app_level_token()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var noToken = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                DisplayName: "Support bot", Slack: new(null, "xapp-1"))));
        Assert.Equal(HttpStatusCode.BadRequest, noToken.StatusCode);
        Assert.Contains("botToken is required", noToken.Body);

        var badToken = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                DisplayName: "Support bot", Slack: new("xoxp-a-user-token", "xapp-1"))));
        Assert.Equal(HttpStatusCode.BadRequest, badToken.StatusCode);
        Assert.Contains("must be the bot token (xoxb-)", badToken.Body);

        var noAppToken = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                DisplayName: "Support bot", Slack: new(NewBotToken(), null))));
        Assert.Equal(HttpStatusCode.BadRequest, noAppToken.StatusCode);
        Assert.Contains("appToken is required", noAppToken.Body);

        var badAppToken = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                DisplayName: "Support bot", Slack: new(NewBotToken(), "xoxb-not-an-app-token"))));
        Assert.Equal(HttpStatusCode.BadRequest, badAppToken.StatusCode);
        Assert.Contains("must be the app-level token (xapp-)", badAppToken.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_an_app_token_slack_rejects_and_never_echoes_it()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        Slack.AddBot(botToken, NewTeamId(), "Acme", NewBotUserId());
        var unknownAppToken = "xapp-1-A0MOCK-" + Guid.NewGuid().ToString("N");

        var e = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                DisplayName: "Support bot", Slack: new(botToken, unknownAppToken))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("rejected the app-level token", e.Body);
        Assert.DoesNotContain(unknownAppToken, e.Body);
        Assert.Contains(unknownAppToken, Slack.SocketOpenCalls);
        Assert.Empty(await app.GetChannelsAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_an_unknown_agent_id()
    {
        await using var app = await NewAppAsync();

        var e = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, "no-such-agent", null,
                DisplayName: "Support bot", Slack: new(NewBotToken(), NewAppToken()))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("unknown agentId 'no-such-agent'", e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_a_token_slack_rejects_and_never_echoes_it()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var unknownToken = NewBotToken();

        var e = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                DisplayName: "Support bot", Slack: new(unknownToken, NewAppToken()))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("slack rejected the bot token", e.Body);
        Assert.DoesNotContain(unknownToken, e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_fails_when_the_slack_api_is_unreachable()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var appToken = NewAppToken();
        Slack.Down = true;

        var e = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                DisplayName: "Support bot", Slack: new(NewBotToken(), appToken))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("unavailable", e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_requires_a_binding_for_every_declared_parameter_and_supported_sources_only()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app, new AiAgentParameter("slackUser", "the sender's Slack user id"));
        var botToken = NewBotToken();
        var appToken = NewAppToken();
        Slack.AddBot(botToken, NewTeamId(), "Acme", NewBotUserId());

        var missing = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                DisplayName: "Support bot", Slack: new(botToken, appToken))));
        Assert.Equal(HttpStatusCode.BadRequest, missing.StatusCode);
        Assert.Contains("missing parameter binding(s)", missing.Body);
        Assert.Contains("missing_parameters", missing.Body);

        var unsupported = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                DisplayName: "Support bot", Slack: new(botToken, appToken,
                    ParameterBindings: new Dictionary<string, ChannelParameterBinding>
                    {
                        ["slackUser"] = new() { Source = ChannelParameterSource.PhoneNumber },
                    }))));
        Assert.Equal(HttpStatusCode.BadRequest, unsupported.StatusCode);
        Assert.Contains("cannot bind PhoneNumber", unsupported.Body);

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                DisplayName: "Support bot", Slack: new(botToken, appToken,
                    ParameterBindings: new Dictionary<string, ChannelParameterBinding>
                    {
                        ["slackUser"] = new() { Source = ChannelParameterSource.UserId },
                    })));
        Assert.NotEmpty(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_accepts_an_email_binding()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app, new AiAgentParameter("senderEmail", "the sender's email"));
        var botToken = NewBotToken();
        Slack.AddBot(botToken, NewTeamId(), "Acme", NewBotUserId());

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                DisplayName: "Support bot", Slack: new(botToken, NewAppToken(),
                    ParameterBindings: new Dictionary<string, ChannelParameterBinding>
                    {
                        ["senderEmail"] = new() { Source = ChannelParameterSource.Email },
                    })));

        var channels = await app.GetChannelsAsync();
        var summary = Assert.Single(channels, c => c.ChannelId == created.ChannelId);
        Assert.Equal(ChannelParameterSource.Email, summary.Slack!.ParameterBindings["senderEmail"].Source);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_a_user_id_binding_for_a_number_parameter()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app,
            new AiAgentParameter("orderLimit", "how many orders to consider")
            {
                Type = AiAgentParameterValueType.Number,
            });
        var botToken = NewBotToken();
        Slack.AddBot(botToken, NewTeamId(), "Acme", NewBotUserId());

        var invalid = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                DisplayName: "Support bot", Slack: new(botToken, NewAppToken(),
                    ParameterBindings: new Dictionary<string, ChannelParameterBinding>
                    {
                        ["orderLimit"] = new() { Source = ChannelParameterSource.UserId },
                    }))));

        Assert.Equal(HttpStatusCode.BadRequest, invalid.StatusCode);
        Assert.Contains("orderLimit", invalid.Body);
        Assert.Contains("UserId", invalid.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_foreign_settings_and_allowed_origins()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var telegram = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                DisplayName: "Support bot", Telegram: new("123:token"),
                Slack: new(NewBotToken(), "xapp-1"))));
        Assert.Contains("telegram settings apply to Telegram channels only", telegram.Body);

        var origins = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, new[] { "https://a.example" },
                DisplayName: "Support bot", Slack: new(NewBotToken(), "xapp-1"))));
        Assert.Contains("allowedOrigins does not apply", origins.Body);

        var crossType = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, agentId, new[] { "https://a.example" },
                DisplayName: "Support bot", Slack: new(NewBotToken(), "xapp-1"))));
        Assert.Contains("slack settings apply to Slack channels only", crossType.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_a_bot_already_connected_in_another_app()
    {
        await using var first = await NewAppAsync();
        await using var second = await NewAppAsync();
        var firstAgent = await SeedAgentAsync(first);
        var secondAgent = await SeedAgentAsync(second);
        var botToken = NewBotToken();
        Slack.AddBot(botToken, NewTeamId(), "Acme", NewBotUserId());

        await first.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, firstAgent, null, DisplayName: "Support bot", Slack: new(botToken, NewAppToken())));

        var e = await Assert.ThrowsAsync<QuillHttpException>(() => second.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, secondAgent, null,
                DisplayName: "Support bot", Slack: new(botToken, NewAppToken()))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("already connected", e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_reclaims_an_orphaned_bot_reservation()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        var teamId = NewTeamId();
        var botUserId = NewBotUserId();
        Slack.AddBot(botToken, teamId, "Acme", botUserId);

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null, DisplayName: "Support bot", Slack: new(botToken, NewAppToken())));

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            session.Delete(Channel.IdPrefix + created.ChannelId);
            await session.SaveChangesAsync();
        }

        var reclaimed = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null, DisplayName: "Support bot", Slack: new(botToken, NewAppToken())));
        Assert.NotEmpty(reclaimed.ChannelId);

        using (var configSession = Host.Config.OpenAsyncSession())
        {
            var reservation = await configSession.LoadAsync<SlackBotReservation>(
                SlackBotReservation.IdFor(teamId, botUserId));
            Assert.NotNull(reservation);
            Assert.Equal(Channel.IdPrefix + reclaimed.ChannelId, reservation!.ChannelId);
        }
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Update_rotates_credentials_and_revalidates_the_token()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var teamId = NewTeamId();
        var botUserId = NewBotUserId();
        var oldToken = NewBotToken();
        Slack.AddBot(oldToken, teamId, "Old Name", botUserId);

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null, DisplayName: "Support bot", Slack: new(oldToken, NewAppToken())));

        var bad = await Assert.ThrowsAsync<QuillHttpException>(() => app.UpdateChannelAsync(created.ChannelId,
            new UpdateChannelRequest(null, null, null, Slack: new(BotToken: NewBotToken()))));
        Assert.Equal(HttpStatusCode.BadRequest, bad.StatusCode);
        Assert.Contains("slack rejected the bot token", bad.Body);

        var foreignToken = NewBotToken();
        Slack.AddBot(foreignToken, NewTeamId(), "Other Workspace", NewBotUserId());
        var foreign = await Assert.ThrowsAsync<QuillHttpException>(() => app.UpdateChannelAsync(created.ChannelId,
            new UpdateChannelRequest(null, null, null, Slack: new(BotToken: foreignToken))));
        Assert.Contains("different workspace or bot", foreign.Body);

        var badAppToken = await Assert.ThrowsAsync<QuillHttpException>(() => app.UpdateChannelAsync(created.ChannelId,
            new UpdateChannelRequest(null, null, null, Slack: new(AppToken: "xoxb-wrong-kind"))));
        Assert.Contains("must be the app-level token (xapp-)", badAppToken.Body);

        var revokedAppToken = "xapp-1-A0MOCK-" + Guid.NewGuid().ToString("N");
        var rejectedAppToken = await Assert.ThrowsAsync<QuillHttpException>(() => app.UpdateChannelAsync(created.ChannelId,
            new UpdateChannelRequest(null, null, null, Slack: new(AppToken: revokedAppToken))));
        Assert.Contains("rejected the app-level token", rejectedAppToken.Body);

        var newToken = NewBotToken();
        var newAppToken = NewAppToken();
        Slack.AddBot(newToken, teamId, "New Name", botUserId);
        var updated = await app.UpdateChannelAsync(created.ChannelId,
            new UpdateChannelRequest(null, null, null, Slack: new(BotToken: newToken, AppToken: newAppToken)));

        Assert.Equal("New Name", updated.Slack!.TeamName);
        Assert.True(Slack.AuthTestCalls.Count >= 3, "rotation must re-validate against auth.test");
        Assert.Contains(newAppToken, Slack.SocketOpenCalls);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Update_replaces_bindings_display_name_and_enabled()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app, new AiAgentParameter("slackUser", "the sender's Slack user id"));
        var botToken = NewBotToken();
        Slack.AddBot(botToken, NewTeamId(), "Acme", NewBotUserId());

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null,
            DisplayName: "Support bot", Slack: new(botToken, NewAppToken(),
                ParameterBindings: new Dictionary<string, ChannelParameterBinding>
                {
                    ["slackUser"] = new() { Source = ChannelParameterSource.UserId },
                })));

        var updated = await app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(
            "Support line", null, false,
            Slack: new(ParameterBindings: new Dictionary<string, ChannelParameterBinding>
            {
                ["slackUser"] = new() { Source = ChannelParameterSource.Constant, Value = "U000" },
            })));

        Assert.Equal("Support line", updated.DisplayName);
        Assert.False(updated.Enabled);
        Assert.Equal(ChannelParameterSource.Constant, updated.Slack!.ParameterBindings["slackUser"].Source);

        var foreign = await Assert.ThrowsAsync<QuillHttpException>(() => app.UpdateChannelAsync(created.ChannelId,
            new UpdateChannelRequest(null, null, null, Telegram: new("123:tok"))));
        Assert.Contains("telegram settings apply to Telegram channels only", foreign.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Update_rejects_slack_settings_on_non_slack_channels()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var iframe = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.IFrame, agentId, new[] { "https://a.example" }, "Storefront widget"));

        var onIFrame = await Assert.ThrowsAsync<QuillHttpException>(() => app.UpdateChannelAsync(iframe.ChannelId,
            new UpdateChannelRequest(null, null, null, Slack: new(AppToken: "xapp-rotated"))));
        Assert.Equal(HttpStatusCode.BadRequest, onIFrame.StatusCode);
        Assert.Contains("slack settings apply to Slack channels only", onIFrame.Body);

        var telegramChannelId = Guid.NewGuid().ToString("N");
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            await session.StoreAsync(new Channel
            {
                Id = Channel.IdPrefix + telegramChannelId,
                Type = ChannelType.Telegram,
                DisplayName = "tg",
                AgentId = agentId,
                AllowedOrigins = [],
                Enabled = true,
                CreatedAt = DateTime.UtcNow,
            });
            await session.SaveChangesAsync();
        }

        var onTelegram = await Assert.ThrowsAsync<QuillHttpException>(() => app.UpdateChannelAsync(telegramChannelId,
            new UpdateChannelRequest(null, null, null, Slack: new(AppToken: "xapp-rotated"))));
        Assert.Equal(HttpStatusCode.BadRequest, onTelegram.StatusCode);
        Assert.Contains("slack settings apply to Slack channels only", onTelegram.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_releases_the_bot_reservation_and_closes_the_socket()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        var teamId = NewTeamId();
        var botUserId = NewBotUserId();
        Slack.AddBot(botToken, teamId, "Acme", botUserId);

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null, DisplayName: "Support bot", Slack: new(botToken, NewAppToken())));
        await Slack.WaitUntilConnectedAsync();

        await app.DeleteChannelAsync(created.ChannelId);
        await Slack.WaitUntilAsync(() => Slack.IsConnected == false, "the socket to close");

        using (var configSession = Host.Config.OpenAsyncSession())
            Assert.Null(await configSession.LoadAsync<SlackBotReservation>(SlackBotReservation.IdFor(teamId, botUserId)));

        var recreated = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null, DisplayName: "Support bot", Slack: new(botToken, NewAppToken())));
        Assert.NotEmpty(recreated.ChannelId);
        await Slack.WaitUntilConnectedAsync();
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Health_reports_token_validity_per_channel_and_caches_the_verdict()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var teamId = NewTeamId();
        var botToken = NewBotToken();
        Slack.AddBot(botToken, teamId, "Acme", NewBotUserId());

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null, DisplayName: "Support bot", Slack: new(botToken, NewAppToken())));

        var rows = await QuillHttp.GetAsync<SlackChannelHealthResponse[]>(
            Host.Client, QuillRoutes.SlackHealth(app.Slug));

        var row = Assert.Single(rows, r => r.ChannelId == created.ChannelId);
        Assert.True(row.TokenValid);
        Assert.Null(row.TokenError);
        Assert.Equal(teamId, row.TeamId);
        Assert.True(row.Enabled);
        Assert.Null(row.LastInboundAt);

        var callsAfterFirstPoll = Slack.AuthTestCalls.Count;
        await QuillHttp.GetAsync<SlackChannelHealthResponse[]>(Host.Client, QuillRoutes.SlackHealth(app.Slug));
        Assert.Equal(callsAfterFirstPoll, Slack.AuthTestCalls.Count);
    }

    private static async Task<string> SeedAgentAsync(QuillApp app, params AiAgentParameter[] parameters)
    {
        var agentId = "slack-agent-" + Guid.NewGuid().ToString("N")[..8];
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Slack Demo Agent",
            SystemPrompt = "You are a placeholder demo agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Parameters = parameters.ToList(),
        });
        return agentId;
    }
}
