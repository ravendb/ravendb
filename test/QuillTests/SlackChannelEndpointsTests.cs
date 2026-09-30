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
            Slack: new(botToken, "signing-secret-1")));

        var channels = await app.GetChannelsAsync();
        var summary = Assert.Single(channels, c => c.ChannelId == created.ChannelId);
        Assert.Equal(ChannelType.Slack, summary.Type);
        Assert.NotNull(summary.Slack);
        Assert.Equal(teamId, summary.Slack!.TeamId);
        Assert.Equal("Acme Coffee", summary.Slack.TeamName);
        Assert.Equal(botUserId, summary.Slack.BotUserId);
        Assert.Equal("quill-bot", summary.DisplayName);
        Assert.Contains(botToken, Slack.AuthTestCalls);

        var raw = await (await Host.Client.GetAsync(QuillRoutes.Channels(app.Slug))).EnsureSuccessAsync();
        Assert.Contains("\"Slack\"", await raw.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_responses_never_leak_credentials_or_webhook_tokens()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        var signingSecret = "secret-signing-" + Guid.NewGuid().ToString("N");
        Slack.AddBot(botToken, NewTeamId(), "Leaky Inc", NewBotUserId());

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null,
            Slack: new(botToken, signingSecret)));

        var info = await QuillHttp.GetAsync<SlackWebhookInfoResponse>(
            Host.Client, QuillRoutes.SlackWebhookInfo(app.Slug, created.ChannelId));
        var webhookToken = info.RequestUrl[(info.RequestUrl.LastIndexOf('/') + 1)..];

        var listResponse = await (await Host.Client.GetAsync(QuillRoutes.Channels(app.Slug))).EnsureSuccessAsync();
        var body = await listResponse.Content.ReadAsStringAsync();
        Assert.DoesNotContain(botToken, body);
        Assert.DoesNotContain(signingSecret, body);
        Assert.DoesNotContain(webhookToken, body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_requires_a_bot_token_and_a_signing_secret()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var noToken = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                Slack: new(null, "s"))));
        Assert.Equal(HttpStatusCode.BadRequest, noToken.StatusCode);
        Assert.Contains("botToken is required", noToken.Body);

        var badToken = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                Slack: new("xoxp-a-user-token", "s"))));
        Assert.Equal(HttpStatusCode.BadRequest, badToken.StatusCode);
        Assert.Contains("must be the bot token (xoxb-)", badToken.Body);

        var noSecret = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                Slack: new(NewBotToken(), null))));
        Assert.Equal(HttpStatusCode.BadRequest, noSecret.StatusCode);
        Assert.Contains("signingSecret is required", noSecret.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_an_unknown_agent_id()
    {
        await using var app = await NewAppAsync();

        var e = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, "no-such-agent", null,
                Slack: new(NewBotToken(), "s"))));

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
                Slack: new(unknownToken, "s"))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("slack rejected the bot token", e.Body);
        Assert.DoesNotContain(unknownToken, e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_fails_when_the_slack_api_is_unreachable()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        Slack.Down = true;

        var e = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                Slack: new(NewBotToken(), "s"))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("unavailable", e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_requires_a_binding_for_every_declared_parameter_and_supported_sources_only()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app, new AiAgentParameter("slackUser", "the sender's Slack user id"));
        var botToken = NewBotToken();
        Slack.AddBot(botToken, NewTeamId(), "Acme", NewBotUserId());

        var missing = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                Slack: new(botToken, "s"))));
        Assert.Equal(HttpStatusCode.BadRequest, missing.StatusCode);
        Assert.Contains("missing parameter binding(s)", missing.Body);
        Assert.Contains("missing_parameters", missing.Body);

        var unsupported = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                Slack: new(botToken, "s",
                    ParameterBindings: new Dictionary<string, ChannelParameterBinding>
                    {
                        ["slackUser"] = new() { Source = ChannelParameterSource.PhoneNumber },
                    }))));
        Assert.Equal(HttpStatusCode.BadRequest, unsupported.StatusCode);
        Assert.Contains("cannot bind PhoneNumber", unsupported.Body);

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, null,
                Slack: new(botToken, "s",
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
                Slack: new(botToken, "s",
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
                Slack: new(botToken, "s",
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
                Telegram: new("123:token"),
                Slack: new(NewBotToken(), "s"))));
        Assert.Contains("telegram settings apply to Telegram channels only", telegram.Body);

        var origins = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, agentId, new[] { "https://a.example" },
                Slack: new(NewBotToken(), "s"))));
        Assert.Contains("allowedOrigins does not apply", origins.Body);

        var crossType = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, agentId, new[] { "https://a.example" },
                Slack: new(NewBotToken(), "s"))));
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
            ChannelType.Slack, firstAgent, null, Slack: new(botToken, "s")));

        var e = await Assert.ThrowsAsync<QuillHttpException>(() => second.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Slack, secondAgent, null,
                Slack: new(botToken, "s"))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("already connected", e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_reclaims_an_orphaned_bot_reservation()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        Slack.AddBot(botToken, NewTeamId(), "Acme", NewBotUserId());

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null, Slack: new(botToken, "s")));
        var orphanToken = await WebhookTokenAsync(app, created.ChannelId);

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            session.Delete(Channel.IdPrefix + created.ChannelId);
            await session.SaveChangesAsync();
        }

        var reclaimed = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null, Slack: new(botToken, "s")));
        Assert.NotEmpty(reclaimed.ChannelId);
        var reclaimedToken = await WebhookTokenAsync(app, reclaimed.ChannelId);

        using (var configSession = Host.Config.OpenAsyncSession())
        {
            Assert.Null(await configSession.LoadAsync<SlackWebhookRoute>(SlackWebhookRoute.IdFor(orphanToken)));

            var route = await configSession.LoadAsync<SlackWebhookRoute>(SlackWebhookRoute.IdFor(reclaimedToken));
            Assert.NotNull(route);
            Assert.Equal(Channel.IdPrefix + reclaimed.ChannelId, route!.ChannelId);
        }
    }

    private async Task<string> WebhookTokenAsync(QuillApp app, string channelId)
    {
        var info = await QuillHttp.GetAsync<SlackWebhookInfoResponse>(
            Host.Client, QuillRoutes.SlackWebhookInfo(app.Slug, channelId));
        return info.RequestUrl[(info.RequestUrl.LastIndexOf('/') + 1)..];
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
            ChannelType.Slack, agentId, null, Slack: new(oldToken, "old-secret")));

        var bad = await Assert.ThrowsAsync<QuillHttpException>(() => app.UpdateChannelAsync(created.ChannelId,
            new UpdateChannelRequest(null, null, null, Slack: new(BotToken: NewBotToken()))));
        Assert.Equal(HttpStatusCode.BadRequest, bad.StatusCode);
        Assert.Contains("slack rejected the bot token", bad.Body);

        var foreignToken = NewBotToken();
        Slack.AddBot(foreignToken, NewTeamId(), "Other Workspace", NewBotUserId());
        var foreign = await Assert.ThrowsAsync<QuillHttpException>(() => app.UpdateChannelAsync(created.ChannelId,
            new UpdateChannelRequest(null, null, null, Slack: new(BotToken: foreignToken))));
        Assert.Contains("different workspace or bot", foreign.Body);

        var newToken = NewBotToken();
        Slack.AddBot(newToken, teamId, "New Name", botUserId);
        var updated = await app.UpdateChannelAsync(created.ChannelId,
            new UpdateChannelRequest(null, null, null, Slack: new(BotToken: newToken, SigningSecret: "new-secret")));

        Assert.Equal("New Name", updated.Slack!.TeamName);
        Assert.True(Slack.AuthTestCalls.Count >= 3, "rotation must re-validate against auth.test");
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
            Slack: new(botToken, "s",
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
            ChannelType.IFrame, agentId, new[] { "https://a.example" }));

        var onIFrame = await Assert.ThrowsAsync<QuillHttpException>(() => app.UpdateChannelAsync(iframe.ChannelId,
            new UpdateChannelRequest(null, null, null, Slack: new(SigningSecret: "rotated"))));
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
            new UpdateChannelRequest(null, null, null, Slack: new(SigningSecret: "rotated"))));
        Assert.Equal(HttpStatusCode.BadRequest, onTelegram.StatusCode);
        Assert.Contains("slack settings apply to Slack channels only", onTelegram.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_releases_the_bot_and_the_webhook_route()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        Slack.AddBot(botToken, NewTeamId(), "Acme", NewBotUserId());

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null, Slack: new(botToken, "s")));
        var info = await QuillHttp.GetAsync<SlackWebhookInfoResponse>(
            Host.Client, QuillRoutes.SlackWebhookInfo(app.Slug, created.ChannelId));
        var webhookToken = info.RequestUrl[(info.RequestUrl.LastIndexOf('/') + 1)..];

        await app.DeleteChannelAsync(created.ChannelId);

        var recreated = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null, Slack: new(botToken, "s")));
        Assert.NotEmpty(recreated.ChannelId);

        var drop = await Host.Client.PostAsync(QuillRoutes.SlackWebhook(webhookToken),
            new StringContent("{}", System.Text.Encoding.UTF8, "application/json"));
        Assert.Equal(HttpStatusCode.OK, drop.StatusCode);
        Assert.Empty(Router.Requests);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Webhook_info_returns_the_public_request_url()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        Slack.AddBot(botToken, NewTeamId(), "Acme", NewBotUserId());

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null, Slack: new(botToken, "s")));

        var info = await QuillHttp.GetAsync<SlackWebhookInfoResponse>(
            Host.Client, QuillRoutes.SlackWebhookInfo(app.Slug, created.ChannelId));

        Assert.Contains("/webhooks/slack/", info.RequestUrl);
        Assert.Equal(32, info.RequestUrl[(info.RequestUrl.LastIndexOf('/') + 1)..].Length);

        var widget = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.IFrame, agentId, Array.Empty<string>()));
        var e = await Assert.ThrowsAsync<QuillHttpException>(() => QuillHttp.GetAsync<SlackWebhookInfoResponse>(
            Host.Client, QuillRoutes.SlackWebhookInfo(app.Slug, widget.ChannelId)));
        Assert.Equal(HttpStatusCode.NotFound, e.StatusCode);
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
            ChannelType.Slack, agentId, null, Slack: new(botToken, "s")));

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
