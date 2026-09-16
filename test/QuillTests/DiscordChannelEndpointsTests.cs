using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Discord;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillDiscordCollection.Name)]
public class DiscordChannelEndpointsTests(ITestOutputHelper output, QuillDiscordFixture fixture)
    : QuillDiscordTestBase(output, fixture)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_projects_bot_metadata_and_serializes_the_type_as_a_string_name()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        var applicationId = NewApplicationId();
        var botUserId = NewBotUserId();
        Discord.AddBot(botToken, applicationId, botUserId, "acme-helper");

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Discord, agentId, null, Discord: new(botToken)));

        var channels = await app.GetChannelsAsync();
        var summary = Assert.Single(channels, c => c.ChannelId == created.ChannelId);
        Assert.Equal(ChannelType.Discord, summary.Type);
        Assert.NotNull(summary.Discord);
        Assert.Equal(applicationId, summary.Discord!.ApplicationId);
        Assert.Equal(botUserId, summary.Discord.BotUserId);
        Assert.Equal("acme-helper", summary.Discord.BotUsername);
        Assert.Equal("acme-helper", summary.DisplayName);
        Assert.Contains(botToken, Discord.IdentityCalls);

        var raw = await (await Host.Client.GetAsync(QuillRoutes.Channels(app.Slug))).EnsureSuccessAsync();
        Assert.Contains("\"Discord\"", await raw.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_responses_never_leak_the_bot_token()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        Discord.AddBot(botToken, NewApplicationId(), NewBotUserId());

        await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Discord, agentId, null, Discord: new(botToken)));

        var listResponse = await (await Host.Client.GetAsync(QuillRoutes.Channels(app.Slug))).EnsureSuccessAsync();
        Assert.DoesNotContain(botToken, await listResponse.Content.ReadAsStringAsync());

        var health = await (await Host.Client.GetAsync(QuillRoutes.DiscordHealth(app.Slug))).EnsureSuccessAsync();
        Assert.DoesNotContain(botToken, await health.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_requires_a_bot_token()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var missing = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Discord, agentId, null, Discord: new(BotToken: null))));
        Assert.Equal(HttpStatusCode.BadRequest, missing.StatusCode);
        Assert.Contains("discord.botToken is required", missing.Body);

        var blank = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Discord, agentId, null, Discord: new(BotToken: "   "))));
        Assert.Equal(HttpStatusCode.BadRequest, blank.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_an_unknown_agent_id()
    {
        await using var app = await NewAppAsync();
        var botToken = NewBotToken();
        Discord.AddBot(botToken, NewApplicationId(), NewBotUserId());

        var e = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Discord, "nope", null, Discord: new(botToken))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("unknown agentId", e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_a_token_discord_rejects_and_never_echoes_it()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();

        var e = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Discord, agentId, null, Discord: new(botToken))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("rejected the bot token", e.Body);
        Assert.DoesNotContain(botToken, e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_fails_when_the_discord_api_is_unreachable()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        Discord.AddBot(botToken, NewApplicationId(), NewBotUserId());
        Discord.Down = true;

        var e = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Discord, agentId, null, Discord: new(botToken))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("unavailable", e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_requires_a_binding_for_every_parameter_and_supported_sources_only()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app, new AiAgentParameter("customerId", "the customer"));
        var botToken = NewBotToken();
        Discord.AddBot(botToken, NewApplicationId(), NewBotUserId());

        var missing = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Discord, agentId, null, Discord: new(botToken))));
        Assert.Contains("missing parameter binding", missing.Body);

        var unsupported = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Discord, agentId, null,
                Discord: new(botToken, new Dictionary<string, ChannelParameterBinding>
                {
                    ["customerId"] = new() { Source = ChannelParameterSource.Email },
                }))));
        Assert.Contains("cannot bind Email", unsupported.Body);

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Discord, agentId, null,
            Discord: new(botToken, new Dictionary<string, ChannelParameterBinding>
            {
                ["customerId"] = new() { Source = ChannelParameterSource.Username },
            })));
        Assert.NotEmpty(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_foreign_settings_and_allowed_origins()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        Discord.AddBot(botToken, NewApplicationId(), NewBotUserId());

        var telegram = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Discord, agentId, null,
                Telegram: new("123:tok"), Discord: new(botToken))));
        Assert.Contains("telegram settings apply to Telegram channels only", telegram.Body);

        var origins = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Discord, agentId, ["https://acme.example"],
                Discord: new(botToken))));
        Assert.Contains("allowedOrigins does not apply to Discord channels", origins.Body);

        var crossType = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Discord: new(botToken))));
        Assert.Contains("discord settings apply to Discord channels only", crossType.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_a_bot_already_connected_in_another_app()
    {
        await using var first = await NewAppAsync();
        await using var second = await NewAppAsync();
        var firstAgent = await SeedAgentAsync(first);
        var secondAgent = await SeedAgentAsync(second);
        var botToken = NewBotToken();
        Discord.AddBot(botToken, NewApplicationId(), NewBotUserId());

        await first.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Discord, firstAgent, null, Discord: new(botToken)));

        var e = await Assert.ThrowsAsync<QuillHttpException>(() => second.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Discord, secondAgent, null, Discord: new(botToken))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("already connected", e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_reclaims_an_orphaned_bot_reservation()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        Discord.AddBot(botToken, NewApplicationId(), NewBotUserId());

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Discord, agentId, null, Discord: new(botToken)));

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            session.Delete(Channel.IdPrefix + created.ChannelId);
            await session.SaveChangesAsync();
        }

        var reclaimed = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Discord, agentId, null, Discord: new(botToken)));
        Assert.NotEmpty(reclaimed.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Update_rotates_the_token_and_refuses_a_different_bot()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        var applicationId = NewApplicationId();
        var botUserId = NewBotUserId();
        Discord.AddBot(botToken, applicationId, botUserId, "same-bot");

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Discord, agentId, null, Discord: new(botToken)));

        var rejected = await Assert.ThrowsAsync<QuillHttpException>(() => app.UpdateChannelAsync(
            created.ChannelId, new UpdateChannelRequest(null, null, null, Discord: new(BotToken: NewBotToken()))));
        Assert.Contains("rejected the bot token", rejected.Body);

        var foreignToken = NewBotToken();
        Discord.AddBot(foreignToken, NewApplicationId(), NewBotUserId(), "other-bot");
        var foreign = await Assert.ThrowsAsync<QuillHttpException>(() => app.UpdateChannelAsync(
            created.ChannelId, new UpdateChannelRequest(null, null, null, Discord: new(BotToken: foreignToken))));
        Assert.Contains("different bot", foreign.Body);

        var rotated = NewBotToken();
        Discord.AddBot(rotated, applicationId, botUserId, "renamed-bot");
        var updated = await app.UpdateChannelAsync(
            created.ChannelId, new UpdateChannelRequest(null, null, null, Discord: new(BotToken: rotated)));

        Assert.Equal("renamed-bot", updated.Discord!.BotUsername);
        Assert.Equal(botUserId, updated.Discord.BotUserId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Update_replaces_bindings_display_name_and_enabled()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app, new AiAgentParameter("customerId", "the customer"));
        var botToken = NewBotToken();
        Discord.AddBot(botToken, NewApplicationId(), NewBotUserId());

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Discord, agentId, null,
            Discord: new(botToken, new Dictionary<string, ChannelParameterBinding>
            {
                ["customerId"] = new() { Source = ChannelParameterSource.UserId },
            })));

        var updated = await app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(
            "Support desk", null, Enabled: false,
            Discord: new(ParameterBindings: new Dictionary<string, ChannelParameterBinding>
            {
                ["customerId"] = new() { Source = ChannelParameterSource.Constant, Value = "customers/7" },
            })));

        Assert.Equal("Support desk", updated.DisplayName);
        Assert.False(updated.Enabled);
        var binding = updated.Discord!.ParameterBindings["customerId"];
        Assert.Equal(ChannelParameterSource.Constant, binding.Source);
        Assert.Equal("customers/7", binding.Value);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Update_rejects_discord_settings_on_non_discord_channels()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var iframe = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, agentId, []));

        var e = await Assert.ThrowsAsync<QuillHttpException>(() => app.UpdateChannelAsync(
            iframe.ChannelId, new UpdateChannelRequest(null, null, null, Discord: new(BotToken: "rotated"))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("discord settings apply to Discord channels only", e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_releases_the_bot_reservation()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        var botUserId = NewBotUserId();
        Discord.AddBot(botToken, NewApplicationId(), botUserId);

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Discord, agentId, null, Discord: new(botToken)));
        await app.DeleteChannelAsync(created.ChannelId);

        using (var session = app.Store.OpenAsyncSession())
        {
            Assert.Null(await session.LoadAsync<DiscordBotReservation>(DiscordBotReservation.IdFor(botUserId)));
        }

        var reused = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Discord, agentId, null, Discord: new(botToken)));
        Assert.NotEmpty(reused.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Health_reports_token_validity_per_channel_and_caches_the_verdict()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var botToken = NewBotToken();
        var applicationId = NewApplicationId();
        var botUserId = NewBotUserId();
        Discord.AddBot(botToken, applicationId, botUserId);

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Discord, agentId, null, Discord: new(botToken)));

        var rows = await QuillHttp.GetAsync<DiscordChannelHealthResponse[]>(
            Host.Client, QuillRoutes.DiscordHealth(app.Slug));

        var row = Assert.Single(rows, r => r.ChannelId == created.ChannelId);
        Assert.True(row.TokenValid);
        Assert.Null(row.TokenError);
        Assert.Equal(applicationId, row.ApplicationId);
        Assert.Equal(botUserId, row.BotUserId);
        Assert.True(row.Enabled);
        Assert.Null(row.LastInboundAt);

        var callsAfterFirstPoll = Discord.IdentityCalls.Count;
        await QuillHttp.GetAsync<DiscordChannelHealthResponse[]>(Host.Client, QuillRoutes.DiscordHealth(app.Slug));
        Assert.Equal(callsAfterFirstPoll, Discord.IdentityCalls.Count);
    }

    private static async Task<string> SeedAgentAsync(QuillApp app, params AiAgentParameter[] parameters)
    {
        var agentId = "discord-agent-" + Guid.NewGuid().ToString("N")[..8];
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Discord Demo Agent",
            SystemPrompt = "You are a placeholder demo agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Parameters = parameters.ToList(),
        });
        return agentId;
    }
}
