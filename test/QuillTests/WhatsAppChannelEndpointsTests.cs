using System.Net;
using System.Net.Http.Json;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.WhatsApp;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillWhatsAppCollection.Name)]
public class WhatsAppChannelEndpointsTests(ITestOutputHelper output, QuillWhatsAppFixture fixture)
    : QuillWhatsAppTestBase(output, fixture)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_creates_the_channel_and_starts_a_bridge_session()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null, DisplayName: "QA phone"));

        Assert.Contains((app.Slug, created.ChannelId), Bridge.StartedSessions);

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            var channel = await session.LoadAsync<Channel>(Channel.IdPrefix + created.ChannelId);
            Assert.NotNull(channel);
            Assert.Equal(ChannelType.WhatsAppPersonal, channel.Type);
            Assert.Equal("QA phone", channel.DisplayName);
            Assert.True(channel.Enabled);
            Assert.Empty(channel.AllowedOrigins);
            Assert.NotNull(channel.WhatsApp);
            Assert.Null(channel.WhatsApp.PhoneNumber);
        }

        var channels = await app.GetChannelsAsync();
        var summary = Assert.Single(channels, c => c.ChannelId == created.ChannelId);
        Assert.Equal(ChannelType.WhatsAppPersonal, summary.Type);
        Assert.Null(summary.WhatsApp?.PhoneNumber);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_list_serializes_the_type_as_a_string_name()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null));

        var raw = await Host.Client.GetStringAsync(QuillRoutes.Channels(app.Slug));
        Assert.Contains("\"WhatsAppPersonal\"", raw);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_an_unknown_agent_id()
    {
        await using var app = await NewAppAsync();

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, "no-such-agent", null)));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("unknown agentId 'no-such-agent'", e.Body);
        Assert.Empty(Bridge.StartedSessions);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_allowed_origins_and_bot_token()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var origins = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.WhatsAppPersonal, agentId, ["https://example.com"])));
        Assert.Equal(HttpStatusCode.BadRequest, origins.StatusCode);
        Assert.Contains("allowedOrigins does not apply", origins.Body);

        var token = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.WhatsAppPersonal, agentId, null,
                Telegram: new TelegramProvisionRequest(BotToken: "123:AAtoken"))));
        Assert.Equal(HttpStatusCode.BadRequest, token.StatusCode);
        Assert.Contains("telegram settings apply to Telegram channels only", token.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_requires_a_binding_for_every_declared_parameter()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app,
            new AiAgentParameter("customerId", "the customer to scope queries to"),
            new AiAgentParameter("sender", "the whatsapp sender"));

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null)));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("missing parameter binding(s) for agent parameter(s): customerId, sender", e.Body);

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.WhatsAppPersonal, agentId, null,
            WhatsApp: new WhatsAppProvisionRequest(new Dictionary<string, TelegramParameterBinding>
            {
                ["customerId"] = new() { Source = TelegramParameterSource.Constant, Value = "customers/1" },
                ["sender"] = new() { Source = TelegramParameterSource.PhoneNumber },
            })));

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            var channel = await session.LoadAsync<Channel>(Channel.IdPrefix + created.ChannelId);
            Assert.Equal("customers/1", channel.WhatsApp!.ParameterBindings["customerId"].Value);
            Assert.Equal(TelegramParameterSource.PhoneNumber, channel.WhatsApp!.ParameterBindings["sender"].Source);
        }

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_binding_sources_whatsapp_cannot_bind()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app, new AiAgentParameter("sender", "the whatsapp sender"));

        foreach (var source in new[] { TelegramParameterSource.UserId, TelegramParameterSource.Username })
        {
            var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
                app.ProvisionChannelAsync(new ProvisionChannelRequest(
                    ChannelType.WhatsAppPersonal, agentId, null,
                    WhatsApp: new WhatsAppProvisionRequest(new Dictionary<string, TelegramParameterBinding>
                    {
                        ["sender"] = new() { Source = source },
                    }))));
            Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
            Assert.Contains($"WhatsApp channels cannot bind {source}", e.Body);
        }
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_succeeds_when_the_bridge_is_down_and_defers_the_session_start()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        Bridge.Down = true;

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null));

        Assert.Empty(Bridge.StartedSessions);
        using (var session = app.Store.OpenAsyncSession(app.Slug))
            Assert.NotNull(await session.LoadAsync<Channel>(Channel.IdPrefix + created.ChannelId));

        Bridge.Down = false;
        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Update_edits_name_and_enabled_and_rejects_origins_and_token()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null));

        var updated = await app.UpdateChannelAsync(created.ChannelId,
            new UpdateChannelRequest("Support phone", null, false));
        Assert.Equal("Support phone", updated.DisplayName);
        Assert.False(updated.Enabled);

        var origins = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, ["https://example.com"], null)));
        Assert.Equal(HttpStatusCode.BadRequest, origins.StatusCode);
        Assert.Contains("allowedOrigins does not apply", origins.Body);

        var token = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, null,
                Telegram: new TelegramUpdateRequest(BotToken: "123:AA"))));
        Assert.Equal(HttpStatusCode.BadRequest, token.StatusCode);
        Assert.Contains("telegram settings apply to Telegram channels only", token.Body);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_unlinks_the_bridge_session_and_removes_the_channel()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null));

        await app.DeleteChannelAsync(created.ChannelId);

        Assert.Contains((app.Slug, created.ChannelId), Bridge.DeletedSessions);
        Assert.False(Bridge.HasSession(app.Slug, created.ChannelId));
        using (var session = app.Store.OpenAsyncSession(app.Slug))
            Assert.Null(await session.LoadAsync<Channel>(Channel.IdPrefix + created.ChannelId));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_refuses_and_keeps_the_channel_when_the_bridge_is_down()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null));

        Bridge.Down = true;
        var e = await Assert.ThrowsAsync<QuillHttpException>(() => app.DeleteChannelAsync(created.ChannelId));
        Assert.Equal(HttpStatusCode.BadGateway, e.StatusCode);

        using (var session = app.Store.OpenAsyncSession(app.Slug))
            Assert.NotNull(await session.LoadAsync<Channel>(Channel.IdPrefix + created.ChannelId));

        Bridge.Down = false;
        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Pairing_returns_the_current_qr_while_pairing()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null));

        var pairing = await Host.Client.GetFromJsonAsync<WhatsAppPairingResponse>(
            QuillRoutes.WhatsAppPairing(app.Slug, created.ChannelId), QuillHttp.Json);

        Assert.NotNull(pairing);
        Assert.Equal(WhatsAppSessionState.Pairing, pairing.State);
        Assert.StartsWith("QR-", pairing.Qr);
        Assert.NotNull(pairing.QrExpiresAt);
        Assert.Null(pairing.PhoneNumber);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Pairing_lazy_starts_a_session_the_bridge_lost()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null));

        Bridge.RemoveSession(app.Slug, created.ChannelId);

        var pairing = await Host.Client.GetFromJsonAsync<WhatsAppPairingResponse>(
            QuillRoutes.WhatsAppPairing(app.Slug, created.ChannelId), QuillHttp.Json);

        Assert.NotNull(pairing);
        Assert.Equal(WhatsAppSessionState.Pairing, pairing.State);
        Assert.Equal(2, Bridge.StartedSessions.Count(s => s == (app.Slug, created.ChannelId)));

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Pairing_persists_the_phone_number_and_clears_it_on_logout()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null));

        Bridge.SetStatus(app.Slug, created.ChannelId, "connected", phoneNumber: "+48111222333");
        var connected = await Host.Client.GetFromJsonAsync<WhatsAppPairingResponse>(
            QuillRoutes.WhatsAppPairing(app.Slug, created.ChannelId), QuillHttp.Json);
        Assert.Equal(WhatsAppSessionState.Connected, connected!.State);
        Assert.Equal("+48111222333", connected.PhoneNumber);

        var summary = Assert.Single(await app.GetChannelsAsync(), c => c.ChannelId == created.ChannelId);
        Assert.Equal("+48111222333", summary.WhatsApp!.PhoneNumber);
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            var channel = await session.LoadAsync<Channel>(Channel.IdPrefix + created.ChannelId);
            Assert.NotNull(channel.WhatsApp!.LinkedAt);
        }

        Bridge.SetStatus(app.Slug, created.ChannelId, "loggedOut", lastError: "the phone unlinked this device");
        var loggedOut = await Host.Client.GetFromJsonAsync<WhatsAppPairingResponse>(
            QuillRoutes.WhatsAppPairing(app.Slug, created.ChannelId), QuillHttp.Json);
        Assert.Equal(WhatsAppSessionState.LoggedOut, loggedOut!.State);

        summary = Assert.Single(await app.GetChannelsAsync(), c => c.ChannelId == created.ChannelId);
        Assert.Null(summary.WhatsApp?.PhoneNumber);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Pairing_404s_for_unknown_and_non_whatsapp_channels()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var iframe = await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, agentId, []));

        var unknown = await Host.Client.GetAsync(QuillRoutes.WhatsAppPairing(app.Slug, Guid.NewGuid().ToString("N")));
        Assert.Equal(HttpStatusCode.NotFound, unknown.StatusCode);

        var wrongType = await Host.Client.GetAsync(QuillRoutes.WhatsAppPairing(app.Slug, iframe.ChannelId));
        Assert.Equal(HttpStatusCode.NotFound, wrongType.StatusCode);

        await app.DeleteChannelAsync(iframe.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Pairing_502s_when_the_bridge_is_down()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null));

        Bridge.Down = true;
        var response = await Host.Client.GetAsync(QuillRoutes.WhatsAppPairing(app.Slug, created.ChannelId));
        Assert.Equal(HttpStatusCode.BadGateway, response.StatusCode);

        Bridge.Down = false;
        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Restart_issues_a_fresh_qr()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null));
        Bridge.SetStatus(app.Slug, created.ChannelId, "loggedOut");

        var response = await Host.Client.PostAsJsonAsync(
            QuillRoutes.WhatsAppPairingRestart(app.Slug, created.ChannelId),
            new WhatsAppPairingRestartRequest(), QuillHttp.Json);
        Assert.True(response.IsSuccessStatusCode,
            $"{(int)response.StatusCode}: {await response.Content.ReadAsStringAsync()}");
        var pairing = await response.Content.ReadFromJsonAsync<WhatsAppPairingResponse>(QuillHttp.Json);

        Assert.Contains((app.Slug, created.ChannelId), Bridge.RestartedSessions);
        Assert.Equal(WhatsAppSessionState.Pairing, pairing!.State);
        Assert.StartsWith("QR-", pairing.Qr);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Restart_with_a_phone_number_issues_a_pairing_code_instead_of_a_qr()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null));

        var response = await Host.Client.PostAsJsonAsync(
            QuillRoutes.WhatsAppPairingRestart(app.Slug, created.ChannelId),
            new WhatsAppPairingRestartRequest("+48 601 234 567"), QuillHttp.Json);
        Assert.True(response.IsSuccessStatusCode,
            $"{(int)response.StatusCode}: {await response.Content.ReadAsStringAsync()}");
        var pairing = await response.Content.ReadFromJsonAsync<WhatsAppPairingResponse>(QuillHttp.Json);

        // written forms are normalized to bare digits before reaching the bridge
        Assert.Contains((app.Slug, created.ChannelId, "48601234567"), Bridge.PairingPhoneNumbers);
        Assert.Equal(WhatsAppSessionState.Pairing, pairing!.State);
        Assert.Equal("ABCD1234", pairing.PairingCode);
        Assert.Null(pairing.Qr);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Restart_rejects_a_malformed_pairing_phone_number()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null));

        var response = await Host.Client.PostAsJsonAsync(
            QuillRoutes.WhatsAppPairingRestart(app.Slug, created.ChannelId),
            new WhatsAppPairingRestartRequest("12345"), QuillHttp.Json);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Empty(Bridge.PairingPhoneNumbers);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Health_reports_each_whatsapp_channel()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var pairing = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null, DisplayName: "Pairing one"));
        var connected = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null, DisplayName: "Connected one"));
        Bridge.SetStatus(app.Slug, connected.ChannelId, "connected", phoneNumber: "+48111222333");
        await app.UpdateChannelAsync(pairing.ChannelId, new UpdateChannelRequest(null, null, false));

        var health = await Host.Client.GetFromJsonAsync<WhatsAppChannelHealthResponse[]>(
            QuillRoutes.WhatsAppHealth(app.Slug), QuillHttp.Json);

        Assert.NotNull(health);
        Assert.Equal(2, health.Length);

        var pairingRow = Assert.Single(health, h => h.ChannelId == pairing.ChannelId);
        Assert.Equal(WhatsAppSessionState.Pairing, pairingRow.State);
        Assert.False(pairingRow.Enabled);

        var connectedRow = Assert.Single(health, h => h.ChannelId == connected.ChannelId);
        Assert.Equal(WhatsAppSessionState.Connected, connectedRow.State);
        Assert.Equal("+48111222333", connectedRow.PhoneNumber);

        Bridge.Down = true;
        var degraded = await Host.Client.GetFromJsonAsync<WhatsAppChannelHealthResponse[]>(
            QuillRoutes.WhatsAppHealth(app.Slug), QuillHttp.Json);
        Assert.All(degraded!, h => Assert.Null(h.State));
        Assert.All(degraded!, h => Assert.Equal("whatsapp bridge is unavailable", h.LastError));

        Bridge.Down = false;
        await app.DeleteChannelAsync(pairing.ChannelId);
        await app.DeleteChannelAsync(connected.ChannelId);
    }

    private static async Task<string> SeedAgentAsync(QuillApp app, params AiAgentParameter[] parameters)
    {
        var agentId = "wa-agent-" + Guid.NewGuid().ToString("N")[..8];
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "WhatsApp Demo Agent",
            SystemPrompt = "You are a placeholder demo agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Parameters = parameters.ToList(),
        });
        return agentId;
    }
}
