using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
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
        Assert.Null(summary.PhoneNumber);

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
                ChannelType.WhatsAppPersonal, agentId, null, BotToken: "123:AAtoken")));
        Assert.Equal(HttpStatusCode.BadRequest, token.StatusCode);
        Assert.Contains("botToken does not apply", token.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_requires_values_for_declared_parameters_except_user_identifier()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app,
            new AiAgentParameter("customerId", "the customer to scope queries to"),
            new AiAgentParameter("UserIdentifier", "the whatsapp sender"));

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null)));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("missing agent parameter(s): customerId", e.Body);
        Assert.DoesNotContain("UserIdentifier", e.Body);

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.WhatsAppPersonal, agentId, null,
            Parameters: new Dictionary<string, string> { ["customerId"] = "customers/1" }));

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            var channel = await session.LoadAsync<Channel>(Channel.IdPrefix + created.ChannelId);
            Assert.Equal("customers/1", channel.WhatsApp!.Parameters["customerId"]);
        }

        await app.DeleteChannelAsync(created.ChannelId);
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
            app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, null, BotToken: "123:AA")));
        Assert.Equal(HttpStatusCode.BadRequest, token.StatusCode);
        Assert.Contains("botToken does not apply", token.Body);

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
