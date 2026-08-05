using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.RegularExpressions;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints;
using Raven.Quill.Metrics;
using Raven.Quill.WhatsApp;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillWhatsAppCollection.Name)]
public class WhatsAppInboundTests(ITestOutputHelper output, QuillWhatsAppFixture fixture)
    : QuillWhatsAppTestBase(output, fixture)
{
    private const string Sender = "48123456789@s.whatsapp.net";
    private const string InboundRoute = "/internal/whatsapp/inbound";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Inbound_text_reaches_the_agent_and_the_reply_returns_to_the_sender()
    {
        await using var app = await NewAppAsync();
        var (agentId, channelId) = await SeedConnectedChannelAsync(app);

        var response = await PostInboundAsync(new
        {
            database = app.Slug,
            channelId,
            sender = Sender,
            messageId = "M1",
            kind = "text",
            text = "What is the total?",
            timestamp = 1754300000,
        });
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        await Bridge.WaitUntilAsync(() => Bridge.SentMessages.Count > 0, "the agent reply");

        var request = Assert.Single(Router.Requests);
        Assert.Equal(app.Slug, request.Database);
        Assert.Equal(agentId, request.AgentId);
        Assert.Equal(Channel.IdPrefix + channelId, request.ChannelId);
        Assert.Equal("What is the total?", request.Prompt);
        Assert.Matches(new Regex($"^chats/wa/{channelId}/48123456789/\\d{{4}}-\\d{{2}}-\\d{{2}}$"), request.ConversationId);

        var sent = Assert.Single(Bridge.SentMessages);
        Assert.Equal((app.Slug, channelId, Sender), (sent.Database, sent.ChannelId, sent.To));
        Assert.Equal("Hello from the fake agent.", sent.Text);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Same_sender_reuses_the_conversation_and_another_sender_gets_a_new_one()
    {
        await using var app = await NewAppAsync();
        var (_, channelId) = await SeedConnectedChannelAsync(app);

        await PostInboundAsync(Inbound(app.Slug, channelId, Sender, "first"));
        await PostInboundAsync(Inbound(app.Slug, channelId, Sender, "second"));
        await PostInboundAsync(Inbound(app.Slug, channelId, "48987654321@s.whatsapp.net", "third"));

        await Bridge.WaitUntilAsync(() => Bridge.SentMessages.Count == 3, "all three replies");

        // only per-sender ordering is guaranteed, so key the requests by prompt
        var byPrompt = Router.Requests.ToDictionary(r => r.Prompt, r => r.ConversationId);
        Assert.Equal(byPrompt["first"], byPrompt["second"]);
        Assert.NotEqual(byPrompt["first"], byPrompt["third"]);
        Assert.DoesNotContain("48987654321", byPrompt["first"]);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task User_identifier_parameter_is_bound_from_the_sender()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app,
            new AiAgentParameter("customerId", "scope"),
            new AiAgentParameter("UserIdentifier", "the whatsapp sender"));
        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.WhatsAppPersonal, agentId, null,
            Parameters: new Dictionary<string, string> { ["customerId"] = "customers/1" }));
        Bridge.SetStatus(app.Slug, created.ChannelId, "connected", phoneNumber: "+48111222333");

        await PostInboundAsync(Inbound(app.Slug, created.ChannelId, Sender, "hello"));
        await Bridge.WaitUntilAsync(() => Bridge.SentMessages.Count > 0, "the reply");

        var request = Assert.Single(Router.Requests);
        Assert.Equal("48123456789", request.Parameters["UserIdentifier"]);
        Assert.Equal("customers/1", request.Parameters["customerId"]);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Missing_or_wrong_token_is_unauthorized()
    {
        await using var app = await NewAppAsync();
        var (_, channelId) = await SeedConnectedChannelAsync(app);

        var missing = await PostInboundAsync(Inbound(app.Slug, channelId, Sender, "x"), token: null);
        Assert.Equal(HttpStatusCode.Unauthorized, missing.StatusCode);

        var wrong = await PostInboundAsync(Inbound(app.Slug, channelId, Sender, "x"), token: "wrong-token");
        Assert.Equal(HttpStatusCode.Unauthorized, wrong.StatusCode);

        Assert.Empty(Router.Requests);
        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Malformed_bodies_are_rejected()
    {
        await using var app = await NewAppAsync();

        var missingSender = await PostInboundAsync(new { database = app.Slug, channelId = "a", kind = "text", text = "x" });
        Assert.Equal(HttpStatusCode.BadRequest, missingSender.StatusCode);

        var unknownKind = await PostInboundAsync(new { database = app.Slug, channelId = "a", sender = Sender, kind = "carrier-pigeon" });
        Assert.Equal(HttpStatusCode.BadRequest, unknownKind.StatusCode);

        using var broken = new HttpRequestMessage(HttpMethod.Post, InboundRoute);
        broken.Headers.Add("X-Quill-Bridge-Token", QuillWhatsAppFixture.BridgeToken);
        broken.Content = new StringContent("{ not json", Encoding.UTF8, "application/json");
        var brokenResponse = await Host.Client.SendAsync(broken);
        Assert.Equal(HttpStatusCode.BadRequest, brokenResponse.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Unroutable_messages_are_dropped_without_calling_the_agent()
    {
        await using var app = await NewAppAsync();
        var (_, channelId) = await SeedConnectedChannelAsync(app);

        // unknown channel
        Assert.Equal(HttpStatusCode.Accepted,
            (await PostInboundAsync(Inbound(app.Slug, Guid.NewGuid().ToString("N"), Sender, "x"))).StatusCode);

        // unknown database
        Assert.Equal(HttpStatusCode.Accepted,
            (await PostInboundAsync(Inbound("no-such-db-" + Guid.NewGuid().ToString("N")[..8], channelId, Sender, "x"))).StatusCode);

        // disabled channel
        await app.UpdateChannelAsync(channelId, new UpdateChannelRequest(null, null, false));
        Assert.Equal(HttpStatusCode.Accepted,
            (await PostInboundAsync(Inbound(app.Slug, channelId, Sender, "x"))).StatusCode);

        // iframe channel
        var agentId = await SeedAgentAsync(app);
        var iframe = await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, agentId, []));
        Assert.Equal(HttpStatusCode.Accepted,
            (await PostInboundAsync(Inbound(app.Slug, iframe.ChannelId, Sender, "x"))).StatusCode);

        await Task.Delay(200);
        Assert.Empty(Router.Requests);
        Assert.Empty(Bridge.SentMessages);

        await app.UpdateChannelAsync(channelId, new UpdateChannelRequest(null, null, true));
        await app.DeleteChannelAsync(channelId);
        await app.DeleteChannelAsync(iframe.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Unsupported_kind_gets_the_fallback_reply_without_an_agent_call()
    {
        await using var app = await NewAppAsync();
        var (_, channelId) = await SeedConnectedChannelAsync(app);

        await PostInboundAsync(new
        {
            database = app.Slug,
            channelId,
            sender = Sender,
            messageId = "M1",
            kind = "unsupported",
            text = (string?)null,
            timestamp = 1754300000,
        });

        await Bridge.WaitUntilAsync(() => Bridge.SentMessages.Count > 0, "the fallback reply");

        Assert.Empty(Router.Requests);
        Assert.Equal(WhatsAppInboundProcessor.UnsupportedKindReply, Assert.Single(Bridge.SentMessages).Text);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agent_failure_sends_an_apology()
    {
        await using var app = await NewAppAsync();
        var (_, channelId) = await SeedConnectedChannelAsync(app);
        Router.Failure = new InvalidOperationException("model exploded");

        await PostInboundAsync(Inbound(app.Slug, channelId, Sender, "hello"));
        await Bridge.WaitUntilAsync(() => Bridge.SentMessages.Count > 0, "the apology");

        Assert.Equal(WhatsAppInboundProcessor.ErrorReply, Assert.Single(Bridge.SentMessages).Text);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Clear_command_wipes_the_conversation_and_confirms()
    {
        await using var app = await NewAppAsync();
        var (_, channelId) = await SeedConnectedChannelAsync(app);

        var conversationId = WhatsAppConversationId.For(channelId, Sender, DateTime.UtcNow);
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            await session.StoreAsync(new ConversationPreview { ConversationId = conversationId },
                ConversationPreview.IdFor(conversationId));
            await session.SaveChangesAsync();
        }

        await PostInboundAsync(Inbound(app.Slug, channelId, Sender, "/clear"));
        await Bridge.WaitUntilAsync(
            () => Bridge.SentMessages.Any(m => m.Text == WhatsAppInboundProcessor.ConversationClearedReply),
            "the confirmation");

        Assert.Empty(Router.Requests);
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            Assert.Null(await session.LoadAsync<object>(conversationId));
            Assert.Null(await session.LoadAsync<object>(ConversationPreview.IdFor(conversationId)));
        }

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Messages_from_one_sender_are_handled_in_order()
    {
        await using var app = await NewAppAsync();
        var (_, channelId) = await SeedConnectedChannelAsync(app);
        Router.ChunkDelay = TimeSpan.FromMilliseconds(150);

        await PostInboundAsync(Inbound(app.Slug, channelId, Sender, "first"));
        await PostInboundAsync(Inbound(app.Slug, channelId, Sender, "second"));

        await Bridge.WaitUntilAsync(() => Bridge.SentMessages.Count == 2, "both replies");

        Assert.Equal(["first", "second"], Router.Requests.Select(r => r.Prompt).ToArray());

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Loopback_guard_accepts_local_and_test_connections_and_rejects_public_addresses()
    {
        Assert.True(WhatsAppEndpoints.IsLoopback(null));
        Assert.True(WhatsAppEndpoints.IsLoopback(IPAddress.Loopback));
        Assert.True(WhatsAppEndpoints.IsLoopback(IPAddress.IPv6Loopback));
        Assert.False(WhatsAppEndpoints.IsLoopback(IPAddress.Parse("10.0.0.5")));
        Assert.False(WhatsAppEndpoints.IsLoopback(IPAddress.Parse("172.17.0.1")));
        Assert.False(WhatsAppEndpoints.IsLoopback(IPAddress.Parse("2001:db8::1")));
    }

    private async Task<(string AgentId, string ChannelId)> SeedConnectedChannelAsync(QuillApp app)
    {
        var agentId = await SeedAgentAsync(app);
        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null));
        Bridge.SetStatus(app.Slug, created.ChannelId, "connected", phoneNumber: "+48111222333");
        return (agentId, created.ChannelId);
    }

    private static object Inbound(string database, string channelId, string sender, string text) => new
    {
        database,
        channelId,
        sender,
        messageId = "M-" + Guid.NewGuid().ToString("N")[..8],
        kind = "text",
        text,
        timestamp = 1754300000,
    };

    private async Task<HttpResponseMessage> PostInboundAsync(object payload, string? token = QuillWhatsAppFixture.BridgeToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, InboundRoute);
        if (token is not null)
            request.Headers.Add("X-Quill-Bridge-Token", token);
        request.Content = JsonContent.Create(payload);
        return await Host.Client.SendAsync(request);
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
