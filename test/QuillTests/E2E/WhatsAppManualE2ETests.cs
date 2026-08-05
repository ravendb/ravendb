using System.Net.Http.Json;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.WhatsApp;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests.E2E;

/// Runs against a real, locally running WhatsApp bridge (src/Raven.Quill.WhatsAppBridge), which in turn
/// talks to the real WhatsApp servers. Covers what MockWhatsAppBridge cannot: the real linked-device
/// handshake and QR issuance. Proving pairing needs no phone; the full reply loop does (see the comment
/// in the test body and the manual QA script in the bridge README).
///
/// To run: start the bridge (npm run build && npm start) with RAVEN_QUILL_WHATSAPP_DATA_DIR pointing at a
/// scratch dir containing a bridge-token file, then set the two env vars below to the bridge's URL and token.
public class WhatsAppManualE2ETests(ITestOutputHelper output) : QuillTestBase(output)
{
    private const string BridgeUrlVariable = "QUILL_WHATSAPP_E2E_BRIDGE_URL";
    private const string BridgeTokenVariable = "QUILL_WHATSAPP_E2E_BRIDGE_TOKEN";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Real_bridge_issues_a_qr_and_cleans_up()
    {
        var bridgeUrl = Environment.GetEnvironmentVariable(BridgeUrlVariable);
        var bridgeToken = Environment.GetEnvironmentVariable(BridgeTokenVariable);
        if (string.IsNullOrEmpty(bridgeUrl) || string.IsNullOrEmpty(bridgeToken))
            Assert.Skip($"Set {BridgeUrlVariable} and {BridgeTokenVariable} to a running bridge to run the live WhatsApp E2E.");

        await using var host = await NewHostAsync(configure: opts =>
        {
            opts.WhatsAppBridgeUrl = bridgeUrl;
            opts.WhatsAppBridgeToken = bridgeToken;
        });
        await using var app = await NewAppAsync(host);

        var agentId = "wa-live-agent";
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Live WhatsApp Agent",
            SystemPrompt = "You are a demo agent; answer briefly.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.WhatsAppPersonal, agentId, null));

        // a real QR (the linked-device payload) proves the bridge completed the
        // WhatsApp websocket handshake; no phone is needed up to this point
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (true)
        {
            var pairing = await host.Client.GetFromJsonAsync<WhatsAppPairingResponse>(
                QuillRoutes.WhatsAppPairing(app.Slug, created.ChannelId), QuillHttp.Json);

            if (pairing is { State: WhatsAppSessionState.Pairing, Qr: not null })
                break;

            Assert.True(DateTime.UtcNow < deadline,
                $"no QR within 30s; state: {pairing?.State}, lastError: {pairing?.LastError}");
            await Task.Delay(500);
        }

        var health = Assert.Single(await host.Client.GetFromJsonAsync<WhatsAppChannelHealthResponse[]>(
            QuillRoutes.WhatsAppHealth(app.Slug), QuillHttp.Json) ?? []);
        Assert.Equal(WhatsAppSessionState.Pairing, health.State);

        // To exercise the full reply loop manually: put a breakpoint (or a long Task.Delay) here,
        // scan the QR from the dashboard (or log it from the bridge) with a test phone, and message
        // the linked number. The seeded LLM connection string in QuillHost is unreachable, so a real
        // reply additionally needs the agent pointed at a live model.

        await app.DeleteChannelAsync(created.ChannelId);
        Assert.Empty(await app.GetChannelsAsync());
    }
}
