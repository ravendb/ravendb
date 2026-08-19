using System.Net.Http.Json;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.WhatsApp;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests.E2E;

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

        await app.DeleteChannelAsync(created.ChannelId);
        Assert.Empty(await app.GetChannelsAsync());
    }
}
