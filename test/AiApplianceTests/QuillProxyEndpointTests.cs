using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using AiApplianceTests.E2E.Fixtures;
using FastTests;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// Server-side tests for the bundled RavenDB Quill proxy (/quill/ai/assist). The appliance posts here;
/// the proxy injects the server's license + client-cert thumbprint and forwards to api.ravendb.net.
/// Ai.Quill.AssistApiUrl redirects that forward at an in-process mock so the real upstream is never hit.
public class QuillProxyEndpointTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance, LicenseRequired = true)]
    public async Task QuillAiAssist_injects_server_license_and_forwards_upstream()
    {
        await using var mockAi = await MockAiApi.StartAsync();
        mockAi.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig()));

        using var server = GetNewServer(new ServerCreationOptions
        {
            RegisterForDisposal = true,
            CustomSettings = new Dictionary<string, string>
            {
                // Make the server licensed so the proxy has a license to inject (LicenseRequired skips
                // when RAVEN_LICENSE is unset), and redirect the upstream forward at the mock.
                [RavenConfiguration.GetKey(x => x.Licensing.License)] = Environment.GetEnvironmentVariable("RAVEN_LICENSE")!,
                [RavenConfiguration.GetKey(x => x.Ai.QuillAssistApiUrl)] = mockAi.BaseAddress,
            }
        });

        // Creating a database bootstraps the server out of passive mode, which activates the license
        // from config so ServerStore.LoadLicense() (read by the proxy) returns it.
        using var store = GetDocumentStore(new Options { Server = server });

        using var http = new HttpClient();
        var resp = await http.PostAsync(
            $"{server.WebUrl}/quill/ai/assist",
            new StringContent("""{"OperationType":"CdcConfigSetup","Prompt":"x"}""", Encoding.UTF8, "application/json"));

        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        // The proxy must have injected the server-held license into the body it forwarded upstream;
        // the appliance no longer carries it. Thumbprint is null on this unsecured server.
        var sent = JsonNode.Parse(mockAi.LastCdcRequestBody!)!;
        Assert.Equal("CdcConfigSetup", (string?)sent["OperationType"]);
        Assert.NotNull(sent["License"]);
        Assert.NotNull((string?)sent["License"]!["Id"]);
        Assert.Null((string?)sent["CertificateThumbprint"]);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task QuillAiAssist_is_forbidden_when_ai_assistant_disabled()
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            RegisterForDisposal = true,
            CustomSettings = new Dictionary<string, string>
            {
                // Admin kill-switch: fires before any license check or upstream call.
                [RavenConfiguration.GetKey(x => x.Ai.DisableAiAssistant)] = "true",
            }
        });

        using var http = new HttpClient();
        var resp = await http.PostAsync(
            $"{server.WebUrl}/quill/ai/assist",
            new StringContent("""{"OperationType":"CdcConfigSetup","Prompt":"x"}""", Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.Forbidden, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task QuillAiAssist_returns_401_when_server_unlicensed()
    {
        await using var mockAi = await MockAiApi.StartAsync();

        using var server = GetNewServer(new ServerCreationOptions
        {
            RegisterForDisposal = true,
            CustomSettings = new Dictionary<string, string>
            {
                // No license in config and no database is created, so the server stays unlicensed and
                // ServerStore.LoadLicense() (read by the proxy) returns null. Point the upstream at the
                // mock to prove the proxy rejects with 401 before any forward (the appliance maps this
                // to InvalidCredentials, the behavior that moved here from the client).
                [RavenConfiguration.GetKey(x => x.Ai.QuillAssistApiUrl)] = mockAi.BaseAddress,
            }
        });

        using var http = new HttpClient();
        var resp = await http.PostAsync(
            $"{server.WebUrl}/quill/ai/assist",
            new StringContent("""{"OperationType":"CdcConfigSetup","Prompt":"x"}""", Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
        Assert.Null(mockAi.LastCdcRequestBody); // rejected before any upstream forward
    }
}
