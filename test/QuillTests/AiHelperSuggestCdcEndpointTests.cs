using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Nodes;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.Contracts;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillSuggestCdcCollection.Name)]
public class AiHelperSuggestCdcEndpointTests(ITestOutputHelper output, QuillAiHelperFixture fixture)
    : QuillAiHelperTestBase(output, fixture)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Returns_draft_from_internal_service_and_does_not_persist_map_config()
    {
        Mock.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig()));
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(new SuggestCdcRequest("shopping cart assistant"));
        Assert.Equal("Success", resp.Status);
        Assert.True(resp.Rationale.Count > 0);

        var config = resp.Configuration!;
        Assert.Equal("shop-cdc", config.Name);
        var table = config.Tables[0];
        Assert.Equal("Orders", table.CollectionName);
        Assert.Equal("Lines", table.EmbeddedTables[0].PropertyName);
        Assert.Equal("Customer", table.LinkedTables[0].PropertyName);

        // opaque mock-AI request body, not a Quill contract — stays JsonNode
        var sent = JsonNode.Parse(Mock.LastCdcRequestBody!)!;
        Assert.Equal("CdcConfigSetup", (string?)sent["OperationType"]);
        Assert.Null(sent["License"]);
        Assert.Null(sent["CertificateThumbprint"]);

        // raw: test-mapping is a separate endpoint, outside the suggest wrapper
        var testMapping = await Host.Client.PostAsJsonAsync(QuillRoutes.SetupTestMapping, new { sourceTableName = "orders" });
        Assert.Equal(HttpStatusCode.BadRequest, testMapping.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Requires_a_discovered_schema()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.SuggestCdcAsync(new SuggestCdcRequest("x")));
        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Null(Mock.LastCdcRequestBody); // internal service not called
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Blank_prompt_falls_back_to_premade_default()
    {
        Mock.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig()));
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(new SuggestCdcRequest("  "));
        Assert.Equal("Success", resp.Status);

        var sent = JsonNode.Parse(Mock.LastCdcRequestBody!)!;
        Assert.False(string.IsNullOrWhiteSpace((string?)sent["Prompt"]));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Surfaces_out_of_tokens_status_without_error()
    {
        Mock.CdcResponse = (429, "{}");
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(new SuggestCdcRequest("x"));
        Assert.Equal("OutOfTokens", resp.Status);
        Assert.Null(resp.Configuration);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Returns_422_when_internal_service_returns_invalid_config()
    {
        // structurally invalid config: no tables
        var invalid = new CdcSinkConfiguration { Name = "x", ConnectionStringName = "src" };
        Mock.CdcResponse = (200, AiHelperSamples.CdcEnvelope(invalid));
        await SeedDiscoveredSchemaAsync(Host);

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.SuggestCdcAsync(new SuggestCdcRequest("x")));
        Assert.Equal(HttpStatusCode.UnprocessableEntity, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Maps_401_to_invalid_credentials()
    {
        Mock.CdcResponse = (401, "{}");
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(new SuggestCdcRequest("x"));
        Assert.Equal("InvalidCredentials", resp.Status);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Surfaces_internal_error_when_ai_service_is_unreachable()
    {
        // own host: points at a dead address instead of the shared mock
        await using var host = await NewMockAiHostAsync("http://nonexistent.invalid");
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.SuggestCdcAsync(new SuggestCdcRequest("x"));
        Assert.Equal("InternalError", resp.Status);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Surfaces_consent_required_status_without_error()
    {
        // non-success status on a 200 must surface no configuration
        Mock.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig(), status: "ConsentRequired"));
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(new SuggestCdcRequest("x"));
        Assert.Equal("ConsentRequired", resp.Status);
        Assert.Null(resp.Configuration);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Maps_malformed_success_body_to_internal_error()
    {
        Mock.CdcResponse = (200, "not json");
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(new SuggestCdcRequest("x"));
        Assert.Equal("InternalError", resp.Status);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Consent_required_then_signs_consent_and_retries_successfully()
    {
        // mock mirrors the real consent gate
        Mock.RequireConsentForAssist = true;
        Mock.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig()));
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(new SuggestCdcRequest("shopping cart assistant"));
        Assert.Equal("Success", resp.Status);
        Assert.Equal("shop-cdc", resp.Configuration!.Name);

        Assert.Equal(1, Mock.GiveConsentCallCount);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Give_consent_rejection_surfaces_invalid_credentials()
    {
        Mock.RequireConsentForAssist = true;
        Mock.GiveConsentResponse = (401, "{\"Status\":\"InvalidCredentials\"}");
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(new SuggestCdcRequest("x"));
        Assert.Equal("InvalidCredentials", resp.Status);
        Assert.Equal(1, Mock.GiveConsentCallCount);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Post_retry_consent_required_is_surfaced_verbatim()
    {
        Mock.RequireConsentForAssist = true;
        // grant succeeds but gate stays closed: retry once, surface ConsentRequired, don't loop
        Mock.ConsentGrantHasNoEffect = true;
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(new SuggestCdcRequest("x"));
        Assert.Equal("ConsentRequired", resp.Status);
        Assert.Null(resp.Configuration);
        Assert.Equal(1, Mock.GiveConsentCallCount);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Slow_generation_within_timeout_returns_success()
    {
        Mock.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig()));
        Mock.AssistDelay = TimeSpan.FromSeconds(2);   // well within the shared host's 30s assist timeout
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(new SuggestCdcRequest("shopping cart assistant"));
        Assert.Equal("Success", resp.Status);
        Assert.Equal("shop-cdc", resp.Configuration!.Name);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Generation_exceeding_timeout_surfaces_internal_error()
    {
        // own host + own mock: needs a 1s assist timeout to trip, which the shared 30s host can't provide
        await using var mockAi = await MockAiApi.StartAsync();
        mockAi.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig()));
        mockAi.AssistDelay = TimeSpan.FromSeconds(10);

        await using var host = await NewMockAiHostAsync(mockAi.BaseAddress, TimeSpan.FromSeconds(1));
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.SuggestCdcAsync(new SuggestCdcRequest("x"));
        Assert.Equal("InternalError", resp.Status);
    }

    private static async Task SeedDiscoveredSchemaAsync(QuillHost host)
    {
        await host.SetupConnectAsync(new ConnectRequest("SqlClient", "invalid"));
        await host.SetupDiscoverAsync(new DiscoverRequest("SqlClient", "invalid"));
    }

    private Task<QuillHost> NewMockAiHostAsync(string aiApiUrl, TimeSpan? aiAssistTimeout = null) =>
        NewHostAsync(
            configure: opts =>
            {
                opts.AiApiUrl = aiApiUrl;
                if (aiAssistTimeout is not null)
                    opts.AiAssistTimeout = aiAssistTimeout.Value;
            },
            setupPackagePath: NewDataPath(forceCreateDir: true));
}
