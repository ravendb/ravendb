using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json.Nodes;
using AiApplianceTests.E2E.Fixtures;
using FastTests;
using Raven.AiAppliance.Hosting;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

public class AiHelperSuggestCdcEndpointTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Returns_draft_from_internal_service_and_does_not_persist_map_config()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        mockAi.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig()));

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();
        await SeedDiscoveredSchemaAsync(client);

        var resp = await client.PostAsJsonAsync("/api/setup/suggest/cdc", new { intentPrompt = "shopping cart assistant" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Equal("Success", (string?)node["status"]);
        Assert.True(node["rationale"]!.AsArray().Count > 0);

        // Nested structure survives the conventions round-trip (MockAiApi canonical JSON
        // -> client conventions deserialize -> object -> endpoint serialize).
        var table = node["configuration"]!["tables"]!.AsArray()[0]!;
        Assert.Equal("shop-cdc", (string?)node["configuration"]!["name"]);
        Assert.Equal("Orders", (string?)table["collectionName"]);
        Assert.Equal("Lines", (string?)table["embeddedTables"]![0]!["propertyName"]);
        Assert.Equal("Customer", (string?)table["linkedTables"]![0]!["propertyName"]);

        // OperationType still rides on the request (the proxy reads the exact enum name to route).
        // License + CertificateThumbprint are now injected by the bundled RavenDB /assistant/assist
        // proxy, so the appliance must NOT attach them itself.
        var sent = JsonNode.Parse(mockAi.LastCdcRequestBody!)!;
        Assert.Equal("CdcConfigSetup", (string?)sent["OperationType"]);
        Assert.Null(sent["License"]);
        Assert.Null(sent["CertificateThumbprint"]);

        // Generate-only: the suggest call must not persist a map configuration;
        // test-mapping has nothing to read back.
        var testMapping = await client.PostAsJsonAsync("/api/setup/test-mapping", new { sourceTableName = "orders" });
        Assert.Equal(HttpStatusCode.BadRequest, testMapping.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Requires_a_discovered_schema()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/api/setup/suggest/cdc", new { intentPrompt = "x" });
        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Null(mockAi.LastCdcRequestBody); // internal service not called
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Blank_prompt_falls_back_to_premade_default()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        mockAi.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig()));

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();
        await SeedDiscoveredSchemaAsync(client);

        // A blank intent prompt is now accepted; the endpoint substitutes a premade default.
        var resp = await client.PostAsJsonAsync("/api/setup/suggest/cdc", new { intentPrompt = "  " });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Equal("Success", (string?)node["status"]);

        // The AI received a non-empty prompt (the premade default), not the blank input.
        var sent = JsonNode.Parse(mockAi.LastCdcRequestBody!)!;
        Assert.False(string.IsNullOrWhiteSpace((string?)sent["Prompt"]));
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Surfaces_out_of_tokens_status_without_error()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        mockAi.CdcResponse = (429, "{}");

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();
        await SeedDiscoveredSchemaAsync(client);

        var resp = await client.PostAsJsonAsync("/api/setup/suggest/cdc", new { intentPrompt = "x" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Equal("OutOfTokens", (string?)node["status"]);
        Assert.True(node["configuration"] is null);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Returns_422_when_internal_service_returns_invalid_config()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();

        // Success envelope with a structurally invalid config (no tables); defensive re-validation rejects it.
        var invalid = new CdcSinkConfiguration { Name = "x", ConnectionStringName = "src" };
        mockAi.CdcResponse = (200, AiHelperSamples.CdcEnvelope(invalid));

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();
        await SeedDiscoveredSchemaAsync(client);

        var resp = await client.PostAsJsonAsync("/api/setup/suggest/cdc", new { intentPrompt = "x" });
        Assert.Equal(HttpStatusCode.UnprocessableEntity, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Maps_401_to_invalid_credentials()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        mockAi.CdcResponse = (401, "{}");

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();
        await SeedDiscoveredSchemaAsync(client);

        var resp = await client.PostAsJsonAsync("/api/setup/suggest/cdc", new { intentPrompt = "x" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Equal("InvalidCredentials", (string?)node["status"]);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Surfaces_internal_error_when_ai_service_is_unreachable()
    {
        var store = GetDocumentStore();

        // Point AiApiUrl at an unresolvable host so the HTTP send throws. The client
        // must surface that as InternalError, not a 500.
        using var factory = NewApplianceFactory(store, "http://nonexistent.invalid");
        var client = factory.CreateClient();
        await SeedDiscoveredSchemaAsync(client);

        var resp = await client.PostAsJsonAsync("/api/setup/suggest/cdc", new { intentPrompt = "x" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Equal("InternalError", (string?)node["status"]);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Surfaces_consent_required_status_without_error()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        // 200 with a ConsentRequired envelope status: the appliance must pass the status through
        // and surface no configuration for a non-success status.
        mockAi.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig(), status: "ConsentRequired"));

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();
        await SeedDiscoveredSchemaAsync(client);

        var resp = await client.PostAsJsonAsync("/api/setup/suggest/cdc", new { intentPrompt = "x" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Equal("ConsentRequired", (string?)node["status"]);
        Assert.True(node["configuration"] is null);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Maps_malformed_success_body_to_internal_error()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        // 200 but an unreadable body. The client must collapse the parse failure to
        // InternalError rather than letting it escape as a 500.
        mockAi.CdcResponse = (200, "not json");

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();
        await SeedDiscoveredSchemaAsync(client);

        var resp = await client.PostAsJsonAsync("/api/setup/suggest/cdc", new { intentPrompt = "x" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Equal("InternalError", (string?)node["status"]);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Consent_required_then_signs_consent_and_retries_successfully()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        // The real service 401s with ConsentRequired until consent is signed; mirror that gate.
        mockAi.RequireConsentForAssist = true;
        mockAi.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig()));

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();
        await SeedDiscoveredSchemaAsync(client);

        var resp = await client.PostAsJsonAsync("/api/setup/suggest/cdc", new { intentPrompt = "shopping cart assistant" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Equal("Success", (string?)node["status"]);
        Assert.Equal("shop-cdc", (string?)node["configuration"]!["name"]);

        // Consent was signed exactly once, then the assist was retried and succeeded.
        Assert.Equal(1, mockAi.GiveConsentCallCount);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Give_consent_rejection_surfaces_invalid_credentials()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        mockAi.RequireConsentForAssist = true;
        // give-consent's own license check rejects the license -> a genuine credential problem,
        // surfaced as InvalidCredentials rather than looping on consent.
        mockAi.GiveConsentResponse = (401, "{\"Status\":\"InvalidCredentials\"}");

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();
        await SeedDiscoveredSchemaAsync(client);

        var resp = await client.PostAsJsonAsync("/api/setup/suggest/cdc", new { intentPrompt = "x" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Equal("InvalidCredentials", (string?)node["status"]);
        Assert.Equal(1, mockAi.GiveConsentCallCount);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Post_retry_consent_required_is_surfaced_verbatim()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        mockAi.RequireConsentForAssist = true;
        // give-consent succeeds but the gate stays closed (propagation lag / thumbprint mismatch):
        // the client retries exactly once and surfaces ConsentRequired honestly — no masking as
        // InvalidCredentials, no consent loop.
        mockAi.ConsentGrantHasNoEffect = true;

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();
        await SeedDiscoveredSchemaAsync(client);

        var resp = await client.PostAsJsonAsync("/api/setup/suggest/cdc", new { intentPrompt = "x" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Equal("ConsentRequired", (string?)node["status"]);
        Assert.True(node["configuration"] is null);
        Assert.Equal(1, mockAi.GiveConsentCallCount);
    }

    private static async Task SeedDiscoveredSchemaAsync(HttpClient client)
    {
        // Discovering with an invalid connection string persists a non-null error schema,
        // enough to satisfy the "schema present" gate without a real source DB.
        var resp = await client.PostAsJsonAsync(
            "/api/setup/discover",
            new { provider = "SqlClient", connectionString = "invalid" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
    }

    private ApplianceWebApplicationFactory NewApplianceFactory(IDocumentStore store, string aiApiUrl)
    {
        var setupPath = NewDataPath(forceCreateDir: true);

        return new ApplianceWebApplicationFactory(
            licenseApiUrl: "http://unused-in-unit-tests",
            setupPackagePath: setupPath,
            applianceStore: store,
            configureOptions: opts =>
            {
                opts.ConfigDatabase = store.Database;
                opts.AiApiUrl = aiApiUrl;
            });
    }
}
