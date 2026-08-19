using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Nodes;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
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

        var resp = await Host.SuggestCdcAsync(Request("shopping cart assistant", "orders"));
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
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.SuggestCdcAsync(Request("x", "orders")));
        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Null(Mock.LastCdcRequestBody); // internal service not called
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Sends_only_the_selected_tables_and_the_foreign_keys_between_them()
    {
        Mock.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig()));
        await SeedDiscoveredSchemaAsync(Host);

        await Host.SuggestCdcAsync(Request("x", "orders", "customers"));

        var sentTables = JsonNode.Parse(Mock.LastCdcRequestBody!)!["Schema"]!["Tables"]!.AsArray();
        Assert.Equal(
            new string?[] { "orders", "customers" },
            sentTables.Select(table => (string?)table!["SourceTableName"]).ToArray());

        // audit_log was left out, so the orders -> audit_log foreign key must not travel with orders
        var orders = sentTables.First(table => (string?)table!["SourceTableName"] == "orders")!;
        Assert.Equal(
            new string?[] { "customers" },
            orders["ForeignKeys"]!.AsArray().Select(key => (string?)key!["ReferencedTable"]).ToArray());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Matches_selected_table_identifiers_case_sensitively()
    {
        Mock.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig()));
        await SeedDiscoveredSchemaAsync(
            Host,
            SourceTable("orders", foreignKeysTo: ["Orders"]),
            SourceTable("Orders"));

        await Host.SuggestCdcAsync(Request("x", "orders"));

        var sentTables = JsonNode.Parse(Mock.LastCdcRequestBody!)!["Schema"]!["Tables"]!.AsArray();
        var sentTable = Assert.Single(sentTables);
        Assert.Equal("orders", (string?)sentTable!["SourceTableName"]);
        Assert.Empty(sentTable["ForeignKeys"]!.AsArray());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Requires_at_least_one_selected_table()
    {
        await SeedDiscoveredSchemaAsync(Host);

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.SuggestCdcAsync(Request("x")));
        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Null(Mock.LastCdcRequestBody);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Rejects_a_selection_the_discovered_schema_does_not_contain()
    {
        await SeedDiscoveredSchemaAsync(Host);

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.SuggestCdcAsync(Request("x", "dropped_table")));
        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Null(Mock.LastCdcRequestBody);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Blank_prompt_falls_back_to_premade_default()
    {
        Mock.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig()));
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(Request("  ", "orders"));
        Assert.Equal("Success", resp.Status);

        var sent = JsonNode.Parse(Mock.LastCdcRequestBody!)!;
        Assert.False(string.IsNullOrWhiteSpace((string?)sent["Prompt"]));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Surfaces_out_of_tokens_status_without_error()
    {
        Mock.CdcResponse = (429, "{}");
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(Request("x", "orders"));
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

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.SuggestCdcAsync(Request("x", "orders")));
        Assert.Equal(HttpStatusCode.UnprocessableEntity, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Returns_422_when_a_join_column_names_a_mapped_property_instead_of_a_source_column()
    {
        var renamed = AiHelperSamples.BuildCdcConfig();
        renamed.Tables[0].Columns.Add(new CdcColumnMapping { Column = "customer_id", Name = "CustomerId" });
        renamed.Tables[0].LinkedTables[0].JoinColumns = ["CustomerId"];
        Mock.CdcResponse = (200, AiHelperSamples.CdcEnvelope(renamed));
        await SeedDiscoveredSchemaAsync(Host);

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.SuggestCdcAsync(Request("x", "orders")));
        Assert.Equal(HttpStatusCode.UnprocessableEntity, ex.StatusCode);
        Assert.Contains("is a mapped property name", ex.Message);
        Assert.Contains("customer_id", ex.Message);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Returns_422_when_a_join_column_is_not_a_column_of_the_source_table()
    {
        var hallucinated = AiHelperSamples.BuildCdcConfig();
        hallucinated.Tables[0].LinkedTables[0].JoinColumns = ["buyer_ref"];
        Mock.CdcResponse = (200, AiHelperSamples.CdcEnvelope(hallucinated));
        await SeedDiscoveredSchemaAsync(Host);

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.SuggestCdcAsync(Request("x", "orders")));
        Assert.Equal(HttpStatusCode.UnprocessableEntity, ex.StatusCode);
        Assert.Contains("buyer_ref", ex.Message);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Maps_401_to_invalid_credentials()
    {
        Mock.CdcResponse = (401, "{}");
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(Request("x", "orders"));
        Assert.Equal("InvalidCredentials", resp.Status);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Surfaces_internal_error_when_ai_service_is_unreachable()
    {
        // own host: points at a dead address instead of the shared mock
        await using var host = await NewMockAiHostAsync("http://nonexistent.invalid");
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.SuggestCdcAsync(Request("x", "orders"));
        Assert.Equal("InternalError", resp.Status);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Surfaces_consent_required_status_without_error()
    {
        // non-success status on a 200 must surface no configuration
        Mock.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig(), status: "ConsentRequired"));
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(Request("x", "orders"));
        Assert.Equal("ConsentRequired", resp.Status);
        Assert.Null(resp.Configuration);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Maps_malformed_success_body_to_internal_error()
    {
        Mock.CdcResponse = (200, "not json");
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(Request("x", "orders"));
        Assert.Equal("InternalError", resp.Status);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Consent_required_is_surfaced_instead_of_being_granted()
    {
        // mock mirrors the real consent gate
        Mock.RequireConsentForAssist = true;
        Mock.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig()));
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(Request("shopping cart assistant", "orders"));

        // Accepting the AI service's terms is the operator's call, made in the assistant panel — the
        // wizard says what is missing rather than consenting for them.
        Assert.Equal("ConsentRequired", resp.Status);
        Assert.Null(resp.Configuration);
        Assert.Equal(0, Mock.GiveConsentCallCount);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Slow_generation_within_timeout_returns_success()
    {
        Mock.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig()));
        Mock.AssistDelay = TimeSpan.FromSeconds(2);   // well within the shared host's 30s assist timeout
        await SeedDiscoveredSchemaAsync(Host);

        var resp = await Host.SuggestCdcAsync(Request("shopping cart assistant", "orders"));
        Assert.Equal("Success", resp.Status);
        Assert.Equal("shop-cdc", resp.Configuration!.Name);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Generation_exceeding_timeout_surfaces_internal_error()
    {
        // own host + own mock: needs a 1s assist timeout to trip, which the shared 30s host can't provide
        await using var mockAi = await MockQuillServices.StartAsync();
        mockAi.CdcResponse = (200, AiHelperSamples.CdcEnvelope(AiHelperSamples.BuildCdcConfig()));
        mockAi.AssistDelay = TimeSpan.FromSeconds(10);

        await using var host = await NewMockAiHostAsync(mockAi.BaseAddress, TimeSpan.FromSeconds(1));
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.SuggestCdcAsync(Request("x", "orders"));
        Assert.Equal("InternalError", resp.Status);
    }

    /// The three-table schema every test's wizard state carries. Discovery can't reach a real source here,
    /// so the state document is written directly.
    private static async Task SeedDiscoveredSchemaAsync(QuillHost host, params CdcSinkSourceTable[] tables)
    {
        CdcSinkSourceTable[] discoveredTables = tables.Length > 0
            ? tables
            : [
                SourceTable("orders", foreignKeysTo: ["customers", "audit_log"]),
                SourceTable("customers"),
                SourceTable("audit_log"),
            ];

        using var session = host.Config.OpenAsyncSession();
        await session.StoreAsync(new WizardState
        {
            Provider = "SqlClient",
            LastDiscoveredSchema = new CdcSinkSourceSchema
            {
                CatalogName = "shop",
                HasPermissionToSetup = true,
                Tables = [.. discoveredTables],
            },
            LastDiscoverAt = DateTime.UtcNow,
        }, WizardState.DocumentIdFor(QuillHost.DefaultWizardSlug));
        await session.SaveChangesAsync();
    }

    private static CdcSinkSourceTable SourceTable(string name, string[]? foreignKeysTo = null) => new()
    {
        SourceTableSchema = "public",
        SourceTableName = name,
        IsCdcEnabled = true,
        PrimaryKeyColumns = ["id"],
        Columns =
        [
            new CdcSinkSourceColumn { Name = "id", NativeType = "int", IsPrimaryKey = true, IsCdcCapturable = true },
            new CdcSinkSourceColumn { Name = "customer_id", NativeType = "int", IsCdcCapturable = true },
        ],
        ForeignKeys = [.. (foreignKeysTo ?? []).Select(referenced => new CdcSinkSourceForeignKey
        {
            Columns = [$"{referenced}_id"],
            ReferencedSchema = "public",
            ReferencedTable = referenced,
            ReferencedColumns = ["id"],
        })],
    };

    private static SuggestCdcRequest Request(string intentPrompt, params string[] selectedTables) =>
        new(intentPrompt, [.. selectedTables.Select(table => new SelectedSourceTable(table, "public"))]);

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
