using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.AiHelper;
using Raven.Quill.AiHelper.Migration;
using Raven.Quill.AiHelper.Migration.Planning;
using Raven.Quill.Contracts;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class MigrationEndpointsTests(ITestOutputHelper output) : QuillTestBase(output)
{
    private const string ConversationId = "MigrationChats/abc";

    private static readonly JsonSerializerOptions ApiJson = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_streams_the_frames_the_planner_produced()
    {
        var planner = StubPlannerHandler.Replying(
            new ProposalFrame
            {
                Collections = [new() { Collection = "Orders", RootTable = "orders" }]
            },
            new CollectionFrame
            {
                Status = "registered",
                Collection = "Orders",
                Version = 1,
                Config = MigrationSamples.ValidOrders()
            },
            new DoneFrame { ConversationId = ConversationId });

        await using var host = await NewMigrationHostAsync(planner);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(
            QuillRoutes.MigrationStart, new { slug = QuillHost.DefaultWizardSlug });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal("application/x-ndjson", resp.Content.Headers.ContentType?.MediaType);

        var frames = await ReadFramesAsync(resp);

        Assert.Equal(["proposal", "collection", "done"], frames.Select(f => (string?)f["type"]));
        Assert.Equal("Orders", (string?)frames[1]["collection"]);
        Assert.Equal(ConversationId, (string?)frames[2]["conversationId"]);

        // the schema the planner was handed is the one discovery stored
        var sent = JsonNode.Parse(planner.LastBody)!;
        Assert.Equal(3, sent["Schema"]!["Tables"]!.AsArray().Count);
        Assert.Equal(MigrationService.DefaultStartPrompt, (string?)sent["Prompt"]);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_narrows_the_schema_to_the_selected_tables()
    {
        var planner = StubPlannerHandler.Replying(new DoneFrame { ConversationId = ConversationId });
        await using var host = await NewMigrationHostAsync(planner);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationStart, new
        {
            slug = QuillHost.DefaultWizardSlug,
            selectedTables = new[] { new { sourceTableName = "orders", sourceTableSchema = "public" } },
            prompt = "just orders please"
        });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);

        var sent = JsonNode.Parse(planner.LastBody)!;
        Assert.Equal("orders", (string?)Assert.Single(sent["Schema"]!["Tables"]!.AsArray())!["SourceTableName"]);
        Assert.Equal("just orders please", (string?)sent["Prompt"]);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_without_a_discovered_schema_is_refused_before_the_planner_is_called()
    {
        var planner = StubPlannerHandler.Replying();
        await using var host = await NewMigrationHostAsync(planner);

        var resp = await host.Client.PostAsJsonAsync(
            QuillRoutes.MigrationStart, new { slug = QuillHost.DefaultWizardSlug });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        var error = await resp.Content.ReadFromJsonAsync<ApiErrorResponse>();
        Assert.Contains("discover", error!.Error);
        Assert.Null(planner.LastPath);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_without_consent_is_401_and_the_planner_is_not_called()
    {
        var planner = StubPlannerHandler.Replying();
        await using var host = await NewMigrationHostAsync(planner, AiHelperStatus.ConsentRequired);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(
            QuillRoutes.MigrationStart, new { slug = QuillHost.DefaultWizardSlug });

        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
        Assert.Null(planner.LastPath);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_unreachable_ai_service_is_502_not_a_consent_problem()
    {
        var planner = StubPlannerHandler.Replying();
        await using var host = await NewMigrationHostAsync(planner, AiHelperStatus.InternalError);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(
            QuillRoutes.MigrationStart, new { slug = QuillHost.DefaultWizardSlug });

        // The operator cannot fix this by giving consent, so it must not be reported as consent.
        Assert.Equal(HttpStatusCode.BadGateway, resp.StatusCode);
        Assert.Null(planner.LastPath);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Ask_relays_to_the_planner_and_mirrors_into_the_stored_plan()
    {
        var planner = StubPlannerHandler.Replying(
            new CollectionFrame { Status = "registered", Collection = "Customers", Version = 1, Config = Customers() },
            new DoneFrame { ConversationId = ConversationId });
        await using var host = await NewMigrationHostAsync(planner);
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAsk, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId,
            prompt = "add customers"
        });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal(MigrationService.AskPath, planner.LastPath);
        Assert.Equal(["collection", "done"], (await ReadFramesAsync(resp)).Select(f => (string?)f["type"]));

        var plan = await MigrationSamples.LoadPlanAsync(host.Config, QuillHost.DefaultWizardSlug);
        Assert.Equal(["Customers", "Orders"], plan!.Entries.Select(e => e.Collection).Order());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Ask_refuses_a_conversation_that_is_not_the_apps_current_one()
    {
        var planner = StubPlannerHandler.Replying();
        await using var host = await NewMigrationHostAsync(planner);
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, "MigrationChats/other", Entry("Orders", MigrationSamples.ValidOrders()));

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAsk, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId,
            prompt = "go ahead with orders"
        });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        var error = await resp.Content.ReadFromJsonAsync<ApiErrorResponse>();
        Assert.Contains("no planning session", error!.Error);
        Assert.Null(planner.LastPath);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Ask_refuses_a_conversation_that_has_no_planning_session()
    {
        var planner = StubPlannerHandler.Replying();
        await using var host = await NewMigrationHostAsync(planner);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAsk, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = "MigrationChats/never-started",
            prompt = "go ahead with orders"
        });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Null(planner.LastPath);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Ask_sends_the_plan_the_user_removals_and_only_the_tables_selected_at_start()
    {
        var planner = StubPlannerHandler.Replying(new DoneFrame { ConversationId = ConversationId });
        await using var host = await NewMigrationHostAsync(planner);
        var plan = MigrationSamples.Plan(ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));
        plan.SelectedTables = [new SelectedSourceTable("orders", "public")];
        plan.PendingUserRemovals = ["Customers"];
        await MigrationSamples.SeedDiscoveredSchemaAsync(host.Config, QuillHost.DefaultWizardSlug, plan);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAsk, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId,
            prompt = "add a lines property"
        });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        await ReadFramesAsync(resp);

        var sent = JsonNode.Parse(planner.LastBody)!;
        Assert.Equal(["orders"], sent["Schema"]!["Tables"]!.AsArray().Select(t => (string?)t!["SourceTableName"]));
        Assert.Equal(["Orders"], sent["Plan"]!["Entries"]!.AsArray().Select(e => (string?)e!["Collection"]));
        Assert.Equal(["Customers"], sent["RemovedByUser"]!.AsArray().Select(c => (string?)c));

        var stored = await MigrationSamples.LoadPlanAsync(host.Config, QuillHost.DefaultWizardSlug);
        Assert.Empty(stored!.PendingUserRemovals);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Removing_a_collection_takes_it_out_of_the_plan_and_queues_it_for_the_planner()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        var plan = MigrationSamples.Plan(ConversationId, Entry("Orders", MigrationSamples.ValidOrders()), Entry("Customers", Customers()));
        await MigrationSamples.SeedDiscoveredSchemaAsync(host.Config, QuillHost.DefaultWizardSlug, plan);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationRemove, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId,
            collection = "orders"
        });

        Assert.Equal(HttpStatusCode.NoContent, resp.StatusCode);

        var stored = await MigrationSamples.LoadPlanAsync(host.Config, QuillHost.DefaultWizardSlug);
        Assert.Equal(["Customers"], stored!.Entries.Select(e => e.Collection));
        Assert.Equal(["orders"], stored.PendingUserRemovals);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Removing_a_collection_the_plan_does_not_hold_is_refused()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationRemove, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId,
            collection = "Products"
        });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        var error = await resp.Content.ReadFromJsonAsync<ApiErrorResponse>();
        Assert.Contains("no collection named Products", error!.Error);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_narrows_the_configuration_to_the_selected_collections()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId,
            Entry("Orders", MigrationSamples.ValidOrders()),
            Entry("Customers", Customers()));

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationApply, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId,
            collections = new[] { "Orders" }
        });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);

        var applied = await resp.Content.ReadFromJsonAsync<MigrationApplyResponse>(ApiJson);
        Assert.Equal("Orders", Assert.Single(applied!.Configuration!.Tables).CollectionName);

        // A collection left behind is reported rather than silently dropped.
        Assert.Contains("public.customers", applied.UnmappedTables);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_refuses_a_collection_the_plan_does_not_hold()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationApply, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId,
            collections = new[] { "Orders", "Invented" }
        });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        var error = await resp.Content.ReadFromJsonAsync<ApiErrorResponse>();
        Assert.Contains("Invented", error!.Error);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_reports_only_selected_tables_as_unmapped()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);

        var plan = MigrationSamples.Plan(ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));
        plan.SelectedTables = [new SelectedSourceTable("orders", "public"), new SelectedSourceTable("customers", "public")];
        await plan.SaveAsync(host.Config, QuillHost.DefaultWizardSlug);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationApply, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId
        });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);

        var applied = await resp.Content.ReadFromJsonAsync<MigrationApplyResponse>(ApiJson);
        Assert.Equal(["public.customers"], applied!.UnmappedTables);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_assembles_the_plan_persists_it_and_reports_uncovered_tables()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationApply, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId
        });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);

        var applied = await resp.Content.ReadFromJsonAsync<MigrationApplyResponse>(ApiJson);
        Assert.NotNull(applied);
        Assert.Equal("Orders", Assert.Single(applied.Configuration!.Tables).CollectionName);

        // only orders is mapped; the other two discovered tables are reported rather than dropped silently
        Assert.Equal(["public.customers", "public.audit_log"], applied.UnmappedTables);

        using var session = host.Config.OpenAsyncSession();
        var state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(QuillHost.DefaultWizardSlug));
        Assert.NotNull(state!.LastMapConfiguration);
        Assert.Equal("Orders", Assert.Single(state.LastMapConfiguration.Tables).CollectionName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_rejects_a_plan_that_does_not_assemble_into_a_valid_configuration()
    {
        var broken = MigrationSamples.ValidOrders();
        broken.PrimaryKeyColumns = [];

        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", broken));

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationApply, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId
        });

        Assert.Equal(HttpStatusCode.UnprocessableEntity, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_refuses_an_empty_plan()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationApply, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId
        });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    // -------------------------------------------------------------------

    private Task<QuillHost> NewMigrationHostAsync(
        StubPlannerHandler planner,
        AiHelperStatus consent = AiHelperStatus.Success) =>
        NewHostAsync(configureServices: services =>
        {
            services.AddHttpClient<MigrationService>().ConfigurePrimaryHttpMessageHandler(() => planner);
            services.RemoveAll<IAiHelperClient>();
            services.AddSingleton<IAiHelperClient>(new FakeConsentClient(consent));
        });

    private static async Task<List<JsonNode>> ReadFramesAsync(HttpResponseMessage resp)
    {
        var body = await resp.Content.ReadAsStringAsync();

        return body
            .Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(line => JsonNode.Parse(line)!)
            .ToList();
    }

    private static Task SeedDiscoveredSchemaAsync(QuillHost host) =>
        MigrationSamples.SeedDiscoveredSchemaAsync(host.Config, QuillHost.DefaultWizardSlug);

    private static Task SeedPlanAsync(QuillHost host, string conversationId, params PlanEntry[] entries) =>
        MigrationSamples.Plan(conversationId, entries).SaveAsync(host.Config, QuillHost.DefaultWizardSlug);

    private static PlanEntry Entry(string collection, CdcSinkTableConfig config) =>
        new() { Collection = collection, Version = 1, Rationale = "because", Config = config };

    private static CdcSinkTableConfig Customers()
    {
        var customers = MigrationSamples.ValidOrders();
        customers.CollectionName = "Customers";
        customers.SourceTableName = "customers";
        return customers;
    }
}
