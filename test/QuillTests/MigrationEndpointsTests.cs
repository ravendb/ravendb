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
        Assert.Equal(3, JsonNode.Parse(MigrationSamples.SentSchemaJson(planner.LastBody))!["Tables"]!.AsArray().Count);
        Assert.Equal(MigrationService.DefaultStartPrompt, (string?)sent["Prompt"]);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_reply_without_gaps_or_open_questions_reaches_the_browser_without_them()
    {
        var planner = StubPlannerHandler.Replying(
            new ReplyFrame { Reply = "Orders it is." },
            new DoneFrame { ConversationId = ConversationId });
        await using var host = await NewMigrationHostAsync(planner);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationStart, new { slug = QuillHost.DefaultWizardSlug });

        var reply = (await ReadFramesAsync(resp))[0].AsObject();
        Assert.Equal("Orders it is.", (string?)reply["reply"]);
        Assert.False(reply.ContainsKey("gaps"));
        Assert.False(reply.ContainsKey("openQuestions"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Transient_errors_and_a_rejected_collection_reach_the_browser_with_located_errors_and_the_generated_config()
    {
        var issue = new ValidationIssue { Message = "its Patch reads $row.total", Path = "patch", Table = "public.orders", Column = "total" };
        var planner = StubPlannerHandler.Replying(
            new TransientCollectionErrorFrame { Collection = "Orders", Errors = [issue], Rejections = 1, MaxRejections = 2 },
            new RejectedCollectionFrame { Collection = "Orders", Config = MigrationSamples.ValidOrders(), Errors = [issue] },
            new DoneFrame { ConversationId = ConversationId });
        await using var host = await NewMigrationHostAsync(planner);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationStart, new { slug = QuillHost.DefaultWizardSlug });
        var frames = await ReadFramesAsync(resp);

        Assert.Equal(["transientCollectionError", "rejectedCollection", "done"], frames.Select(f => (string?)f["type"]));
        Assert.Equal(1, (int)frames[0]["rejections"]!);
        Assert.Equal(2, (int)frames[0]["maxRejections"]!);
        Assert.Equal("patch", (string?)frames[0]["errors"]![0]!["path"]);
        Assert.Equal("total", (string?)frames[1]["errors"]![0]!["column"]);
        Assert.Equal("Orders", (string?)frames[1]["config"]!["collectionName"]);

        var plan = await MigrationSamples.LoadPlanAsync(host.Config, QuillHost.DefaultWizardSlug);
        Assert.Empty(plan!.Entries);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Each_proposed_collection_reaches_the_browser_with_its_field_names()
    {
        var planner = StubPlannerHandler.Replying(
            new ProposalFrame
            {
                Collections =
                [
                    new()
                    {
                        Collection = "Orders",
                        RootTable = "orders",
                        Fields = [new ProposedField { Table = "public.orders", Column = "order_id", Property = "OrderId" }]
                    }
                ]
            },
            new DoneFrame { ConversationId = ConversationId });
        await using var host = await NewMigrationHostAsync(planner);
        await SeedDiscoveredSchemaAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationStart, new { slug = QuillHost.DefaultWizardSlug });
        var proposal = (await ReadFramesAsync(resp))[0].AsObject();

        Assert.False(proposal.ContainsKey("fields"));
        var field = proposal["collections"]![0]!["fields"]![0]!;
        Assert.Equal(("public.orders", "order_id", "OrderId"), ((string?)field["table"], (string?)field["column"], (string?)field["property"]));
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
        Assert.Equal("orders", (string?)Assert.Single(JsonNode.Parse(MigrationSamples.SentSchemaJson(planner.LastBody))!["Tables"]!.AsArray())!["SourceTableName"]);
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
    public async Task Ask_sends_the_plan_and_the_user_edits_without_a_schema_and_clears_them_on_done()
    {
        var planner = StubPlannerHandler.Replying(new DoneFrame { ConversationId = ConversationId });
        await using var host = await NewMigrationHostAsync(planner);
        var plan = MigrationSamples.Plan(ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));
        plan.SelectedTables = [new SelectedSourceTable("orders", "public")];
        plan.PendingUserEdits = [new UserEdit("Customers", UserEditKind.Removed), new UserEdit("Sales", UserEditKind.Renamed, "Orders")];
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
        Assert.Null(sent["Schema"]);
        Assert.Null(sent["SchemaGzip"]);
        Assert.Equal(["Orders"], sent["Plan"]!["Entries"]!.AsArray().Select(e => (string?)e!["Collection"]));
        var edits = sent["UserEdits"]!.AsArray();
        Assert.Equal(["Customers", "Sales"], edits.Select(e => (string?)e!["Collection"]));
        Assert.Equal(["Removed", "Renamed"], edits.Select(e => (string?)e!["Kind"]));
        Assert.Equal("Orders", (string?)edits[1]!["PreviousName"]);

        var stored = await MigrationSamples.LoadPlanAsync(host.Config, QuillHost.DefaultWizardSlug);
        Assert.Empty(stored!.PendingUserEdits);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Ask_with_only_a_selection_relays_it_without_a_prompt()
    {
        var planner = StubPlannerHandler.Replying(new DoneFrame { ConversationId = ConversationId });
        await using var host = await NewMigrationHostAsync(planner);
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAsk, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId,
            selectedAreas = new[] { "Sales" },
            selectedCollections = new[] { "Customers" }
        });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        await ReadFramesAsync(resp);
        var sent = JsonNode.Parse(planner.LastBody)!;
        Assert.Equal(["Sales"], sent["SelectedAreas"]!.AsArray().Select(a => (string?)a));
        Assert.Equal(["Customers"], sent["SelectedCollections"]!.AsArray().Select(c => (string?)c));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Ask_without_a_prompt_or_a_selection_is_refused()
    {
        var planner = StubPlannerHandler.Replying();
        await using var host = await NewMigrationHostAsync(planner);
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAsk, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId,
            prompt = " ",
            selectedCollections = new[] { " " }
        });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Null(planner.LastPath);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Deleting_a_collection_takes_it_out_of_the_plan_and_queues_the_removal_for_the_planner()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        var plan = MigrationSamples.Plan(ConversationId, Entry("Orders", MigrationSamples.ValidOrders()), Entry("Customers", Customers()));
        await MigrationSamples.SeedDiscoveredSchemaAsync(host.Config, QuillHost.DefaultWizardSlug, plan);

        var resp = await EditAsync(host, "orders", config: null);

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var stored = await MigrationSamples.LoadPlanAsync(host.Config, QuillHost.DefaultWizardSlug);
        Assert.Equal(["Customers"], stored!.Entries.Select(e => e.Collection));
        Assert.Equal([new UserEdit("orders", UserEditKind.Removed)], stored.PendingUserEdits);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Deleting_a_collection_the_planner_only_proposed_queues_the_removal_for_the_planner()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));

        var resp = await EditAsync(host, "Products", config: null);

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var stored = await MigrationSamples.LoadPlanAsync(host.Config, QuillHost.DefaultWizardSlug);
        Assert.Equal(["Orders"], stored!.Entries.Select(e => e.Collection));
        Assert.Equal([new UserEdit("Products", UserEditKind.Removed)], stored.PendingUserEdits);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Editing_a_collection_replaces_its_mapping_bumps_its_version_and_queues_the_edit()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));
        var edited = MigrationSamples.ValidOrders();
        edited.Columns[1].Name = "PlacedAt";

        var resp = await EditAsync(host, "Orders", edited);

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var stored = await MigrationSamples.LoadPlanAsync(host.Config, QuillHost.DefaultWizardSlug);
        var entry = Assert.Single(stored!.Entries);
        Assert.Equal(2, entry.Version);
        Assert.Equal("because", entry.Rationale);
        Assert.Equal("PlacedAt", entry.Config!.Columns[1].Name);
        Assert.Equal([new UserEdit("Orders", UserEditKind.Edited)], stored.PendingUserEdits);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Adding_a_collection_the_plan_does_not_hold_registers_it_and_queues_the_addition()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));

        var resp = await EditAsync(host, "Customers", Customers());

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var stored = await MigrationSamples.LoadPlanAsync(host.Config, QuillHost.DefaultWizardSlug);
        Assert.Equal(1, stored!.Entries.Single(e => e.Collection == "Customers").Version);
        Assert.Equal([new UserEdit("Customers", UserEditKind.Added)], stored.PendingUserEdits);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Renaming_a_collection_re_keys_it_and_queues_the_rename()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));
        var renamed = MigrationSamples.ValidOrders();
        renamed.CollectionName = "Sales";

        var resp = await EditAsync(host, "Orders", renamed);

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var stored = await MigrationSamples.LoadPlanAsync(host.Config, QuillHost.DefaultWizardSlug);
        var entry = Assert.Single(stored!.Entries);
        Assert.Equal("Sales", entry.Collection);
        Assert.Equal("because", entry.Rationale);
        Assert.Equal([new UserEdit("Sales", UserEditKind.Renamed, "Orders")], stored.PendingUserEdits);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Renaming_a_collection_onto_another_one_is_refused()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()), Entry("Customers", Customers()));
        var renamed = MigrationSamples.ValidOrders();
        renamed.CollectionName = "Customers";

        var resp = await EditAsync(host, "Orders", renamed);

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        var stored = await MigrationSamples.LoadPlanAsync(host.Config, QuillHost.DefaultWizardSlug);
        Assert.Equal(["Customers", "Orders"], stored!.Entries.Select(e => e.Collection).Order());
        Assert.Empty(stored.PendingUserEdits);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_edit_cdc_sink_would_reject_is_422_with_its_errors_and_leaves_the_plan_alone()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));
        var edited = MigrationSamples.ValidOrders();
        edited.PrimaryKeyColumns = [];

        var resp = await EditAsync(host, "Orders", edited);

        Assert.Equal(HttpStatusCode.UnprocessableEntity, resp.StatusCode);
        var error = await resp.Content.ReadFromJsonAsync<ApiErrorResponse>();
        Assert.NotEmpty(error!.Errors!);
        var stored = await MigrationSamples.LoadPlanAsync(host.Config, QuillHost.DefaultWizardSlug);
        Assert.Equal(1, Assert.Single(stored!.Entries).Version);
        Assert.Empty(stored.PendingUserEdits);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_edit_joining_on_a_column_the_source_table_lacks_is_422()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));
        var edited = MigrationSamples.ValidOrders();
        var customer = MigrationSamples.ValidCustomer();
        customer.JoinColumns = ["buyer_ref"];
        edited.LinkedTables = [customer];

        var resp = await EditAsync(host, "Orders", edited);

        Assert.Equal(HttpStatusCode.UnprocessableEntity, resp.StatusCode);
        var error = await resp.Content.ReadFromJsonAsync<ApiErrorResponse>();
        Assert.Contains("buyer_ref", Assert.Single(error!.Errors!));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Deleting_a_collection_others_link_to_warns_about_the_dangling_links()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        var orders = MigrationSamples.ValidOrders();
        var customer = MigrationSamples.ValidCustomer();
        customer.JoinColumns = ["order_id"];
        orders.LinkedTables = [customer];
        await SeedPlanAsync(host, ConversationId, Entry("Orders", orders), Entry("Customers", Customers()));

        var resp = await EditAsync(host, "Customers", config: null);

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var body = await resp.Content.ReadFromJsonAsync<MigrationEditCollectionResponse>(ApiJson);
        var warning = Assert.Single(body!.Warnings);
        Assert.Contains("Orders", warning);
        Assert.Contains("Customers", warning);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Renaming_a_collection_others_link_to_warns_about_the_dangling_links()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        var orders = MigrationSamples.ValidOrders();
        var customer = MigrationSamples.ValidCustomer();
        customer.JoinColumns = ["order_id"];
        orders.LinkedTables = [customer];
        await SeedPlanAsync(host, ConversationId, Entry("Orders", orders), Entry("Customers", Customers()));
        var renamed = Customers();
        renamed.CollectionName = "Buyers";

        var resp = await EditAsync(host, "Customers", renamed);

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var body = await resp.Content.ReadFromJsonAsync<MigrationEditCollectionResponse>(ApiJson);
        Assert.Contains("Orders", Assert.Single(body!.Warnings));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Editing_while_a_turn_is_running_is_409_and_allowed_again_once_it_is_done()
    {
        var planner = new GatedPlannerHandler(MigrationSamples.Sse(new DoneFrame { ConversationId = ConversationId }));
        await using var host = await NewMigrationHostAsync(planner);
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));

        var ask = host.Client.PostAsJsonAsync(QuillRoutes.MigrationAsk, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId,
            prompt = "add customers"
        });
        await planner.Reached.Task.WaitAsync(TimeSpan.FromSeconds(30));

        var during = await EditAsync(host, "Orders", config: null);

        planner.Release.SetResult();
        await ReadFramesAsync(await ask);
        var after = await EditAsync(host, "Orders", config: null);

        Assert.Equal(HttpStatusCode.Conflict, during.StatusCode);
        Assert.Equal(HttpStatusCode.OK, after.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Editing_while_a_turn_is_still_checking_consent_is_409_so_the_turn_cannot_overwrite_the_edit()
    {
        var consent = new GatedConsentClient();
        var planner = StubPlannerHandler.Replying(new DoneFrame { ConversationId = ConversationId });
        await using var host = await NewMigrationHostAsync(planner, consentClient: consent);
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));

        var ask = host.Client.PostAsJsonAsync(QuillRoutes.MigrationAsk, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId,
            prompt = "add customers"
        });
        await consent.Reached.Task.WaitAsync(TimeSpan.FromSeconds(30));

        var during = await EditAsync(host, "Orders", config: null);
        consent.Release.SetResult();
        await ReadFramesAsync(await ask);

        Assert.Equal(HttpStatusCode.Conflict, during.StatusCode);
        var stored = await MigrationSamples.LoadPlanAsync(host.Config, QuillHost.DefaultWizardSlug);
        Assert.Equal(["Orders"], stored!.Entries.Select(e => e.Collection));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Editing_without_a_slug_is_400()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationEdit, new { conversationId = ConversationId, collection = "Orders" });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_edit_without_a_collection_name_in_its_config_takes_the_collection_it_edits()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));
        var edited = MigrationSamples.ValidOrders();
        edited.CollectionName = null;

        var resp = await EditAsync(host, "Orders", edited);

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var stored = await MigrationSamples.LoadPlanAsync(host.Config, QuillHost.DefaultWizardSlug);
        Assert.Equal("Orders", Assert.Single(stored!.Entries).Config!.CollectionName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_second_turn_while_one_is_running_is_409_instead_of_waiting()
    {
        var planner = new GatedPlannerHandler(MigrationSamples.Sse(new DoneFrame { ConversationId = ConversationId }));
        await using var host = await NewMigrationHostAsync(planner);
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));
        var body = new { slug = QuillHost.DefaultWizardSlug, conversationId = ConversationId, prompt = "add customers" };

        var first = host.Client.PostAsJsonAsync(QuillRoutes.MigrationAsk, body);
        await planner.Reached.Task.WaitAsync(TimeSpan.FromSeconds(30));
        var second = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAsk, body);
        planner.Release.SetResult();
        await ReadFramesAsync(await first);

        Assert.Equal(HttpStatusCode.Conflict, second.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Adding_a_collection_under_another_name_warns_about_nothing()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        var orders = MigrationSamples.ValidOrders();
        var customer = MigrationSamples.ValidCustomer();
        customer.JoinColumns = ["order_id"];
        customer.LinkedCollectionName = "Buyers";
        orders.LinkedTables = [customer];
        await SeedPlanAsync(host, ConversationId, Entry("Orders", orders));

        var resp = await EditAsync(host, "Buyers", Customers());

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var body = await resp.Content.ReadFromJsonAsync<MigrationEditCollectionResponse>(ApiJson);
        Assert.Empty(body!.Warnings);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_failed_turn_does_not_keep_edits_locked()
    {
        await using var host = await NewMigrationHostAsync(new StubPlannerHandler(HttpStatusCode.InternalServerError, "{}"));
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, ConversationId, Entry("Orders", MigrationSamples.ValidOrders()));

        await ReadFramesAsync(await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAsk, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId,
            prompt = "add customers"
        }));

        Assert.Equal(HttpStatusCode.OK, (await EditAsync(host, "Orders", config: null)).StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Editing_a_conversation_that_is_not_the_apps_current_one_is_refused()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await SeedDiscoveredSchemaAsync(host);
        await SeedPlanAsync(host, "MigrationChats/other", Entry("Orders", MigrationSamples.ValidOrders()));

        var resp = await EditAsync(host, "Orders", config: null);

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
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

    private static Task<HttpResponseMessage> EditAsync(QuillHost host, string collection, CdcSinkTableConfig? config) =>
        host.Client.PostAsJsonAsync(QuillRoutes.MigrationEdit, new
        {
            slug = QuillHost.DefaultWizardSlug,
            conversationId = ConversationId,
            collection,
            config
        });

    private Task<QuillHost> NewMigrationHostAsync(
        HttpMessageHandler planner,
        AiHelperStatus consent = AiHelperStatus.Success,
        IAiHelperClient? consentClient = null) =>
        NewHostAsync(configureServices: services =>
        {
            services.AddHttpClient<MigrationService>().ConfigurePrimaryHttpMessageHandler(() => planner);
            services.RemoveAll<IAiHelperClient>();
            services.AddSingleton(consentClient ?? new FakeConsentClient(consent));
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
