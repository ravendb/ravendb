using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using QuillTests.E2E.Fixtures;
using Raven.Quill.AiHelper;
using Raven.Quill.AiHelper.Migration;
using Raven.Quill.Contracts;
using Raven.Quill.Wizard;
using Raven.Server.Documents.AI.AiAssistant.Handlers;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class MigrationEndpointsTests(ITestOutputHelper output) : QuillTestBase(output)
{
    private const string Slug = QuillHost.DefaultWizardSlug;

    private const string PlanId = MigrationSamples.PlanId;

    private static readonly JsonSerializerOptions ApiJson = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Proposal_start_streams_the_frames_the_planner_produced()
    {
        var planner = StubPlannerHandler.Replying(
            new ProposalFrame { Proposal = new MigrationProposal { Areas = [new() { Area = "Sales", Collections = [new() { Collection = "Orders", RootTable = "orders" }] }] } },
            new CollectionFrame { Status = "registered", Collection = "Orders", Version = 1, Config = MigrationSamples.ValidOrders() },
            MigrationSamples.Done());

        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationProposalStart, new { slug = Slug });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal("application/x-ndjson", resp.Content.Headers.ContentType?.MediaType);

        var frames = await ReadFramesAsync(resp);

        Assert.Equal(["proposal", "collection", "done"], frames.Select(f => (string?)f["type"]));
        Assert.Equal("Orders", (string?)frames[0]["proposal"]!["areas"]![0]!["collections"]![0]!["collection"]);
        Assert.Equal("Orders", (string?)frames[1]["collection"]);
        Assert.Equal(PlanId, (string?)frames[2]["planId"]);

        var sent = JsonNode.Parse(planner.LastBody)!;
        Assert.Equal(3, JsonNode.Parse(MigrationSamples.SentSchemaJson(planner.LastBody))!["Tables"]!.AsArray().Count);
        Assert.Equal(MigrationService.DefaultStartPrompt, (string?)sent["Prompt"]);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_reply_without_gaps_or_open_questions_reaches_the_browser_without_them()
    {
        var planner = StubPlannerHandler.Replying(new ReplyFrame { Reply = "Orders it is." }, MigrationSamples.Done());
        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationProposalStart, new { slug = Slug });

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
            new TransientCollectionErrorFrame { Collection = "Orders", Errors = [issue] },
            new RejectedCollectionFrame { Collection = "Orders", Config = MigrationSamples.ValidOrders(), Errors = [issue] },
            MigrationSamples.Done(PlanId + "/area/Sales"));
        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host, PlanId);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAreaStart, new { slug = Slug, area = "Sales" });
        var frames = await ReadFramesAsync(resp);

        Assert.Equal(["transientCollectionError", "rejectedCollection", "done"], frames.Select(f => (string?)f["type"]));
        Assert.Equal("patch", (string?)frames[0]["errors"]![0]!["path"]);
        Assert.Equal("total", (string?)frames[1]["errors"]![0]!["column"]);
        Assert.Equal("Orders", (string?)frames[1]["config"]!["collectionName"]);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Each_proposed_collection_reaches_the_browser_with_its_field_names()
    {
        var planner = StubPlannerHandler.Replying(
            new ProposalFrame
            {
                Proposal = new MigrationProposal
                {
                    Areas =
                    [
                        new()
                        {
                            Area = "Sales",
                            Collections =
                            [
                                new()
                                {
                                    Collection = "Orders",
                                    RootTable = "orders",
                                    Fields = [new ProposedField { Table = "public.orders", Column = "order_id", Property = "OrderId" }]
                                }
                            ]
                        }
                    ]
                }
            },
            MigrationSamples.Done());
        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationProposalStart, new { slug = Slug });
        var proposal = (await ReadFramesAsync(resp))[0]["proposal"]!.AsObject();

        Assert.False(proposal.ContainsKey("fields"));
        var field = proposal["areas"]![0]!["collections"]![0]!["fields"]![0]!;
        Assert.Equal(("public.orders", "order_id", "OrderId"), ((string?)field["table"], (string?)field["column"], (string?)field["property"]));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_narrows_the_schema_to_the_selected_tables()
    {
        var planner = StubPlannerHandler.Replying(MigrationSamples.Done());
        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationProposalStart, new
        {
            slug = Slug,
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

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationProposalStart, new { slug = Slug });

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
        await SeedAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationProposalStart, new { slug = Slug });

        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
        Assert.Null(planner.LastPath);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_unreachable_ai_service_is_502_not_a_consent_problem()
    {
        var planner = StubPlannerHandler.Replying();
        await using var host = await NewMigrationHostAsync(planner, AiHelperStatus.InternalError);
        await SeedAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationProposalStart, new { slug = Slug });

        // The operator cannot fix this by giving consent, so it must not be reported as consent.
        Assert.Equal(HttpStatusCode.BadGateway, resp.StatusCode);
        Assert.Null(planner.LastPath);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Area_start_relays_to_the_area_agent_with_the_plan_id()
    {
        var planner = StubPlannerHandler.Replying(
            new CollectionFrame { Status = "registered", Collection = "Customers", Version = 1, Config = MigrationSamples.ValidCustomers() },
            MigrationSamples.Done(PlanId + "/area/Sales"));
        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host, PlanId);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAreaStart, new { slug = Slug, area = "Sales" });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal(MigrationSamples.ServerPath(nameof(AiAssistantHandler.MigrationAreaStart)), planner.LastPath);
        Assert.Equal(["collection", "done"], (await ReadFramesAsync(resp)).Select(f => (string?)f["type"]));

        var sent = JsonNode.Parse(planner.LastBody)!;
        Assert.Equal(PlanId, (string?)sent["PlanId"]);
        Assert.Equal("Sales", (string?)sent["Area"]);
        Assert.Null(sent["Prompt"]);
        Assert.Null(sent["SchemaGzip"]);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(QuillRoutes.MigrationProposalStart, "{\"prompt\":\"plan it\"}")]
    [InlineData(QuillRoutes.MigrationProposalAsk, "{\"slug\":\"" + Slug + "\",\"prompt\":\" \"}")]
    [InlineData(QuillRoutes.MigrationAreaStart, "{\"slug\":\"" + Slug + "\"}")]
    [InlineData(QuillRoutes.MigrationAreaAsk, "{\"slug\":\"" + Slug + "\",\"area\":\"Sales\"}")]
    [InlineData(QuillRoutes.MigrationPlanGet, "{}")]
    [InlineData(QuillRoutes.MigrationPlanSave, "{\"slug\":\"" + Slug + "\",\"changes\":[]}")]
    [InlineData(QuillRoutes.MigrationApply, "{}")]
    public async Task A_request_missing_its_slug_area_prompt_or_changes_is_400(string route, string body)
    {
        var planner = StubPlannerHandler.Replying(MigrationSamples.Done());
        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host, PlanId);

        var resp = await host.Client.PostAsync(route, new StringContent(body, Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Null(planner.LastPath);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_turn_before_the_proposal_is_400()
    {
        var planner = StubPlannerHandler.Replying(MigrationSamples.Done());
        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAreaStart, new { slug = Slug, area = "Sales" });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        var error = await resp.Content.ReadFromJsonAsync<ApiErrorResponse>();
        Assert.Contains("proposal/start", error!.Error);
        Assert.Null(planner.LastPath);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Plan_get_answers_the_apis_plan_in_camel_case()
    {
        var planner = new StubPlannerHandler(HttpStatusCode.OK, MigrationSamples.PlanGetBody(MigrationSamples.Entry(MigrationSamples.ValidOrders())));
        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host, PlanId);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationPlanGet, new { slug = Slug });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var plan = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!["plan"]!;
        Assert.Equal(PlanId + "/plan", (string?)plan["id"]);
        var orders = plan["entries"]!["Orders"]!.AsObject();
        Assert.Equal("Registered", (string?)orders["status"]);
        Assert.Equal("Orders", (string?)orders["config"]!["collectionName"]);
        Assert.False(orders.ContainsKey("collection"));
        Assert.Equal(PlanId, (string?)JsonNode.Parse(planner.LastBody)!["PlanId"]);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Plan_get_on_a_plan_the_api_does_not_know_is_400()
    {
        await using var host = await NewMigrationHostAsync(new StubPlannerHandler(HttpStatusCode.NotFound, "{\"Status\":\"InvalidData\"}"));
        await SeedAsync(host, PlanId);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationPlanGet, new { slug = Slug });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Contains("No plan found", (await resp.Content.ReadFromJsonAsync<ApiErrorResponse>())!.Error);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(HttpStatusCode.OK)]
    [InlineData(HttpStatusCode.UnprocessableEntity)]
    public async Task Plan_save_answers_the_apis_outcome_in_camel_case_with_its_status(HttpStatusCode status)
    {
        ValidationIssue[] issues = status == HttpStatusCode.OK ? [] : [new ValidationIssue { Message = "no such column", Path = "changes[0].columns[2].column" }];
        var planner = new StubPlannerHandler(status, MigrationSamples.PlanSaveBody(issues, "Loans links to Clients"));
        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host, PlanId);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationPlanSave, new
        {
            slug = Slug,
            changes = new[] { new { kind = "Delete", area = "Sales", collection = "Orders" } }
        }, ApiJson);

        Assert.Equal(status, resp.StatusCode);
        var outcome = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Equal(issues.Length, outcome["issues"]!.AsArray().Count);
        Assert.Equal("Loans links to Clients", (string?)outcome["warnings"]![0]);
        Assert.Equal("Delete", (string?)JsonNode.Parse(planner.LastBody)!["Changes"]![0]!["Kind"]);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Plan_save_is_409_while_a_turn_runs_and_relayed_once_it_is_done()
    {
        var planner = new GatedPlannerHandler(MigrationSamples.PlanSaveBody([]));
        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host, PlanId);

        var turn = host.Client.PostAsJsonAsync(QuillRoutes.MigrationAreaStart, new { slug = Slug, area = "Sales" });
        await planner.WaitForArrivalsAsync(1);

        var during = await SaveAsync(host);

        planner.Release.SetResult();
        await ReadFramesAsync(await turn);
        var after = await SaveAsync(host);

        Assert.Equal(HttpStatusCode.Conflict, during.StatusCode);
        Assert.Equal(HttpStatusCode.OK, after.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_turn_is_409_while_a_save_runs_and_reaches_the_planner_once_it_is_done()
    {
        var planner = new GatedPlannerHandler(MigrationSamples.PlanSaveBody([]));
        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host, PlanId);

        var save = SaveAsync(host);
        await planner.WaitForArrivalsAsync(1);

        var during = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAreaStart, new { slug = Slug, area = "Sales" });

        planner.Release.SetResult();
        Assert.Equal(HttpStatusCode.OK, (await save).StatusCode);
        var after = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAreaStart, new { slug = Slug, area = "Sales" });
        await planner.WaitForArrivalsAsync(2);

        Assert.Equal(HttpStatusCode.Conflict, during.StatusCode);
        Assert.Contains("the plan is being saved", await during.Content.ReadAsStringAsync());
        Assert.Equal(HttpStatusCode.OK, after.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Plan_save_while_a_turn_is_still_checking_consent_is_409()
    {
        var consent = new GatedConsentClient();
        var planner = StubPlannerHandler.Replying(MigrationSamples.Done(PlanId + "/area/Sales"));
        await using var host = await NewMigrationHostAsync(planner, consentClient: consent);
        await SeedAsync(host, PlanId);

        var turn = host.Client.PostAsJsonAsync(QuillRoutes.MigrationAreaStart, new { slug = Slug, area = "Sales" });
        await consent.Reached.Task.WaitAsync(TimeSpan.FromSeconds(30));

        var during = await SaveAsync(host);
        consent.Release.SetResult();
        await ReadFramesAsync(await turn);

        Assert.Equal(HttpStatusCode.Conflict, during.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Stop_ends_the_running_area_turn_and_answers_204()
    {
        var planner = new GatedPlannerHandler(MigrationSamples.Sse(MigrationSamples.Done(PlanId + "/area/Sales")));
        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host, PlanId);

        var turn = host.Client.PostAsJsonAsync(QuillRoutes.MigrationAreaStart, new { slug = Slug, area = "Sales" });
        await planner.WaitForArrivalsAsync(1);

        var stop = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationStop, new { slug = Slug, agent = "Area", area = "Sales" });

        Assert.Equal(HttpStatusCode.NoContent, stop.StatusCode);
        var frame = Assert.Single(await ReadFramesAsync(await turn.WaitAsync(TimeSpan.FromSeconds(30))));
        Assert.Equal("The turn was stopped.", (string?)frame["message"]);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("{\"slug\":\"" + Slug + "\",\"agent\":\"Area\"}")]
    [InlineData("{\"slug\":\"" + Slug + "\",\"agent\":\"Proposal\",\"area\":\"Sales\"}")]
    [InlineData("{\"slug\":\"" + Slug + "\"}")]
    [InlineData("{\"slug\":\"" + Slug + "\",\"agent\":2,\"area\":\"Sales\"}")]
    public async Task A_stop_without_a_known_agent_or_with_an_area_that_does_not_fit_it_is_400(string body)
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());

        var resp = await host.Client.PostAsync(QuillRoutes.MigrationStop, new StringContent(body, Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Two_areas_run_at_once_and_a_second_turn_on_a_held_area_is_409()
    {
        var planner = new GatedPlannerHandler(MigrationSamples.Sse(MigrationSamples.Done(PlanId + "/area/x")));
        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host, PlanId);

        var sales = host.Client.PostAsJsonAsync(QuillRoutes.MigrationAreaStart, new { slug = Slug, area = "Sales" });
        var lending = host.Client.PostAsJsonAsync(QuillRoutes.MigrationAreaStart, new { slug = Slug, area = "Lending" });
        await planner.WaitForArrivalsAsync(2);

        var again = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAreaStart, new { slug = Slug, area = "Sales" });

        planner.Release.SetResult();
        Assert.Equal(HttpStatusCode.Conflict, again.StatusCode);
        Assert.Equal(["done"], (await ReadFramesAsync(await sales)).Select(f => (string?)f["type"]));
        Assert.Equal(["done"], (await ReadFramesAsync(await lending)).Select(f => (string?)f["type"]));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_failed_turn_does_not_keep_the_lock()
    {
        await using var host = await NewMigrationHostAsync(new StubPlannerHandler(HttpStatusCode.InternalServerError, "{}"));
        await SeedAsync(host, PlanId);

        var first = await ReadFramesAsync(await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAreaStart, new { slug = Slug, area = "Sales" }));
        var second = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationAreaStart, new { slug = Slug, area = "Sales" });

        Assert.Equal("error", (string?)Assert.Single(first)["type"]);
        Assert.Equal(HttpStatusCode.OK, second.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_assembles_the_plan_persists_it_and_reports_uncovered_tables()
    {
        var planner = new StubPlannerHandler(HttpStatusCode.OK, MigrationSamples.PlanGetBody(MigrationSamples.Entry(MigrationSamples.ValidOrders())));
        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host, PlanId);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationApply, new { slug = Slug });

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal(MigrationSamples.ServerPath(nameof(AiAssistantHandler.MigrationPlanGet)), planner.LastPath);

        var applied = await resp.Content.ReadFromJsonAsync<MigrationApplyResponse>(ApiJson);
        Assert.Equal("Orders", Assert.Single(applied!.Configuration!.Tables).CollectionName);

        // only orders is mapped; the other two discovered tables are reported rather than dropped silently
        Assert.Equal(["public.customers", "public.audit_log"], applied.UnmappedTables);

        var state = await MigrationSamples.LoadStateAsync(host.Config, Slug);
        Assert.Equal("Orders", Assert.Single(state!.LastMapConfiguration!.Tables).CollectionName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_rejects_a_plan_that_does_not_assemble_into_a_valid_configuration()
    {
        var broken = MigrationSamples.ValidOrders();
        broken.PrimaryKeyColumns = [];
        var planner = new StubPlannerHandler(HttpStatusCode.OK, MigrationSamples.PlanGetBody(MigrationSamples.Entry(broken)));
        await using var host = await NewMigrationHostAsync(planner);
        await SeedAsync(host, PlanId);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationApply, new { slug = Slug });

        Assert.Equal(HttpStatusCode.UnprocessableEntity, resp.StatusCode);
        var error = await resp.Content.ReadFromJsonAsync<ApiErrorResponse>();
        Assert.NotEmpty(error!.Errors!);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_on_a_plan_the_api_does_not_know_is_400()
    {
        await using var host = await NewMigrationHostAsync(new StubPlannerHandler(HttpStatusCode.NotFound, "{\"Status\":\"InvalidData\"}"));
        await SeedAsync(host, PlanId);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationApply, new { slug = Slug });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        var error = await resp.Content.ReadFromJsonAsync<ApiErrorResponse>();
        Assert.Contains("No plan found", error!.Error);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(HttpStatusCode.InternalServerError, HttpStatusCode.BadGateway)]
    [InlineData(HttpStatusCode.Unauthorized, HttpStatusCode.Unauthorized)]
    public async Task Apply_reports_an_ai_service_outage_or_refusal_with_its_own_status(HttpStatusCode upstream, HttpStatusCode expected)
    {
        await using var host = await NewMigrationHostAsync(new StubPlannerHandler(upstream, "{}"));
        await SeedAsync(host, PlanId);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.MigrationApply, new { slug = Slug });

        Assert.Equal(expected, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Connecting_to_a_different_source_forgets_the_plan()
    {
        await using var host = await NewMigrationHostAsync(StubPlannerHandler.Replying());
        await MigrationSamples.SeedDiscoveredSchemaAsync(host.Config, Slug, PlanId, selectedTables: [new SelectedSourceTable("orders", "public")]);

        await host.SetupConnectAsync(new ConnectRequest("Npgsql", "invalid"));

        var state = await MigrationSamples.LoadStateAsync(host.Config, Slug);
        Assert.Null(state!.PlanId);
        Assert.Null(state.PlanSelectedTables);
    }

    // -------------------------------------------------------------------

    private static Task<HttpResponseMessage> SaveAsync(QuillHost host) =>
        host.Client.PostAsJsonAsync(QuillRoutes.MigrationPlanSave, new
        {
            slug = Slug,
            changes = new[] { new { kind = "Edit", area = "Sales", collection = "Orders", config = MigrationSamples.ValidOrders() } }
        }, ApiJson);

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

    private static Task SeedAsync(QuillHost host, string? planId = null) =>
        MigrationSamples.SeedDiscoveredSchemaAsync(host.Config, Slug, planId);
}
