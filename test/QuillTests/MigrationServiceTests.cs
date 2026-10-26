using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using FastTests;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.AiHelper;
using Raven.Quill.AiHelper.Migration;
using Raven.Quill.AiHelper.Migration.Planning;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Logging;
using Raven.Quill.Wizard;
using Raven.Server.Documents.AI.AiAssistant.Handlers;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class MigrationServiceTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string Slug = "shop";

    private const string PlanId = MigrationSamples.PlanId;

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Proposal_start_relays_the_frames_skips_keepalives_and_stores_the_plan_id()
    {
        using var store = await StoreWithSchemaAsync();
        var handler = StubPlannerHandler.Replying(
            new NoteFrame { Text = "reading" },
            new CollectionFrame { Status = "registered", Collection = "Orders", Version = 1, Rationale = "because", Config = Orders() },
            new ReplyFrame { Reply = "Orders it is." },
            MigrationSamples.Done());
        var (stream, ctx) = NewStream();

        var refusal = await NewService(store, handler).ProposalStartAsync(
            new MigrationProposalStartRequest(Slug, SelectedTables: [new SelectedSourceTable("orders", "public")], Prompt: "plan it"),
            stream,
            CancellationToken.None);

        Assert.Null(refusal);
        Assert.Equal(MigrationSamples.ServerPath(nameof(AiAssistantHandler.MigrationProposalStart)), handler.LastPath);
        Assert.Collection(Frames(ctx),
            f => Assert.IsType<NoteFrame>(f),
            f => Assert.Equal("Orders", Assert.IsType<CollectionFrame>(f).Config!.CollectionName),
            f => Assert.Equal("Orders it is.", Assert.IsType<ReplyFrame>(f).Reply),
            f => Assert.Equal(PlanId, Assert.IsType<DoneFrame>(f).PlanId));

        var state = await MigrationSamples.LoadStateAsync(store, Slug);
        Assert.Equal(PlanId, state!.PlanId);
        Assert.Equal("orders", Assert.Single(state.PlanSelectedTables!).SourceTableName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task The_relay_reads_upstream_to_the_end_after_the_browser_leaves()
    {
        using var store = await StoreWithSchemaAsync();
        var handler = StubPlannerHandler.Replying(
            new CollectionFrame { Status = "registered", Collection = "Orders", Version = 1, Config = Orders() },
            MigrationSamples.Done());
        var (stream, ctx) = NewStream(browserGone: true);

        await NewService(store, handler).ProposalStartAsync(new MigrationProposalStartRequest(Slug), stream, CancellationToken.None);

        Assert.True(stream.BrowserGone);
        Assert.Equal(0, ctx.Response.Body.Length);
        Assert.Equal(PlanId, (await MigrationSamples.LoadStateAsync(store, Slug))!.PlanId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_area_turn_stores_nothing()
    {
        using var store = await StoreWithSchemaAsync(planId: PlanId);
        var handler = StubPlannerHandler.Replying(
            new CollectionFrame { Status = "registered", Collection = "Orders", Version = 1, Config = Orders() },
            MigrationSamples.Done(PlanId + "/area/Sales", planId: "planner/other"));

        var refusal = await NewService(store, handler).AreaStartAsync(new MigrationAreaStartRequest(Slug, "Sales"), NewStream().Stream, CancellationToken.None);

        Assert.Null(refusal);
        Assert.Equal(MigrationSamples.ServerPath(nameof(AiAssistantHandler.MigrationAreaStart)), handler.LastPath);
        Assert.Equal(PlanId, (await MigrationSamples.LoadStateAsync(store, Slug))!.PlanId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_proposal_start_leaves_the_rest_of_the_wizard_state_alone()
    {
        var mapped = new CdcSinkConfiguration { Tables = [Orders("Mapped")] };
        using var store = GetDocumentStore();
        await MigrationSamples.SeedDiscoveredSchemaAsync(store, Slug, mapConfiguration: mapped);
        var handler = StubPlannerHandler.Replying(MigrationSamples.Done());

        await NewService(store, handler).ProposalStartAsync(new MigrationProposalStartRequest(Slug), NewStream().Stream, CancellationToken.None);

        var state = await MigrationSamples.LoadStateAsync(store, Slug);
        Assert.Equal(3, state!.LastDiscoveredSchema!.Tables.Count);
        Assert.Equal("Mapped", Assert.Single(state.LastMapConfiguration!.Tables).CollectionName);
        Assert.Equal(PlanId, state.PlanId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Turns_post_the_plan_id_and_only_what_the_stage_needs()
    {
        using var store = await StoreWithSchemaAsync(planId: PlanId);
        var handler = StubPlannerHandler.Replying(MigrationSamples.Done());
        var service = NewService(store, handler);

        await service.ProposalAskAsync(new MigrationProposalAskRequest(Slug, "add deposits"), NewStream().Stream, CancellationToken.None);
        Assert.Equal(MigrationSamples.ServerPath(nameof(AiAssistantHandler.MigrationProposalAsk)), handler.LastPath);
        AssertSent(handler.LastBody, planId: PlanId, area: null, prompt: "add deposits");

        await service.AreaStartAsync(new MigrationAreaStartRequest(Slug, "Sales"), NewStream().Stream, CancellationToken.None);
        Assert.Equal(MigrationSamples.ServerPath(nameof(AiAssistantHandler.MigrationAreaStart)), handler.LastPath);
        AssertSent(handler.LastBody, planId: PlanId, area: "Sales", prompt: null);

        await service.AreaAskAsync(new MigrationAreaAskRequest(Slug, "Sales", "link the customer"), NewStream().Stream, CancellationToken.None);
        Assert.Equal(MigrationSamples.ServerPath(nameof(AiAssistantHandler.MigrationAreaAsk)), handler.LastPath);
        AssertSent(handler.LastBody, planId: PlanId, area: "Sales", prompt: "link the customer");

        await service.PlanGetAsync(new MigrationPlanGetRequest(Slug), CancellationToken.None);
        Assert.Equal(MigrationSamples.ServerPath(nameof(AiAssistantHandler.MigrationPlanGet)), handler.LastPath);
        AssertSent(handler.LastBody, planId: PlanId, area: null, prompt: null);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Every_call_but_the_proposal_start_is_refused_before_the_proposal()
    {
        using var store = await StoreWithSchemaAsync();
        var handler = StubPlannerHandler.Replying(MigrationSamples.Done());
        var service = NewService(store, handler);
        var change = new MigrationPlanChange(PlanChangeKind.Edit, "Sales", "Orders", Orders());

        var refusals = new[]
        {
            await service.ProposalAskAsync(new MigrationProposalAskRequest(Slug, "go on"), NewStream().Stream, CancellationToken.None),
            await service.AreaStartAsync(new MigrationAreaStartRequest(Slug, "Sales"), NewStream().Stream, CancellationToken.None),
            await service.AreaAskAsync(new MigrationAreaAskRequest(Slug, "Sales", "go on"), NewStream().Stream, CancellationToken.None),
            (await service.PlanGetAsync(new MigrationPlanGetRequest(Slug), CancellationToken.None)).Refusal,
            (await service.PlanSaveAsync(new MigrationPlanSaveRequest(Slug, [change]), CancellationToken.None)).Refusal,
            (await service.ApplyAsync(new MigrationApplyRequest(Slug), CancellationToken.None)).Refusal
        };

        Assert.All(refusals, refusal => Assert.Contains("no plan yet", refusal!.Message));
        Assert.Null(handler.LastPath);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Two_areas_run_at_once_and_a_second_turn_on_a_held_area_is_refused()
    {
        using var store = await StoreWithSchemaAsync(planId: PlanId);
        var handler = new GatedPlannerHandler(MigrationSamples.Sse(MigrationSamples.Done()));
        var service = NewService(store, handler);

        var sales = Task.Run(() => service.AreaStartAsync(new MigrationAreaStartRequest(Slug, "Sales"), NewStream().Stream, CancellationToken.None));
        var lending = Task.Run(() => service.AreaStartAsync(new MigrationAreaStartRequest(Slug, "Lending"), NewStream().Stream, CancellationToken.None));
        await handler.WaitForArrivalsAsync(2);

        var again = await service.AreaStartAsync(new MigrationAreaStartRequest(Slug, "sales"), NewStream().Stream, CancellationToken.None);

        handler.Release.SetResult();
        Assert.True(again!.IsConflict);
        Assert.Null(await sales);
        Assert.Null(await lending);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Plan_save_is_refused_while_a_turn_of_the_app_runs_and_relayed_after()
    {
        using var store = await StoreWithSchemaAsync(planId: PlanId);
        var handler = new GatedPlannerHandler(MigrationSamples.PlanSaveBody([], "Loans links to Clients"));
        var service = NewService(store, handler);
        var save = new MigrationPlanSaveRequest(Slug, [new MigrationPlanChange(PlanChangeKind.Edit, "Sales", "Orders", Orders())]);

        var turn = Task.Run(() => service.AreaStartAsync(new MigrationAreaStartRequest(Slug, "Sales"), NewStream().Stream, CancellationToken.None));
        await handler.WaitForArrivalsAsync(1);
        var during = await service.PlanSaveAsync(save, CancellationToken.None);

        handler.Release.SetResult();
        await turn;
        var after = await service.PlanSaveAsync(save, CancellationToken.None);

        Assert.True(during.Refusal!.IsConflict);
        Assert.Null(after.Refusal);
        Assert.Equal(["Loans links to Clients"], after.Outcome!.Warnings);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Plan_save_sends_each_change_with_its_kind_by_name()
    {
        using var store = await StoreWithSchemaAsync(planId: PlanId);
        var handler = new StubPlannerHandler(HttpStatusCode.OK, MigrationSamples.PlanSaveBody([]));

        await NewService(store, handler).PlanSaveAsync(new MigrationPlanSaveRequest(Slug,
        [
            new MigrationPlanChange(PlanChangeKind.Delete, "Sales", "Orders"),
            new MigrationPlanChange(PlanChangeKind.Rename, "Sales", "Customers", NewName: "Clients")
        ]), CancellationToken.None);

        Assert.Equal(MigrationSamples.ServerPath(nameof(AiAssistantHandler.MigrationPlanSave)), handler.LastPath);
        var changes = JsonNode.Parse(handler.LastBody)!["Changes"]!;
        Assert.Equal("Delete", (string?)changes[0]!["Kind"]);
        Assert.Null(changes[0]!["Config"]);
        Assert.Equal("Rename", (string?)changes[1]!["Kind"]);
        Assert.Equal("Clients", (string?)changes[1]!["NewName"]);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Plan_save_reads_the_issues_and_warnings_of_a_422()
    {
        using var store = await StoreWithSchemaAsync(planId: PlanId);
        var handler = new StubPlannerHandler(HttpStatusCode.UnprocessableEntity, MigrationSamples.PlanSaveBody(
            [new ValidationIssue { Message = "Ghost is not registered, so it cannot be removed.", Path = "changes[0]" }],
            "Orders links to Customers, which is no longer registered."));

        var (outcome, refusal) = await NewService(store, handler).PlanSaveAsync(
            new MigrationPlanSaveRequest(Slug, [new MigrationPlanChange(PlanChangeKind.Delete, "Sales", "Ghost")]), CancellationToken.None);

        Assert.Null(refusal);
        Assert.Equal("changes[0]", Assert.Single(outcome!.Issues).Path);
        Assert.Equal(["Orders links to Customers, which is no longer registered."], outcome.Warnings);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Plan_get_reads_the_apis_pascal_case_plan()
    {
        using var store = await StoreWithSchemaAsync(planId: PlanId);
        var handler = new StubPlannerHandler(HttpStatusCode.OK, MigrationSamples.PlanGetBody(
            MigrationSamples.Entry(MigrationSamples.ValidOrders(), PlanEntryStatus.Rejected)));

        var (response, refusal) = await NewService(store, handler).PlanGetAsync(new MigrationPlanGetRequest(Slug), CancellationToken.None);

        Assert.Null(refusal);
        Assert.Equal(PlanId + "/plan", response!.Plan.Id);
        var orders = Assert.Single(response.Plan.Entries).Value;
        Assert.Equal(PlanEntryStatus.Rejected, orders.Status);
        Assert.Equal("order_id", Assert.Single(orders.Config!.PrimaryKeyColumns));
        Assert.NotNull(response.Proposal);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("not json")]
    [InlineData("{\"Proposal\":null}")]
    public async Task Plan_get_refuses_an_answer_it_cannot_read(string body)
    {
        using var store = await StoreWithSchemaAsync(planId: PlanId);

        var (response, refusal) = await NewService(store, new StubPlannerHandler(HttpStatusCode.OK, body))
            .PlanGetAsync(new MigrationPlanGetRequest(Slug), CancellationToken.None);

        Assert.Null(response);
        Assert.Equal(AiHelperStatus.InternalError, refusal!.Status);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_builds_the_configuration_from_the_plan_and_skips_rejected_and_removed_collections()
    {
        using var store = await StoreWithSchemaAsync(planId: PlanId);
        var handler = new StubPlannerHandler(HttpStatusCode.OK, MigrationSamples.PlanGetBody(
            MigrationSamples.Entry(MigrationSamples.ValidOrders()),
            MigrationSamples.Entry(MigrationSamples.ValidCustomers(), PlanEntryStatus.Rejected),
            MigrationSamples.Entry(Orders("Products"), PlanEntryStatus.Removed)));

        var (response, refusal) = await NewService(store, handler).ApplyAsync(new MigrationApplyRequest(Slug), CancellationToken.None);

        Assert.Null(refusal);
        Assert.Equal(MigrationSamples.ServerPath(nameof(AiAssistantHandler.MigrationPlanGet)), handler.LastPath);
        AssertSent(handler.LastBody, planId: PlanId, area: null, prompt: null);
        Assert.Equal("Orders", Assert.Single(response!.Configuration!.Tables).CollectionName);
        Assert.Equal(["public.customers", "public.audit_log"], response.UnmappedTables);

        var state = await MigrationSamples.LoadStateAsync(store, Slug);
        Assert.Equal("Orders", Assert.Single(state!.LastMapConfiguration!.Tables).CollectionName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_reports_unmapped_tables_against_the_tables_handed_to_the_planner()
    {
        using var store = GetDocumentStore();
        await MigrationSamples.SeedDiscoveredSchemaAsync(store, Slug, PlanId,
            selectedTables: [new SelectedSourceTable("orders", "public"), new SelectedSourceTable("customers", "public")]);
        var handler = new StubPlannerHandler(HttpStatusCode.OK, MigrationSamples.PlanGetBody(MigrationSamples.Entry(MigrationSamples.ValidOrders())));

        var (response, refusal) = await NewService(store, handler).ApplyAsync(new MigrationApplyRequest(Slug), CancellationToken.None);

        Assert.Null(refusal);
        Assert.Equal(["public.customers"], response!.UnmappedTables);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_on_a_plan_the_api_does_not_know_is_refused()
    {
        using var store = await StoreWithSchemaAsync(planId: PlanId);

        var (response, refusal) = await NewService(store, new StubPlannerHandler(HttpStatusCode.NotFound, "{\"Status\":\"InvalidData\"}"))
            .ApplyAsync(new MigrationApplyRequest(Slug), CancellationToken.None);

        Assert.Null(response);
        Assert.Contains("No plan found", refusal!.Message);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_refuses_a_plan_with_nothing_registered()
    {
        using var store = await StoreWithSchemaAsync(planId: PlanId);
        var handler = new StubPlannerHandler(HttpStatusCode.OK, MigrationSamples.PlanGetBody(
            MigrationSamples.Entry(MigrationSamples.ValidOrders(), PlanEntryStatus.Removed)));

        var (_, refusal) = await NewService(store, handler).ApplyAsync(new MigrationApplyRequest(Slug), CancellationToken.None);

        Assert.Contains("no collections yet", refusal!.Message);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Apply_reports_a_join_column_that_is_not_a_column_of_the_source_table()
    {
        var orders = MigrationSamples.ValidOrders();
        var customer = MigrationSamples.ValidCustomer();
        customer.JoinColumns = ["buyer_ref"];
        orders.LinkedTables = [customer];
        using var store = await StoreWithSchemaAsync(planId: PlanId);
        var handler = new StubPlannerHandler(HttpStatusCode.OK, MigrationSamples.PlanGetBody(MigrationSamples.Entry(orders)));

        var (response, refusal) = await NewService(store, handler).ApplyAsync(new MigrationApplyRequest(Slug), CancellationToken.None);

        Assert.Null(refusal);
        Assert.Null(response!.Configuration);
        Assert.Contains("buyer_ref", Assert.Single(response.Errors));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_failed_start_keeps_the_previous_plan_id()
    {
        using var store = await StoreWithSchemaAsync(planId: "planner/old");

        await NewService(store, new StubPlannerHandler(HttpStatusCode.BadGateway, "{}"))
            .ProposalStartAsync(new MigrationProposalStartRequest(Slug), NewStream().Stream, CancellationToken.None);

        Assert.Equal("planner/old", (await MigrationSamples.LoadStateAsync(store, Slug))!.PlanId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_timeout_becomes_one_error_frame()
    {
        using var store = await StoreWithSchemaAsync();
        var (stream, ctx) = NewStream();

        await NewService(store, new ThrowingPlannerHandler(new TaskCanceledException("timeout", new TimeoutException())))
            .ProposalStartAsync(new MigrationProposalStartRequest(Slug), stream, CancellationToken.None);

        Assert.Contains("could not be reached", Assert.IsType<ErrorFrame>(Assert.Single(Frames(ctx))).Message);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_stream_that_breaks_mid_turn_ends_with_an_error_frame_and_keeps_the_previous_plan_id()
    {
        using var store = await StoreWithSchemaAsync(planId: "planner/old");
        var handler = new BreakingStreamPlannerHandler(MigrationSamples.Sse(
            new CollectionFrame { Status = "registered", Collection = "Orders", Version = 1, Config = Orders() }));
        var (stream, ctx) = NewStream();

        await NewService(store, handler).ProposalStartAsync(new MigrationProposalStartRequest(Slug), stream, CancellationToken.None);

        Assert.Collection(Frames(ctx),
            f => Assert.Equal("Orders", Assert.IsType<CollectionFrame>(f).Collection),
            f => Assert.Contains("could not be reached", Assert.IsType<ErrorFrame>(f).Message));
        Assert.Equal("planner/old", (await MigrationSamples.LoadStateAsync(store, Slug))!.PlanId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_stalled_stream_is_cut_after_one_silent_timeout_and_the_turn_is_free_again()
    {
        using var store = await StoreWithSchemaAsync();
        var turns = new MigrationTurns();
        var handler = new HangingStreamPlannerHandler(MigrationSamples.Sse(
            new CollectionFrame { Status = "registered", Collection = "Orders", Version = 1, Config = Orders() }));
        var (stream, ctx) = NewStream();

        await NewService(store, handler, turns: turns, timeout: TimeSpan.FromSeconds(1))
            .ProposalStartAsync(new MigrationProposalStartRequest(Slug), stream, CancellationToken.None);

        Assert.Collection(Frames(ctx),
            f => Assert.IsType<CollectionFrame>(f),
            f => Assert.Contains("could not be reached", Assert.IsType<ErrorFrame>(f).Message));
        Assert.Null(turns.TryBeginTurn(Slug, PlannerAgent.Proposal, area: null).Refusal);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Stop_cancels_the_running_area_turn_upstream_and_frees_the_app()
    {
        using var store = await StoreWithSchemaAsync(planId: PlanId);
        var turns = new MigrationTurns();
        var handler = new GatedPlannerHandler(MigrationSamples.Sse(MigrationSamples.Done()));
        var service = NewService(store, handler, turns: turns);
        var (stream, ctx) = NewStream();

        var turn = Task.Run(() => service.AreaStartAsync(new MigrationAreaStartRequest(Slug, "Sales"), stream, CancellationToken.None));
        await handler.WaitForArrivalsAsync(1);

        Assert.Null(service.Stop(new MigrationStopRequest(Slug, PlannerAgent.Area, "sales")));

        Assert.Null(await turn.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal("The turn was stopped.", Assert.IsType<ErrorFrame>(Assert.Single(Frames(ctx))).Message);
        Assert.Null(turns.TryBeginSave(Slug).Refusal);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Stop_cuts_a_stream_that_is_already_answering()
    {
        using var store = await StoreWithSchemaAsync(planId: PlanId);
        var turns = new MigrationTurns();
        var handler = new HangingStreamPlannerHandler(MigrationSamples.Sse(
            new CollectionFrame { Status = "registered", Collection = "Orders", Version = 1, Config = Orders() }));
        var service = NewService(store, handler, turns: turns);
        var (stream, ctx) = NewStream();

        var turn = Task.Run(() => service.AreaAskAsync(new MigrationAreaAskRequest(Slug, "Sales", "link the customer"), stream, CancellationToken.None));
        await handler.Hanging.Task.WaitAsync(TimeSpan.FromSeconds(30));

        service.Stop(new MigrationStopRequest(Slug, PlannerAgent.Area, "Sales"));

        Assert.Null(await turn.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Collection(Frames(ctx),
            f => Assert.IsType<CollectionFrame>(f),
            f => Assert.Equal("The turn was stopped.", Assert.IsType<ErrorFrame>(f).Message));
        Assert.Null(turns.TryBeginSave(Slug).Refusal);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Stop_reaches_only_the_turn_of_its_agent()
    {
        var turns = new MigrationTurns();
        using var proposal = turns.TryBeginTurn(Slug, PlannerAgent.Proposal, area: null);
        using var areaNamedProposal = turns.TryBeginTurn(Slug, PlannerAgent.Area, "proposal");
        using var sales = turns.TryBeginTurn(Slug, PlannerAgent.Area, "Sales");

        turns.Stop(Slug, PlannerAgent.Proposal, area: null);

        Assert.True(proposal.Stopped.IsCancellationRequested);
        Assert.False(areaNamedProposal.Stopped.IsCancellationRequested);
        Assert.False(sales.Stopped.IsCancellationRequested);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_save_and_the_turns_of_its_app_refuse_each_other()
    {
        var turns = new MigrationTurns();

        var sales = turns.TryBeginTurn(Slug, PlannerAgent.Area, "Sales");
        Assert.Contains("working on the plan", turns.TryBeginSave(Slug).Refusal!.Message);
        Assert.Contains("already running", turns.TryBeginTurn(Slug, PlannerAgent.Area, "Sales").Refusal!.Message);
        Assert.Contains("working on the plan", turns.TryBeginSave(Slug).Refusal!.Message);
        sales.Dispose();

        var save = turns.TryBeginSave(Slug);
        Assert.Null(save.Refusal);
        Assert.Contains("being saved", turns.TryBeginTurn(Slug, PlannerAgent.Proposal, area: null).Refusal!.Message);
        Assert.Contains("being saved", turns.TryBeginSave(Slug).Refusal!.Message);
        Assert.Null(turns.TryBeginTurn("other-app", PlannerAgent.Proposal, area: null).Refusal);
        save.Dispose();

        Assert.Null(turns.TryBeginTurn(Slug, PlannerAgent.Proposal, area: null).Refusal);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(PlannerAgent.Area, null)]
    [InlineData(PlannerAgent.Proposal, "Sales")]
    public async Task Stop_refuses_an_agent_without_its_area_or_a_proposal_with_one(PlannerAgent agent, string? area)
    {
        using var store = await StoreWithSchemaAsync(planId: PlanId);

        Assert.NotNull(NewService(store, StubPlannerHandler.Replying()).Stop(new MigrationStopRequest(Slug, agent, area)));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Invalid_credentials_are_not_reported_as_missing_consent()
    {
        using var store = await StoreWithSchemaAsync();
        var handler = StubPlannerHandler.Replying();

        var refusal = await NewService(store, handler, AiHelperStatus.InvalidCredentials)
            .ProposalStartAsync(new MigrationProposalStartRequest(Slug), NewStream().Stream, CancellationToken.None);

        Assert.Equal(AiHelperStatus.InvalidCredentials, refusal!.Status);
        Assert.Contains("license", refusal.Message);
        Assert.DoesNotContain("consent", refusal.Message);
        Assert.Null(handler.LastPath);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_start_stream_that_ends_without_done_reports_an_error_and_keeps_the_previous_plan_id()
    {
        using var store = await StoreWithSchemaAsync(planId: "planner/old");
        var handler = StubPlannerHandler.Replying(
            new CollectionFrame { Status = "registered", Collection = "Orders", Version = 1, Config = Orders() });
        var (stream, ctx) = NewStream();

        await NewService(store, handler).ProposalStartAsync(new MigrationProposalStartRequest(Slug), stream, CancellationToken.None);

        Assert.Collection(Frames(ctx),
            f => Assert.IsType<CollectionFrame>(f),
            f => Assert.Contains("stopped answering", Assert.IsType<ErrorFrame>(f).Message));
        Assert.Equal("planner/old", (await MigrationSamples.LoadStateAsync(store, Slug))!.PlanId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_start_that_errors_after_its_reply_relays_both_and_continues_on_the_plan_the_error_names()
    {
        using var store = await StoreWithSchemaAsync(planId: "planner/old");
        var handler = StubPlannerHandler.Replying(
            new CollectionFrame { Status = "registered", Collection = "Orders", Version = 1, Config = Orders() },
            new ReplyFrame { Reply = "Here is the plan." },
            new ErrorFrame { Message = "Could not store the schema (error id: 'x')", PlanId = PlanId });
        var (stream, ctx) = NewStream();

        await NewService(store, handler).ProposalStartAsync(new MigrationProposalStartRequest(Slug), stream, CancellationToken.None);

        Assert.Collection(Frames(ctx),
            f => Assert.IsType<CollectionFrame>(f),
            f => Assert.IsType<ReplyFrame>(f),
            f => Assert.Equal(PlanId, Assert.IsType<ErrorFrame>(f).PlanId));
        Assert.Equal(PlanId, (await MigrationSamples.LoadStateAsync(store, Slug))!.PlanId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_start_that_errors_before_any_plan_exists_keeps_the_previous_plan_id()
    {
        using var store = await StoreWithSchemaAsync(planId: "planner/old");

        await NewService(store, StubPlannerHandler.Replying(new ErrorFrame { Message = "The schema could not be read." }))
            .ProposalStartAsync(new MigrationProposalStartRequest(Slug), NewStream().Stream, CancellationToken.None);

        Assert.Equal("planner/old", (await MigrationSamples.LoadStateAsync(store, Slug))!.PlanId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_failed_area_turn_stores_nothing()
    {
        using var store = await StoreWithSchemaAsync(planId: PlanId);

        await NewService(store, StubPlannerHandler.Replying(new ErrorFrame { Message = "failed", PlanId = "planner/other" }))
            .AreaStartAsync(new MigrationAreaStartRequest(Slug, "Sales"), NewStream().Stream, CancellationToken.None);

        Assert.Equal(PlanId, (await MigrationSamples.LoadStateAsync(store, Slug))!.PlanId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_sends_the_selected_tables_gzipped_and_the_prompt()
    {
        using var store = await StoreWithSchemaAsync();
        var handler = StubPlannerHandler.Replying(MigrationSamples.Done());

        await NewService(store, handler).ProposalStartAsync(
            new MigrationProposalStartRequest(Slug, SelectedTables: [new SelectedSourceTable("orders", "public")], Prompt: "plan it"),
            NewStream().Stream,
            CancellationToken.None);

        Assert.Contains($"\"Slug\":\"{Slug}\"", handler.LastBody);
        Assert.Contains("\"Prompt\":\"plan it\"", handler.LastBody);
        Assert.DoesNotContain("\"PlanId\"", handler.LastBody);

        using var body = JsonDocument.Parse(handler.LastBody);
        Assert.False(body.RootElement.TryGetProperty("Schema", out _));

        var schemaJson = MigrationSamples.SentSchemaJson(handler.LastBody);

        Assert.Equal(await ExpectedNarrowedSchemaJsonAsync(store, "orders"), schemaJson);
        Assert.Contains("\"SourceTableName\":\"orders\"", schemaJson);
        Assert.DoesNotContain("customers", schemaJson);
        Assert.DoesNotContain("audit_log", schemaJson);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(HttpStatusCode.Unauthorized, "consent")]
    [InlineData(HttpStatusCode.NotFound, "No plan found")]
    [InlineData(HttpStatusCode.RequestEntityTooLarge, "too large")]
    [InlineData(HttpStatusCode.BadGateway, "HTTP 502")]
    public async Task A_refused_request_becomes_one_error_frame(HttpStatusCode status, string expected)
    {
        using var store = await StoreWithSchemaAsync();
        var (stream, ctx) = NewStream();

        await NewService(store, new StubPlannerHandler(status, "{\"Status\":\"Refused\"}"))
            .ProposalStartAsync(new MigrationProposalStartRequest(Slug), stream, CancellationToken.None);

        Assert.Contains(expected, Assert.IsType<ErrorFrame>(Assert.Single(Frames(ctx))).Message);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("{\"Status\":\"OutOfTokens\",\"InputTokenCount\":0,\"OutputTokenCount\":0}", "monthly AI token quota")]
    [InlineData("{\"Status\":\"InternalError\"}", "busy")]
    [InlineData("", "busy")]
    [InlineData("Rate limit exceeded, error id: 'x'", "busy")]
    public async Task Too_many_requests_is_the_quota_only_when_the_body_says_out_of_tokens(string body, string expected)
    {
        using var store = await StoreWithSchemaAsync();
        var (stream, ctx) = NewStream();

        await NewService(store, new StubPlannerHandler(HttpStatusCode.TooManyRequests, body))
            .ProposalStartAsync(new MigrationProposalStartRequest(Slug), stream, CancellationToken.None);

        Assert.Contains(expected, Assert.IsType<ErrorFrame>(Assert.Single(Frames(ctx))).Message);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_unreadable_frame_is_skipped_and_the_rest_still_arrive()
    {
        using var store = await StoreWithSchemaAsync();
        var body = "data: {\"type\":\"no-such-frame\"}\n\n" + MigrationSamples.Sse(MigrationSamples.Done());
        var (stream, ctx) = NewStream();

        await NewService(store, new StubPlannerHandler(HttpStatusCode.OK, body, "text/event-stream"))
            .ProposalStartAsync(new MigrationProposalStartRequest(Slug), stream, CancellationToken.None);

        Assert.IsType<DoneFrame>(Assert.Single(Frames(ctx)));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Arrays_the_planner_leaves_out_arrive_empty()
    {
        using var store = await StoreWithSchemaAsync();
        var body =
            "data: {\"type\":\"collection\",\"status\":\"registered\",\"collection\":\"Orders\",\"version\":1}\n\n" +
            "data: {\"type\":\"reply\",\"openQuestions\":[{\"question\":\"Embed lines?\"}]}\n\n" +
            MigrationSamples.Sse(MigrationSamples.Done());
        var (stream, ctx) = NewStream();

        await NewService(store, new StubPlannerHandler(HttpStatusCode.OK, body, "text/event-stream"))
            .ProposalStartAsync(new MigrationProposalStartRequest(Slug), stream, CancellationToken.None);

        Assert.Collection(Frames(ctx),
            f => Assert.Empty(Assert.IsType<CollectionFrame>(f).Warnings),
            f =>
            {
                var reply = Assert.IsType<ReplyFrame>(f);
                Assert.Null(reply.Gaps);
                Assert.Empty(Assert.Single(reply.OpenQuestions).Options);
            },
            f => Assert.IsType<DoneFrame>(f));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_sends_only_the_foreign_keys_between_the_selected_tables()
    {
        using var store = await StoreWithSchemaAsync(tables:
        [
            MigrationSamples.SourceTable("orders", "customers", "audit_log"),
            MigrationSamples.SourceTable("customers"),
            MigrationSamples.SourceTable("audit_log")
        ]);
        var handler = StubPlannerHandler.Replying(MigrationSamples.Done());

        await NewService(store, handler).ProposalStartAsync(
            new MigrationProposalStartRequest(Slug, SelectedTables: [new SelectedSourceTable("orders", "public"), new SelectedSourceTable("customers", "public")]),
            NewStream().Stream,
            CancellationToken.None);

        using var sent = JsonDocument.Parse(MigrationSamples.SentSchemaJson(handler.LastBody));
        var tables = sent.RootElement.GetProperty("Tables").EnumerateArray().ToArray();
        Assert.Equal(["orders", "customers"], tables.Select(t => t.GetProperty("SourceTableName").GetString()));
        Assert.Equal(["customers"], tables[0].GetProperty("ForeignKeys").EnumerateArray().Select(k => k.GetProperty("ReferencedTable").GetString()));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_matches_the_selected_tables_case_sensitively()
    {
        using var store = await StoreWithSchemaAsync(tables:
        [
            MigrationSamples.SourceTable("orders", "Orders"),
            MigrationSamples.SourceTable("Orders")
        ]);
        var handler = StubPlannerHandler.Replying(MigrationSamples.Done());

        await NewService(store, handler).ProposalStartAsync(
            new MigrationProposalStartRequest(Slug, SelectedTables: [new SelectedSourceTable("orders", "public")]),
            NewStream().Stream,
            CancellationToken.None);

        using var sent = JsonDocument.Parse(MigrationSamples.SentSchemaJson(handler.LastBody));
        var table = Assert.Single(sent.RootElement.GetProperty("Tables").EnumerateArray().ToArray());
        Assert.Equal("orders", table.GetProperty("SourceTableName").GetString());
        Assert.Empty(table.GetProperty("ForeignKeys").EnumerateArray());
    }

    private async Task<IDocumentStore> StoreWithSchemaAsync(string? planId = null, CdcSinkSourceTable[]? tables = null)
    {
        var store = GetDocumentStore();
        await MigrationSamples.SeedDiscoveredSchemaAsync(store, Slug, planId, tables: tables);
        return store;
    }

    private static async Task<string> ExpectedNarrowedSchemaJsonAsync(IDocumentStore store, string tableName)
    {
        var discovered = (await MigrationSamples.LoadStateAsync(store, Slug))!.LastDiscoveredSchema!;

        var narrowed = new CdcSinkSourceSchema
        {
            CatalogName = discovered.CatalogName,
            Tables = discovered.Tables.Where(t => t.SourceTableName == tableName).ToList(),
            Errors = [.. discovered.Errors],
            HasPermissionToSetup = discovered.HasPermissionToSetup,
            Warnings = [.. discovered.Warnings]
        };

        using var ctx = JsonOperationContext.ShortTermSingleUse();
        return store.Conventions.Serialization.DefaultConverter.ToBlittable(narrowed, ctx).ToString();
    }

    private static MigrationService NewService(
        IDocumentStore store,
        HttpMessageHandler handler,
        AiHelperStatus consent = AiHelperStatus.Success,
        MigrationTurns? turns = null,
        TimeSpan? timeout = null) =>
        new(new HttpClient(handler) { BaseAddress = new Uri("http://localhost"), Timeout = timeout ?? TimeSpan.FromSeconds(100) },
            store,
            new FakeConsentClient(consent),
            turns ?? new MigrationTurns(),
            new QuillLogger<MigrationService>());

    /// <summary>A browser-side stream over an in-memory response; a gone browser is one whose request is already aborted.</summary>
    private static (MigrationStream Stream, HttpContext Context) NewStream(bool browserGone = false)
    {
        var ctx = new DefaultHttpContext();
        ctx.Response.Body = new MemoryStream();
        if (browserGone)
            ctx.RequestAborted = new CancellationToken(canceled: true);

        return (new MigrationStream(ctx), ctx);
    }

    private static List<MigrationFrame> Frames(HttpContext ctx) =>
        Encoding.UTF8.GetString(((MemoryStream)ctx.Response.Body).ToArray())
            .Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(line => JsonSerializer.Deserialize<MigrationFrame>(line, NdjsonStream.JsonOpts)!)
            .ToList();

    private static void AssertSent(string body, string planId, string? area, string? prompt)
    {
        using var sent = JsonDocument.Parse(body);
        var root = sent.RootElement;
        Assert.Equal(planId, root.GetProperty("PlanId").GetString());
        Assert.Equal(area, Optional(root, "Area"));
        Assert.Equal(prompt, Optional(root, "Prompt"));
        Assert.False(root.TryGetProperty("Slug", out _));
        Assert.False(root.TryGetProperty("SchemaGzip", out _));
    }

    private static string? Optional(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static CdcSinkTableConfig Orders(string collection = "Orders") => new()
    {
        CollectionName = collection,
        SourceTableSchema = "public",
        SourceTableName = "orders",
        PrimaryKeyColumns = ["order_id"],
        Columns = [new CdcColumnMapping { Column = "order_id", Name = "OrderId" }]
    };
}
