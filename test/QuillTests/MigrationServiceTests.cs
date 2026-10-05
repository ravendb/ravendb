using System.Net;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.AiHelper;
using Raven.Quill.AiHelper.Migration;
using Raven.Quill.AiHelper.Migration.Planning;
using Raven.Quill.Contracts;
using Raven.Quill.Logging;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class MigrationServiceTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string Slug = "shop";

    private const string ConversationId = "quill-cdc-planner/1";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_relays_the_frames_skips_keepalives_and_mirrors_the_plan_on_done()
    {
        using var store = await StoreWithSchemaAsync();
        var handler = StubPlannerHandler.Replying(
            new NoteFrame { Text = "reading" },
            new CollectionFrame { Status = "registered", Collection = "Orders", Version = 1, Rationale = "because", Config = Orders() },
            new ReplyFrame { Reply = "Orders it is." },
            new DoneFrame { ConversationId = ConversationId });
        var frames = new List<MigrationFrame>();

        var refusal = await NewService(store, handler).StartAsync(new MigrationStartRequest(Slug, Prompt: "plan it"), Collect(frames), CancellationToken.None);

        Assert.Null(refusal);
        Assert.Equal(MigrationService.StartPath, handler.LastPath);
        Assert.Collection(frames,
            f => Assert.IsType<NoteFrame>(f),
            f => Assert.Equal("Orders", Assert.IsType<CollectionFrame>(f).Config!.CollectionName),
            f => Assert.Equal("Orders it is.", Assert.IsType<ReplyFrame>(f).Reply),
            f => Assert.Equal(ConversationId, Assert.IsType<DoneFrame>(f).ConversationId));

        var plan = await MigrationSamples.LoadPlanAsync(store, Slug);
        Assert.Equal(ConversationId, plan!.ConversationId);
        Assert.Equal("Orders", Assert.Single(plan.Entries).Collection);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_replaces_the_previous_plan_on_done()
    {
        using var store = await StoreWithSchemaAsync(MigrationSamples.Plan("quill-cdc-planner/old", Entry("Products")));
        var handler = StubPlannerHandler.Replying(
            new CollectionFrame { Status = "registered", Collection = "Orders", Version = 1, Config = Orders() },
            new DoneFrame { ConversationId = ConversationId });

        await NewService(store, handler).StartAsync(new MigrationStartRequest(Slug), Collect([]), CancellationToken.None);

        var plan = await MigrationSamples.LoadPlanAsync(store, Slug);
        Assert.Equal(ConversationId, plan!.ConversationId);
        Assert.Equal("Orders", Assert.Single(plan.Entries).Collection);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_failed_start_keeps_the_previous_plan()
    {
        using var store = await StoreWithSchemaAsync(MigrationSamples.Plan(ConversationId, Entry("Orders")));

        await NewService(store, new StubPlannerHandler(HttpStatusCode.BadGateway, "{}"))
            .StartAsync(new MigrationStartRequest(Slug), Collect([]), CancellationToken.None);

        var plan = await MigrationSamples.LoadPlanAsync(store, Slug);
        Assert.Equal(ConversationId, plan!.ConversationId);
        Assert.Equal("Orders", Assert.Single(plan.Entries).Collection);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_timeout_becomes_one_error_frame()
    {
        using var store = await StoreWithSchemaAsync();
        var frames = new List<MigrationFrame>();

        await NewService(store, new ThrowingPlannerHandler(new TaskCanceledException("timeout", new TimeoutException())))
            .StartAsync(new MigrationStartRequest(Slug), Collect(frames), CancellationToken.None);

        Assert.Contains("could not be reached", Assert.IsType<ErrorFrame>(Assert.Single(frames)).Message);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_stream_that_breaks_mid_turn_ends_with_an_error_frame()
    {
        using var store = await StoreWithSchemaAsync(MigrationSamples.Plan("quill-cdc-planner/old", Entry("Products")));
        var handler = new BreakingStreamPlannerHandler(MigrationSamples.Sse(
            new CollectionFrame { Status = "registered", Collection = "Orders", Version = 1, Config = Orders() }));
        var frames = new List<MigrationFrame>();

        await NewService(store, handler).StartAsync(new MigrationStartRequest(Slug), Collect(frames), CancellationToken.None);

        Assert.Collection(frames,
            f => Assert.Equal("Orders", Assert.IsType<CollectionFrame>(f).Collection),
            f => Assert.Contains("could not be reached", Assert.IsType<ErrorFrame>(f).Message));

        var plan = await MigrationSamples.LoadPlanAsync(store, Slug);
        Assert.Equal("quill-cdc-planner/old", plan!.ConversationId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_cancelled_caller_is_not_reported_as_an_error()
    {
        using var store = await StoreWithSchemaAsync();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var frames = new List<MigrationFrame>();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            NewService(store, new ThrowingPlannerHandler(new TaskCanceledException()))
                .StartAsync(new MigrationStartRequest(Slug), Collect(frames), cts.Token));

        Assert.Empty(frames);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Invalid_credentials_are_not_reported_as_missing_consent()
    {
        using var store = await StoreWithSchemaAsync();
        var handler = StubPlannerHandler.Replying();

        var refusal = await NewService(store, handler, AiHelperStatus.InvalidCredentials)
            .StartAsync(new MigrationStartRequest(Slug), Collect([]), CancellationToken.None);

        Assert.Equal(AiHelperStatus.InvalidCredentials, refusal!.Status);
        Assert.Contains("license", refusal.Message);
        Assert.DoesNotContain("consent", refusal.Message);
        Assert.Null(handler.LastPath);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_start_stream_that_ends_without_done_keeps_the_previous_plan()
    {
        using var store = await StoreWithSchemaAsync(MigrationSamples.Plan("quill-cdc-planner/old", Entry("Products")));
        var handler = StubPlannerHandler.Replying(
            new CollectionFrame { Status = "registered", Collection = "Orders", Version = 1, Config = Orders() });

        await NewService(store, handler).StartAsync(new MigrationStartRequest(Slug), Collect([]), CancellationToken.None);

        var plan = await MigrationSamples.LoadPlanAsync(store, Slug);
        Assert.Equal("quill-cdc-planner/old", plan!.ConversationId);
        Assert.Equal("Products", Assert.Single(plan.Entries).Collection);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_planner_save_leaves_the_rest_of_the_wizard_state_alone()
    {
        var mapped = new CdcSinkConfiguration { Tables = [Orders("Mapped")] };
        using var store = GetDocumentStore();
        await MigrationSamples.SeedDiscoveredSchemaAsync(store, Slug, mapConfiguration: mapped);
        var handler = StubPlannerHandler.Replying(
            new CollectionFrame { Status = "registered", Collection = "Orders", Version = 1, Config = Orders() },
            new DoneFrame { ConversationId = ConversationId });

        await NewService(store, handler).StartAsync(new MigrationStartRequest(Slug), Collect([]), CancellationToken.None);

        using var session = store.OpenAsyncSession();
        var state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(Slug));
        Assert.Equal(3, state!.LastDiscoveredSchema!.Tables.Count);
        Assert.Equal("Mapped", Assert.Single(state.LastMapConfiguration!.Tables).CollectionName);
        Assert.Equal("Orders", Assert.Single(state.MigrationPlan!.Entries).Collection);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Ask_continues_from_the_stored_mirror_and_applies_removals_and_conventions()
    {
        var stored = new MigrationPlan { ConversationId = ConversationId };
        stored.Upsert("Orders", null, Orders());
        stored.Upsert("Products", null, Orders("Products"));
        using var store = await StoreWithSchemaAsync();
        await stored.SaveAsync(store, Slug);

        var handler = StubPlannerHandler.Replying(
            new RemovedFrame { Collection = "Products", Reason = "embedded instead" },
            new ConventionsFrame { PropertyCase = PropertyCase.SnakeCase, PropertyLanguage = "Spanish", MustReEmit = ["Orders"] },
            new ReplyFrame { Reply = "Done." },
            new DoneFrame { ConversationId = ConversationId });

        var refusal = await NewService(store, handler).AskAsync(new MigrationAskRequest(Slug, ConversationId, "drop products"), Collect([]), CancellationToken.None);

        Assert.Null(refusal);
        Assert.Equal(MigrationService.AskPath, handler.LastPath);
        Assert.Contains($"\"ConversationId\":\"{ConversationId}\"", handler.LastBody);

        var plan = await MigrationSamples.LoadPlanAsync(store, Slug);
        Assert.Equal("Orders", Assert.Single(plan!.Entries).Collection);
        Assert.Equal(PropertyCase.SnakeCase, plan.Conventions.PropertyCase);
        Assert.Equal("Spanish", plan.Conventions.PropertyLanguage);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_sends_the_schema_and_prompt_without_a_conversation()
    {
        using var store = await StoreWithSchemaAsync();
        var handler = StubPlannerHandler.Replying(new DoneFrame { ConversationId = ConversationId });

        await NewService(store, handler).StartAsync(new MigrationStartRequest(Slug, Prompt: "plan it"), Collect([]), CancellationToken.None);

        Assert.Contains($"\"Slug\":\"{Slug}\"", handler.LastBody);
        Assert.Contains("\"Prompt\":\"plan it\"", handler.LastBody);
        Assert.Contains("\"SourceTableName\":\"orders\"", handler.LastBody);
        Assert.DoesNotContain("\"ConversationId\":\"", handler.LastBody);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(HttpStatusCode.Unauthorized, "consent")]
    [InlineData(HttpStatusCode.TooManyRequests, "quota")]
    [InlineData(HttpStatusCode.NotFound, "No planning session")]
    [InlineData(HttpStatusCode.BadGateway, "HTTP 502")]
    public async Task A_refused_request_becomes_one_error_frame(HttpStatusCode status, string expected)
    {
        using var store = await StoreWithSchemaAsync();
        var frames = new List<MigrationFrame>();

        await NewService(store, new StubPlannerHandler(status, "{\"Status\":\"Refused\"}"))
            .StartAsync(new MigrationStartRequest(Slug), Collect(frames), CancellationToken.None);

        Assert.Contains(expected, Assert.IsType<ErrorFrame>(Assert.Single(frames)).Message);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_unreadable_frame_is_skipped_and_the_rest_still_arrive()
    {
        using var store = await StoreWithSchemaAsync();
        var body = "data: {\"type\":\"no-such-frame\"}\n\n" + MigrationSamples.Sse(new DoneFrame { ConversationId = ConversationId });
        var frames = new List<MigrationFrame>();

        await NewService(store, new StubPlannerHandler(HttpStatusCode.OK, body))
            .StartAsync(new MigrationStartRequest(Slug), Collect(frames), CancellationToken.None);

        Assert.IsType<DoneFrame>(Assert.Single(frames));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Upsert_bumps_the_version_case_insensitively_and_remove_drops_it()
    {
        using var store = await StoreWithSchemaAsync();
        var plan = new MigrationPlan { ConversationId = ConversationId };

        plan.Upsert("Orders", "first", Orders());
        plan.Upsert("orders", "second", Orders());
        plan.Upsert("Products", null, Orders("Products"));
        plan.Remove("PRODUCTS");
        await plan.SaveAsync(store, Slug);

        var loaded = await MigrationSamples.LoadPlanAsync(store, Slug);

        var entry = Assert.Single(loaded!.Entries);
        Assert.Equal("orders", entry.Collection);
        Assert.Equal("second", entry.Rationale);
        Assert.Equal(2, entry.Version);

        loaded.Upsert("Orders", "third", Orders());
        await loaded.SaveAsync(store, Slug);

        var reloaded = await MigrationSamples.LoadPlanAsync(store, Slug);
        Assert.Equal(3, Assert.Single(reloaded!.Entries).Version);
    }

    private async Task<IDocumentStore> StoreWithSchemaAsync(MigrationPlan? plan = null)
    {
        var store = GetDocumentStore();
        await MigrationSamples.SeedDiscoveredSchemaAsync(store, Slug, plan);
        return store;
    }

    private static MigrationService NewService(
        IDocumentStore store,
        HttpMessageHandler handler,
        AiHelperStatus consent = AiHelperStatus.Success) =>
        new(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") },
            store,
            new FakeConsentClient(consent),
            new QuillLogger<MigrationService>());

    private static Func<MigrationFrame, Task> Collect(List<MigrationFrame> frames) => frame =>
    {
        frames.Add(frame);
        return Task.CompletedTask;
    };

    private static PlanEntry Entry(string collection) =>
        new() { Collection = collection, Version = 1, Config = Orders(collection) };

    private static CdcSinkTableConfig Orders(string collection = "Orders") => new()
    {
        CollectionName = collection,
        SourceTableSchema = "public",
        SourceTableName = "orders",
        PrimaryKeyColumns = ["order_id"],
        Columns = [new CdcColumnMapping { Column = "order_id", Name = "OrderId" }]
    };
}
