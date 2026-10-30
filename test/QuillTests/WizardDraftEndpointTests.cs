using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Quill.Contracts;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillWizardCollection.Name)]
public class WizardDraftEndpointTests(ITestOutputHelper output, QuillCollectionHost collection)
    : QuillTestBase(output, collection)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Draft_returns_what_the_wizard_stored()
    {
        var slug = NewSlug();
        SelectedSourceTable[] selectedTables = [new("orders", "sales")];

        await Host.SetupConnectAsync(new ConnectRequest("SqlClient", "invalid"), slug: slug);
        await Host.SetupDiscoverAsync(new DiscoverRequest("SqlClient", "invalid", Schemas: ["sales", " sales "]), slug: slug);
        await Host.SetupDraftSaveAsync(new WizardDraftSaveRequest("Acme Shop", selectedTables, "map"), slug: slug);

        var draft = await Host.SetupDraftAsync(slug);

        Assert.False(draft.IsAppCreated);
        Assert.Equal("Acme Shop", draft.AppName);
        Assert.Equal("SqlClient", draft.Provider);
        Assert.Equal("invalid", draft.ConnectionString);
        Assert.False(draft.IsConnected);
        Assert.Equal(["sales"], draft.DiscoverSchemas);
        Assert.NotNull(draft.Discovery);
        Assert.Equal(selectedTables, draft.SelectedTables);
        Assert.Equal("map", draft.Step);
        Assert.False(draft.HasPlan);
        Assert.Empty(draft.PlanSelectedTables);
        Assert.Null(draft.AppliedPlanVersions);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Draft_for_an_unknown_slug_returns_404()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.SetupDraftAsync(NewSlug()));

        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Draft_of_a_created_app_says_so()
    {
        var slug = NewSlug();
        await SeedWizardMapAsync(slug, appliedPlanVersions: new() { ["Orders"] = 3 });
        await Host.ProvisionAsync(new ProvisionRequest("Created", slug));

        var draft = await Host.SetupDraftAsync(slug);

        Assert.True(draft.IsAppCreated);
        Assert.Equal(new Dictionary<string, int> { ["Orders"] = 3 }, draft.AppliedPlanVersions);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Mapping_by_hand_forgets_the_applied_plan_versions()
    {
        var slug = NewSlug();
        await SeedWizardMapAsync(slug, appliedPlanVersions: new() { ["Orders"] = 3 });

        var sample = AiHelperSamples.BuildCdcConfig();
        await Host.SetupMapAsync(new MapRequest
        {
            Slug = slug,
            Name = $"{slug}-cdc",
            ConnectionStringName = sample.ConnectionStringName,
            Tables = sample.Tables,
            Postgres = sample.Postgres,
            Disabled = true,
            SkipInitialLoad = true,
        });

        Assert.Null((await Host.SetupDraftAsync(slug)).AppliedPlanVersions);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Draft_save_before_connect_returns_400()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(
            () => Host.SetupDraftSaveAsync(new WizardDraftSaveRequest("Acme Shop", [], "verifySchema"), slug: NewSlug()));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    private static string NewSlug() => $"draft-{Guid.NewGuid():N}";

    private async Task SeedWizardMapAsync(string slug, Dictionary<string, int>? appliedPlanVersions = null)
    {
        var cdc = AiHelperSamples.BuildCdcConfig();
        cdc.Disabled = true;
        cdc.SkipInitialLoad = true;

        using var session = Host.Config.OpenAsyncSession();
        await session.StoreAsync(
            new WizardState
            {
                Provider = "Npgsql",
                SourceConnectionString = "Host=localhost;Database=src",
                LastMapConfiguration = cdc,
                AppliedPlanVersions = appliedPlanVersions,
            },
            WizardState.DocumentIdFor(slug));
        await session.SaveChangesAsync();
    }
}
