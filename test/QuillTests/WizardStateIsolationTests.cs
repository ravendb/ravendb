using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillWizardCollection.Name)]
public class WizardStateIsolationTests(ITestOutputHelper output, QuillCollectionHost collection)
    : QuillTestBase(output, collection)
{

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Map_persists_a_separate_wizard_doc_per_app()
    {
        var (a, b) = TwoSlugs();
        await StartWizardAsync(a);
        await StartWizardAsync(b);

        await Host.SetupMapAsync(MapRequestFor(a, name: $"{a}-cdc"));
        await Host.SetupMapAsync(MapRequestFor(b, name: $"{b}-cdc"));

        var stateA = await LoadWizardStateAsync(a);
        var stateB = await LoadWizardStateAsync(b);

        Assert.Equal($"{a}-cdc", stateA!.LastMapConfiguration!.Name);
        Assert.Equal($"{b}-cdc", stateB!.LastMapConfiguration!.Name);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Discover_persists_a_separate_wizard_doc_per_app()
    {
        var (a, b) = TwoSlugs();
        await StartWizardAsync(a);
        await StartWizardAsync(b);

        await Host.SetupDiscoverAsync(new DiscoverRequest("SqlClient", "invalid"), slug: a);
        await Host.SetupDiscoverAsync(new DiscoverRequest("Npgsql", "invalid"), slug: b);

        var stateA = await LoadWizardStateAsync(a);
        var stateB = await LoadWizardStateAsync(b);

        Assert.Equal("Microsoft.Data.SqlClient", stateA!.Provider);
        Assert.Equal("Npgsql", stateB!.Provider);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_reads_the_map_config_of_its_own_app()
    {
        var (mine, other) = TwoSlugs();

        await SeedWizardMapAsync(mine, collection: "MyOrders");
        await SeedWizardMapAsync(other, collection: "OtherOrders");

        await Host.ProvisionAsync(new ProvisionRequest("Mine", mine));

        var cdc = await Host.GetCdcAsync(mine);
        Assert.Equal("MyOrders", cdc.Configuration.Tables[0].CollectionName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_transplants_the_connecting_operators_own_source_not_a_concurrent_one()
    {
        var (mine, other) = TwoSlugs();
        const string mySource = "invalid-mine";
        const string otherSource = "invalid-other";

        await Host.SetupConnectAsync(new ConnectRequest("Npgsql", mySource), slug: mine);
        await Host.SetupMapAsync(MapRequestFor(mine, name: $"{mine}-cdc"));

        // a second operator connects to a different source before I provision
        await Host.SetupConnectAsync(new ConnectRequest("Npgsql", otherSource), slug: other);

        await Host.ProvisionAsync(new ProvisionRequest("Mine", mine));

        // raw: the source credentials get transplanted onto the app DB under the mapped connection-string name
        var strings = await Host.Config.Maintenance.ForDatabase(mine).SendAsync(
            new GetConnectionStringsOperation("src", ConnectionStringType.Sql));

        Assert.Equal(mySource, strings.SqlConnectionStrings!["src"].ConnectionString);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Deleting_an_app_removes_its_wizard_doc()
    {
        await using var app = await NewAppAsync();
        await SeedWizardMapAsync(app.Slug, collection: "Orders");

        await Host.DeleteAppAsync(app.Slug);

        Assert.Null(await LoadWizardStateAsync(app.Slug));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Connect_starts_a_fresh_wizard_doc_dropping_leftovers()
    {
        var (slug, _) = TwoSlugs();
        await SeedWizardMapAsync(slug, collection: "Stale");

        await StartWizardAsync(slug);

        var state = await LoadWizardStateAsync(slug);
        Assert.Equal("Microsoft.Data.SqlClient", state!.Provider);
        Assert.Null(state.LastMapConfiguration);
        Assert.Null(state.LastDiscoveredSchema);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Wizard_step_rejects_an_empty_slug()
    {
        // empty slug: no app to key the wizard doc by
        var ex = await Assert.ThrowsAsync<QuillHttpException>(
            () => Host.SetupDiscoverAsync(new DiscoverRequest("SqlClient", "invalid"), slug: ""));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Discover_before_connect_returns_400()
    {
        var (slug, _) = TwoSlugs();

        // no connect for this slug, so there is no wizard doc to discover into
        var ex = await Assert.ThrowsAsync<QuillHttpException>(
            () => Host.SetupDiscoverAsync(new DiscoverRequest("SqlClient", "invalid"), slug: slug));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Map_before_connect_returns_400()
    {
        var (slug, _) = TwoSlugs();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(
            () => Host.SetupMapAsync(MapRequestFor(slug, name: $"{slug}-cdc")));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    private static (string A, string B) TwoSlugs()
    {
        var suffix = Guid.NewGuid().ToString("N");
        return ($"iso-a-{suffix}", $"iso-b-{suffix}");
    }

    // connect is the first step: it creates the per-app wizard doc that discover/map then require
    private Task StartWizardAsync(string slug) =>
        Host.SetupConnectAsync(new ConnectRequest("SqlClient", "invalid"), slug: slug);

    private async Task<WizardState?> LoadWizardStateAsync(string slug)
    {
        using var session = Host.Config.OpenAsyncSession();
        return await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(slug));
    }

    private async Task SeedWizardMapAsync(string slug, string collection)
    {
        var cdc = AiHelperSamples.BuildCdcConfig();
        cdc.Tables[0].CollectionName = collection;
        cdc.Disabled = true;
        cdc.SkipInitialLoad = true;

        using var session = Host.Config.OpenAsyncSession();
        await session.StoreAsync(
            new WizardState
            {
                Provider = "Npgsql",
                SourceConnectionString = "Host=localhost;Database=src",
                LastMapConfiguration = cdc,
            },
            WizardState.DocumentIdFor(slug));
        await session.SaveChangesAsync();
    }

    private static MapRequest MapRequestFor(string slug, string name)
    {
        var sample = AiHelperSamples.BuildCdcConfig();
        return new MapRequest
        {
            Slug = slug,
            Name = name,
            ConnectionStringName = sample.ConnectionStringName,
            Tables = sample.Tables,
            Postgres = sample.Postgres,
            Disabled = true,
            SkipInitialLoad = true,
        };
    }
}
