using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FastTests;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class WizardProvisionEndpointTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string WizardSourceProbeName = "_wizard-source-probe";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_with_invalid_explicit_slug_returns_400()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/api/setup/provision", new { appName = "Fine Name", slug = "!!!" });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        var body = await resp.Content.ReadAsStringAsync();
        Assert.Contains("slug '!!!'", body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_with_over_length_slug_returns_400()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var slug = new string('a', Slugifier.MaxLength + 1);
        var resp = await client.PostAsJsonAsync("/api/setup/provision", new { appName = "Fine Name", slug });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Contains("maximum length", await resp.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_with_reserved_slug_returns_400()
    {
        var store = GetDocumentStore(new Options
        {
            ModifyDatabaseName = _ => "cfg-" + Guid.NewGuid().ToString("N"),
        });
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/api/setup/provision", new { appName = "Fine Name", slug = store.Database });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        Assert.Contains("reserved", await resp.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_with_duplicate_slug_returns_409()
    {
        var store = GetDocumentStore();
        await SeedWizardMapAsync(store);
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var taken = "taken-" + Guid.NewGuid().ToString("N");
        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(taken)));
        using var _db = Databases.EnsureDatabaseDeletion(taken, store);

        var resp = await client.PostAsJsonAsync("/api/setup/provision", new { appName = UniqueAppName(), slug = taken });

        Assert.Equal(HttpStatusCode.Conflict, resp.StatusCode);
        Assert.Contains(taken, await resp.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_normalizes_explicit_slug_before_uniqueness_gate()
    {
        var store = GetDocumentStore();
        await SeedWizardMapAsync(store);
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var suffix = Guid.NewGuid().ToString("N");
        var normalized = $"my-custom-app-{suffix}";
        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(normalized)));
        using var _db = Databases.EnsureDatabaseDeletion(normalized, store);

        var resp = await client.PostAsJsonAsync("/api/setup/provision",
            new { appName = UniqueAppName(), slug = $"My Custom App {suffix}!!" });

        Assert.Equal(HttpStatusCode.Conflict, resp.StatusCode);
        Assert.Contains(normalized, await resp.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_returns_409_when_slug_is_already_provisioned()
    {
        var store = GetDocumentStore();
        await SeedWizardMapAsync(store);
        await RegisterProbeAsync(store);
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var slug = "twice-" + Guid.NewGuid().ToString("N");
        var first = await client.PostAsJsonAsync("/api/setup/provision", new { appName = "First App", slug });
        using var _db = Databases.EnsureDatabaseDeletion(slug, store);
        Assert.True(first.IsSuccessStatusCode, await first.Content.ReadAsStringAsync());

        var second = await client.PostAsJsonAsync("/api/setup/provision", new { appName = "Second App", slug });

        Assert.Equal(HttpStatusCode.Conflict, second.StatusCode);
        Assert.Contains(slug, await second.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_uses_explicit_slug_override()
    {
        var store = GetDocumentStore();
        await SeedWizardMapAsync(store);
        await RegisterProbeAsync(store);
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var slug = "override-" + Guid.NewGuid().ToString("N");
        var resp = await client.PostAsJsonAsync("/api/setup/provision", new { appName = "Pretty Display Name", slug });
        var body = await resp.Content.ReadAsStringAsync();
        using var _db = Databases.EnsureDatabaseDeletion(slug, store);

        Assert.True(resp.IsSuccessStatusCode, body);
        var json = JsonSerializer.Deserialize<JsonElement>(body);
        Assert.Equal(slug, json.GetProperty("slug").GetString());
        Assert.Equal($"apps/{slug}", json.GetProperty("id").GetString());

        using var session = store.OpenAsyncSession();
        var app = await session.LoadAsync<App>($"apps/{slug}");
        Assert.NotNull(app);
        Assert.Equal(slug, app.Slug);
        Assert.Equal(slug, app.Database);
        Assert.Equal("Pretty Display Name", app.AppName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_removes_the_probe_connection_string()
    {
        var store = GetDocumentStore();
        await SeedWizardMapAsync(store);
        await RegisterProbeAsync(store);
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var slug = "probe-clean-" + Guid.NewGuid().ToString("N");
        var resp = await client.PostAsJsonAsync("/api/setup/provision", new { appName = "Probe Clean", slug });
        using var _db = Databases.EnsureDatabaseDeletion(slug, store);
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        // the probe CS must not outlive provisioning on the config DB
        var result = await store.Maintenance.ForDatabase(store.Database)
            .SendAsync(new GetConnectionStringsOperation(WizardSourceProbeName, ConnectionStringType.Sql));
        Assert.True(result.SqlConnectionStrings is null || result.SqlConnectionStrings.ContainsKey(WizardSourceProbeName) == false);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_derives_slug_from_app_name_when_no_override()
    {
        var store = GetDocumentStore();
        await SeedWizardMapAsync(store);
        await RegisterProbeAsync(store);
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var suffix = Guid.NewGuid().ToString("N");
        var resp = await client.PostAsJsonAsync("/api/setup/provision", new { appName = $"Derive Me {suffix}" });
        var body = await resp.Content.ReadAsStringAsync();
        using var _db = Databases.EnsureDatabaseDeletion($"derive-me-{suffix}", store);

        Assert.True(resp.IsSuccessStatusCode, body);
        var json = JsonSerializer.Deserialize<JsonElement>(body);
        Assert.Equal($"derive-me-{suffix}", json.GetProperty("slug").GetString());
    }

    // ---- helpers ----

    private static string UniqueAppName() => "App " + Guid.NewGuid().ToString("N");

    private static async Task SeedWizardMapAsync(IDocumentStore store)
    {
        var cdc = AiHelperSamples.BuildCdcConfig();
        cdc.Disabled = true;
        cdc.SkipInitialLoad = true;

        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new WizardState { Provider = "Npgsql", LastMapConfiguration = cdc }, WizardState.DocumentId);
        await session.SaveChangesAsync();
    }

    private static async Task RegisterProbeAsync(IDocumentStore store)
    {
        await store.Maintenance.ForDatabase(store.Database).SendAsync(
            new PutConnectionStringOperation<SqlConnectionString>(new SqlConnectionString
            {
                Name = WizardSourceProbeName,
                FactoryName = "Npgsql",
                ConnectionString = "Host=localhost;Database=src",
            }));
    }

    private ApplianceWebApplicationFactory NewApplianceFactory(IDocumentStore store) =>
        new(licenseApiUrl: "http://unused-in-unit-tests",
            setupPackagePath: NewDataPath(forceCreateDir: true),
            applianceStore: store,
            configureOptions: opts => opts.ConfigDatabase = store.Database);
}
