using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillWizardCollection.Name)]
public class WizardProvisionEndpointTests(ITestOutputHelper output, QuillCollectionHost collection)
    : QuillTestBase(output, collection)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_with_invalid_explicit_slug_returns_400()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.ProvisionAsync(new ProvisionRequest("Fine Name", "!!!")));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Contains("slug '!!!'", ex.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_with_over_length_slug_returns_400()
    {
        var slug = new string('a', Slugifier.MaxLength + 1);
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.ProvisionAsync(new ProvisionRequest("Fine Name", slug)));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Contains("maximum length", ex.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_with_reserved_slug_returns_400()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.ProvisionAsync(new ProvisionRequest("Fine Name", Host.Config.Database)));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Contains("reserved", ex.Body);
    }

    // provision is an upsert: the edit wizard reruns the whole flow and submits to the same endpoint
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_on_an_existing_slug_updates_the_app()
    {
        var slug = "twice-" + Guid.NewGuid().ToString("N");
        await SeedWizardMapAsync(Host.Config, slug, collection: "Orders");
        var first = await Host.ProvisionAsync(new ProvisionRequest("First App", slug));

        await SeedWizardMapAsync(Host.Config, slug, collection: "RenamedOrders");
        var second = await Host.ProvisionAsync(new ProvisionRequest("Second App", slug));

        Assert.Equal(first.Id, second.Id);
        Assert.Equal(slug, second.Slug);

        var cdc = await Host.GetCdcAsync(slug);
        Assert.Equal("RenamedOrders", cdc.Configuration.Tables[0].CollectionName);

        using var session = Host.Config.OpenAsyncSession();
        var app = await session.LoadAsync<App>($"apps/{slug}");
        Assert.Equal("Second App", app.AppName);
        // the CDC process state is keyed by task name, so an update must not rename the task
        Assert.Equal($"{slug}-cdc", app.CdcTaskName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_normalizes_explicit_slug_before_looking_for_an_existing_app()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var normalized = $"my-custom-app-{suffix}";
        await SeedWizardMapAsync(Host.Config, normalized);

        var first = await Host.ProvisionAsync(new ProvisionRequest("First App", normalized));

        // messy slug normalizes to the already-provisioned one, so this updates it
        var second = await Host.ProvisionAsync(new ProvisionRequest("Renamed App", $"My Custom App {suffix}!!"));

        Assert.Equal(first.Id, second.Id);
        Assert.Equal(normalized, second.Slug);
    }

    // a database under the slug with no app behind it is not ours to adopt
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_returns_409_when_the_database_exists_without_an_app()
    {
        var slug = "orphan-" + Guid.NewGuid().ToString("N");
        await SeedWizardMapAsync(Host.Config, slug);
        await Host.Config.Maintenance.Server.SendAsync(
            new CreateDatabaseOperation(new DatabaseRecord(slug)));

        try
        {
            var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.ProvisionAsync(new ProvisionRequest(UniqueAppName(), slug)));

            Assert.Equal(HttpStatusCode.Conflict, ex.StatusCode);
            Assert.Contains(slug, ex.Body);
        }
        finally
        {
            await Host.Config.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(slug, hardDelete: true));
        }
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_uses_explicit_slug_override()
    {
        var slug = "override-" + Guid.NewGuid().ToString("N");
        await SeedWizardMapAsync(Host.Config, slug);
        var response = await Host.ProvisionAsync(new ProvisionRequest("Pretty Display Name", slug));

        Assert.Equal(slug, response.Slug);
        Assert.Equal($"apps/{slug}", response.Id);

        using var session = Host.Config.OpenAsyncSession();
        var app = await session.LoadAsync<App>($"apps/{slug}");
        Assert.NotNull(app);
        Assert.Equal(slug, app.Slug);
        Assert.Equal(slug, app.Database);
        Assert.Equal("Pretty Display Name", app.AppName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_derives_slug_from_app_name_when_no_override()
    {
        var suffix = Guid.NewGuid().ToString("N");
        await SeedWizardMapAsync(Host.Config, $"derive-me-{suffix}");

        var response = await Host.ProvisionAsync(new ProvisionRequest($"Derive Me {suffix}"));

        Assert.Equal($"derive-me-{suffix}", response.Slug);
    }

    private static string UniqueAppName() => "App " + Guid.NewGuid().ToString("N");

    private static async Task SeedWizardMapAsync(IDocumentStore store, string slug, string? collection = null)
    {
        var cdc = AiHelperSamples.BuildCdcConfig();
        if (collection is not null)
            cdc.Tables[0].CollectionName = collection;
        cdc.Disabled = true;
        cdc.SkipInitialLoad = true;

        using var session = store.OpenAsyncSession();
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
}
