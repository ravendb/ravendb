using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents;
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

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_with_duplicate_slug_returns_409()
    {
        var taken = "taken-" + Guid.NewGuid().ToString("N");
        await SeedWizardMapAsync(Host.Config, taken);

        await Host.ProvisionAsync(new ProvisionRequest("First App", taken));

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.ProvisionAsync(new ProvisionRequest(UniqueAppName(), taken)));

        Assert.Equal(HttpStatusCode.Conflict, ex.StatusCode);
        Assert.Contains(taken, ex.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_normalizes_explicit_slug_before_uniqueness_gate()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var normalized = $"my-custom-app-{suffix}";
        await SeedWizardMapAsync(Host.Config, normalized);

        await Host.ProvisionAsync(new ProvisionRequest("First App", normalized));

        // messy slug normalizes to the already-provisioned one
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.ProvisionAsync(new ProvisionRequest(UniqueAppName(), $"My Custom App {suffix}!!")));

        Assert.Equal(HttpStatusCode.Conflict, ex.StatusCode);
        Assert.Contains(normalized, ex.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_returns_409_when_slug_is_already_provisioned()
    {
        var slug = "twice-" + Guid.NewGuid().ToString("N");
        await SeedWizardMapAsync(Host.Config, slug);
        await Host.ProvisionAsync(new ProvisionRequest("First App", slug));

        var second = await Assert.ThrowsAsync<QuillHttpException>(() => Host.ProvisionAsync(new ProvisionRequest("Second App", slug)));

        Assert.Equal(HttpStatusCode.Conflict, second.StatusCode);
        Assert.Contains(slug, second.Body);
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

    private static async Task SeedWizardMapAsync(IDocumentStore store, string slug)
    {
        var cdc = AiHelperSamples.BuildCdcConfig();
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
