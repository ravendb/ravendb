using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class CdcGetEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcGet_returns_current_configuration()
    {
        await using var app = await NewAppAsync();

        var cdc = await app.GetCdcAsync();
        Assert.Equal($"{app.Slug}-cdc", cdc.Configuration.Name);
        Assert.Equal("src", cdc.Configuration.ConnectionStringName);
        Assert.Equal("Host=localhost;Database=src", cdc.ConnectionString);
        var table = Assert.Single(cdc.Configuration.Tables);
        Assert.Equal("Orders", table.CollectionName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcGet_returns_404_when_no_cdc_configured()
    {
        await using var app = await NewAppAsync();

        var session = Host.Config.OpenAsyncSession();
        var wizard = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(app.Slug));
        wizard.LastMapConfiguration = null;
        await session.SaveChangesAsync();
        
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.GetCdcAsync());
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcGet_returns_404_for_unknown_slug()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetCdcAsync("nonexistent"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }
}
