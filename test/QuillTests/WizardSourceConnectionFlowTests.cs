using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class WizardSourceConnectionFlowTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Discover_uses_inline_connection_string_and_persists_no_connection_string()
    {
        await using var host = await NewHostAsync();

        // raw: seed the wizard doc connect would create, so discover (which now requires it) runs standalone
        using (var session = host.Config.OpenAsyncSession())
        {
            await session.StoreAsync(new WizardState(), WizardState.DocumentIdFor(QuillHost.DefaultWizardSlug));
            await session.SaveChangesAsync();
        }

        // unreachable source: success=false with errors, not an HTTP error
        var discover = await host.SetupDiscoverAsync(new DiscoverRequest("SqlClient", "invalid"));
        Assert.False(discover.Success);
        Assert.NotEmpty(discover.Errors);

        // the source is used inline; discover persists nothing as a config-DB connection string
        var result = await host.Config.Maintenance.ForDatabase(host.Config.Database)
            .SendAsync(new GetConnectionStringsOperation());
        Assert.True(result.SqlConnectionStrings is null || result.SqlConnectionStrings.Count == 0);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Connect_stores_the_source_on_the_wizard_doc_and_reports_reachability()
    {
        await using var host = await NewHostAsync();

        var connect = await host.SetupConnectAsync(new ConnectRequest("SqlClient", "invalid"));
        Assert.False(connect.Success);

        var error = Assert.Single(connect.Errors);
        Assert.Contains("Could not connect to the source database", error.Message);
        Assert.DoesNotContain("Exception", error.Message);
        Assert.DoesNotContain("   at ", error.Message);
        Assert.False(string.IsNullOrEmpty(error.Details));

        // the source connection string lives on the per-app wizard doc, not as a config-DB connection string
        using (var session = host.Config.OpenAsyncSession())
        {
            var state = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(QuillHost.DefaultWizardSlug));
            Assert.Equal("Microsoft.Data.SqlClient", state!.Provider);
            Assert.Equal("invalid", state.SourceConnectionString);
        }

        var result = await host.Config.Maintenance.ForDatabase(host.Config.Database)
            .SendAsync(new GetConnectionStringsOperation());
        Assert.True(result.SqlConnectionStrings is null || result.SqlConnectionStrings.Count == 0);
    }
}
