using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class WizardSourceConnectionFlowTests(ITestOutputHelper output) : QuillTestBase(output)
{
    private const string WizardSourceProbeName = "_wizard-source-probe";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Discover_uses_inline_connection_string_and_does_not_persist_probe()
    {
        await using var host = await NewHostAsync();

        // unreachable source: success=false with errors, not an HTTP error
        var discover = await host.SetupDiscoverAsync(new DiscoverRequest("SqlClient", "invalid"));
        Assert.False(discover.Success);
        Assert.NotEmpty(discover.Errors);

        // maintenance op: discover must NOT persist the probe CS
        var result = await host.Config.Maintenance.ForDatabase(host.Config.Database)
            .SendAsync(new GetConnectionStringsOperation(WizardSourceProbeName, ConnectionStringType.Sql));

        Assert.True(result.SqlConnectionStrings is null || result.SqlConnectionStrings.ContainsKey(WizardSourceProbeName) == false);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Connect_persists_probe_connection_string_and_reports_reachability()
    {
        await using var host = await NewHostAsync();

        var connect = await host.SetupConnectAsync(new ConnectRequest("SqlClient", "invalid"));
        Assert.False(connect.Success);

        var error = Assert.Single(connect.Errors);
        Assert.Contains("Could not connect to the source database", error.Message);
        Assert.DoesNotContain("Exception", error.Message);
        Assert.DoesNotContain("   at ", error.Message);

        Assert.False(string.IsNullOrEmpty(error.Details));

        // maintenance op: probe CS persisted on the config DB; Provision transplants it later
        var result = await host.Config.Maintenance.ForDatabase(host.Config.Database)
            .SendAsync(new GetConnectionStringsOperation(WizardSourceProbeName, ConnectionStringType.Sql));
        var probe = Assert.Single(result.SqlConnectionStrings!).Value;

        Assert.Equal(WizardSourceProbeName, probe.Name);
        Assert.Equal("Microsoft.Data.SqlClient", probe.FactoryName);
        Assert.Equal("invalid", probe.ConnectionString);
    }
}
