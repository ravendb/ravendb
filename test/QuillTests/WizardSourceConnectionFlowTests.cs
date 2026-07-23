using System.Net.Http.Json;
using System.Text.Json;
using FastTests;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class WizardSourceConnectionFlowTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string WizardSourceProbeName = "_wizard-source-probe";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Discover_uses_inline_connection_string_and_does_not_persist_probe()
    {
        var store = GetDocumentStore();

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/setup/discover",
            new
            {
                provider = "SqlClient",
                connectionString = "invalid",
            });

        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        // Discover now carries the merged verification result. An unreachable source
        // surfaces as success=false with a non-empty errors list (not an HTTP error).
        var discover = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.False(discover.GetProperty("success").GetBoolean());
        Assert.NotEmpty(discover.GetProperty("errors").EnumerateArray());

        var result = await store.Maintenance.ForDatabase(store.Database)
            .SendAsync(new GetConnectionStringsOperation(WizardSourceProbeName, ConnectionStringType.Sql));

        Assert.True(result.SqlConnectionStrings is null || result.SqlConnectionStrings.ContainsKey(WizardSourceProbeName) == false);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Connect_persists_probe_connection_string_and_reports_reachability()
    {
        var store = GetDocumentStore();

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/setup/connect",
            new
            {
                provider = "SqlClient",
                connectionString = "invalid",
            });

        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        // Connect is a plain reachability probe now (SQL test-connection); an
        // unparseable connection string fails fast with success=false.
        var connect = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.False(connect.GetProperty("success").GetBoolean());

        // The failure is surfaced as an actionable, stack-trace-free summary; the raw exception
        // text (with its stack trace) is kept in `details` for the UI's "show details" disclosure.
        var error = Assert.Single(connect.GetProperty("errors").EnumerateArray());
        var message = error.GetProperty("message").GetString();
        Assert.Contains("Could not connect to the source database", message);
        Assert.DoesNotContain("Exception", message);
        Assert.DoesNotContain("   at ", message);

        var details = error.GetProperty("details").GetString();
        Assert.False(string.IsNullOrEmpty(details));

        // The probe connection string is still persisted on the config DB —
        // Provision later transplants its credentials into the per-app database.
        var result = await store.Maintenance.ForDatabase(store.Database)
            .SendAsync(new GetConnectionStringsOperation(WizardSourceProbeName, ConnectionStringType.Sql));
        var probe = Assert.Single(result.SqlConnectionStrings!).Value;

        Assert.Equal(WizardSourceProbeName, probe.Name);
        Assert.Equal("Microsoft.Data.SqlClient", probe.FactoryName);
        Assert.Equal("invalid", probe.ConnectionString);
    }

    private ApplianceWebApplicationFactory NewApplianceFactory(IDocumentStore store) =>
        new(licenseApiUrl: "http://unused-in-unit-tests",
            setupPackagePath: NewDataPath(forceCreateDir: true),
            applianceStore: store,
            configureOptions: opts => opts.ConfigDatabase = store.Database);
}
