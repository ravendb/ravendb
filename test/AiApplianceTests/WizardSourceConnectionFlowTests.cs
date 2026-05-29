using System.Net.Http.Json;
using AiApplianceTests.E2E.Fixtures;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

public class WizardSourceConnectionFlowTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private const string WizardSourceProbeName = "_wizard-source-probe";

    [RavenFact(RavenTestCategory.AiAppliance)]
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

        var result = await store.Maintenance.ForDatabase(store.Database)
            .SendAsync(new GetConnectionStringsOperation(WizardSourceProbeName, ConnectionStringType.Sql));

        Assert.True(result.SqlConnectionStrings is null || result.SqlConnectionStrings.ContainsKey(WizardSourceProbeName) == false);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Connect_persists_probe_connection_string_for_table_verification()
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
                tableNames = new[] { "customers" },
            });

        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

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
