using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for <c>GET /api/apps/{slug}/cdc</c>: the read-side that returns
/// the app's current CDC sink configuration so the UI can populate an edit form.
/// Adding the sink is a metadata op (no live source is dialed). Unknown slug and
/// no-CDC-configured both return 404.
/// </summary>
public class CdcGetEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcGet_returns_current_configuration()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await SeedCdcSinkAsync(store, perAppDb, name: "app-cdc");

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/my-app/cdc");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("app-cdc", json.GetProperty("name").GetString());
        Assert.Equal("src", json.GetProperty("connectionStringName").GetString());
        Assert.Equal(1, json.GetProperty("tables").GetArrayLength());
        Assert.Equal("Customers", json.GetProperty("tables")[0].GetProperty("collectionName").GetString());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcGet_returns_404_when_no_cdc_configured()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/my-app/cdc");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcGet_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/nonexistent/cdc");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    // Registers a CDC sink task on the per-app DB. The source connection string +
    // sink config are cluster metadata; no Postgres is contacted until the task runs.
    private static async Task SeedCdcSinkAsync(IDocumentStore store, string database, string name)
    {
        await store.Maintenance.ForDatabase(database).SendAsync(
            new PutConnectionStringOperation<SqlConnectionString>(new SqlConnectionString
            {
                Name = "src",
                FactoryName = "Npgsql",
                ConnectionString = "Host=localhost;Port=5432;Database=northwind;Username=u;Password=p",
            }));

        await store.Maintenance.ForDatabase(database).SendAsync(new AddCdcSinkOperation(new CdcSinkConfiguration
        {
            Name = name,
            ConnectionStringName = "src",
            Tables =
            [
                new CdcSinkTableConfig
                {
                    CollectionName = "Customers",
                    SourceTableSchema = "public",
                    SourceTableName = "customers",
                    PrimaryKeyColumns = ["customer_id"],
                    Columns =
                    [
                        new CdcColumnMapping { Column = "customer_id", Name = "Id" },
                        new CdcColumnMapping { Column = "company_name", Name = "CompanyName" },
                    ],
                },
            ],
        }));
    }
}
