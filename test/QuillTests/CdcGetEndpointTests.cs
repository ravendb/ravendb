using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class CdcGetEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcGet_returns_current_configuration()
    {
        await using var app = await NewAppAsync();
        await SeedCdcSinkAsync(app.Store, app.Slug, name: "app-cdc");

        var cdc = await app.GetCdcAsync();
        Assert.Equal("app-cdc", cdc.Name);
        Assert.Equal("src", cdc.ConnectionStringName);
        var table = Assert.Single(cdc.Tables);
        Assert.Equal("Customers", table.CollectionName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcGet_returns_404_when_no_cdc_configured()
    {
        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.GetCdcAsync());
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcGet_returns_404_for_unknown_slug()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetCdcAsync("nonexistent"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    // CDC sink is cluster metadata — no Postgres contacted until the task runs
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
