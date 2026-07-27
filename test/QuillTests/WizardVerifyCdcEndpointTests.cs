using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class WizardVerifyCdcEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
{
    private const string WizardSourceProbeName = "_wizard-source-probe";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Requires_at_least_one_table()
    {
        await using var host = await NewHostAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => host.VerifyCdcAsync(new VerifyCdcRequest([])));
        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Requires_a_tables_array()
    {
        await using var host = await NewHostAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(
            () => QuillHttp.PostAsync<VerifyCdcResponse>(host.Client, QuillRoutes.SetupVerifyCdc, new { }));
        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Requires_a_discovered_schema()
    {
        await using var host = await NewHostAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(
            () => host.VerifyCdcAsync(new VerifyCdcRequest([new VerifyCdcTableRequest("orders")])));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Contains("discover", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Requires_the_probe_connection_string()
    {
        await using var host = await NewHostAsync();

        await host.SetupDiscoverAsync(new DiscoverRequest("SqlClient", "invalid"));

        var ex = await Assert.ThrowsAsync<QuillHttpException>(
            () => host.VerifyCdcAsync(new VerifyCdcRequest([new VerifyCdcTableRequest("orders")])));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Contains("connect", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Rejects_a_table_that_is_not_in_the_discovered_schema()
    {
        await using var host = await NewHostAsync();

        await host.SetupConnectAsync(new ConnectRequest("Npgsql", "invalid"));
        await host.SetupDiscoverAsync(new DiscoverRequest("Npgsql", "invalid"));

        var result = await host.VerifyCdcAsync(new VerifyCdcRequest([new VerifyCdcTableRequest("orders", "public")]));

        Assert.False(result.Success);
        var error = Assert.Single(result.Errors);
        Assert.Contains("public.orders", error.Message);
        Assert.Contains("discovered schema", error.Message);
        Assert.Empty(result.CompletedTables);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Rejects_a_table_without_a_primary_key()
    {
        await using var host = await NewHostAsync();

        await SeedDiscoveredSchemaAsync(host.Config, Table("audit_log", primaryKeyColumns: []));
        await RegisterProbeAsync(host.Config);

        var result = await host.VerifyCdcAsync(new VerifyCdcRequest([new VerifyCdcTableRequest("audit_log", "public")]));

        Assert.False(result.Success);
        var error = Assert.Single(result.Errors);
        Assert.Contains("no primary key", error.Message);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Rejects_an_unsupported_table()
    {
        await using var host = await NewHostAsync();

        var view = Table("daily_sales_view");
        view.UnsupportedReason = "Views cannot be captured by CDC.";
        await SeedDiscoveredSchemaAsync(host.Config, view);
        await RegisterProbeAsync(host.Config);

        var result = await host.VerifyCdcAsync(
            new VerifyCdcRequest([new VerifyCdcTableRequest("daily_sales_view", "public")]));

        Assert.False(result.Success);
        var error = Assert.Single(result.Errors);
        Assert.Contains("Views cannot be captured by CDC.", error.Message);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Reports_every_rejected_table_in_one_response()
    {
        await using var host = await NewHostAsync();

        await SeedDiscoveredSchemaAsync(host.Config, Table("audit_log", primaryKeyColumns: []));
        await RegisterProbeAsync(host.Config);

        var result = await host.VerifyCdcAsync(new VerifyCdcRequest([
            new VerifyCdcTableRequest("audit_log", "public"),
            new VerifyCdcTableRequest("ghost", "public"),
        ]));

        Assert.False(result.Success);
        Assert.Equal(2, result.Errors.Length);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Folds_an_unreachable_source_into_errors_without_failing_the_request()
    {
        await using var host = await NewHostAsync();

        await SeedDiscoveredSchemaAsync(host.Config, Table("orders"));
        await RegisterProbeAsync(host.Config);

        var result = await host.VerifyCdcAsync(new VerifyCdcRequest([new VerifyCdcTableRequest("orders", "public")]));

        Assert.False(result.Success);
        Assert.NotEmpty(result.Errors);
        Assert.Empty(result.CompletedTables);

        var error = Assert.Single(result.Errors);
        Assert.DoesNotContain("Exception", error.Message);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Does_not_persist_a_map_configuration()
    {
        await using var host = await NewHostAsync();

        await SeedDiscoveredSchemaAsync(host.Config, Table("orders"));
        await RegisterProbeAsync(host.Config);

        await host.VerifyCdcAsync(new VerifyCdcRequest([new VerifyCdcTableRequest("orders", "public")]));

        using var session = host.Config.OpenAsyncSession();
        var state = await session.LoadAsync<WizardState>(WizardState.DocumentId);
        Assert.Null(state!.LastMapConfiguration);
    }

    private static CdcSinkSourceTable Table(string name, string[]? primaryKeyColumns = null) => new()
    {
        SourceTableSchema = "public",
        SourceTableName = name,
        PrimaryKeyColumns = [.. primaryKeyColumns ?? ["id"]],
        Columns =
        [
            new CdcSinkSourceColumn { Name = "id", NativeType = "integer", SuggestedType = CdcColumnType.Default, IsPrimaryKey = true, IsCdcCapturable = true },
            new CdcSinkSourceColumn { Name = "name", NativeType = "text", SuggestedType = CdcColumnType.Default, IsCdcCapturable = true },
        ],
    };

    private static async Task SeedDiscoveredSchemaAsync(IDocumentStore store, params CdcSinkSourceTable[] tables)
    {
        var schema = new CdcSinkSourceSchema
        {
            CatalogName = "src",
            HasPermissionToSetup = true,
            Tables = [.. tables],
        };

        using var session = store.OpenAsyncSession();
        await session.StoreAsync(
            new WizardState { Provider = "Npgsql", LastDiscoveredSchema = schema }, WizardState.DocumentId);
        await session.SaveChangesAsync();
    }

    private static async Task RegisterProbeAsync(IDocumentStore store)
    {
        await store.Maintenance.ForDatabase(store.Database).SendAsync(
            new PutConnectionStringOperation<SqlConnectionString>(new SqlConnectionString
            {
                Name = WizardSourceProbeName,
                FactoryName = "Npgsql",
                ConnectionString = "Host=localhost;Port=1;Database=src;Username=u;Password=p;Timeout=1",
            }));
    }
}
