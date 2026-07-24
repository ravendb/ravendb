using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;

namespace QuillTests.E2E.Fixtures;

/// Seeds a CDC sink and its source connection string onto an app. The sink is cluster metadata with no
/// endpoint, so — like <see cref="ConversationSeed"/> — tests can't reach it through a typed wrapper.
/// Disabled + SkipInitialLoad by default so nothing dials a live Postgres.
public static class CdcSeed
{
    public const string SourceConnectionStringName = "src";

    public static async Task SeedCdcSinkAsync(this QuillApp app, CdcSinkConfiguration config,
        bool disabled = true, bool skipInitialLoad = true,
        string sourceConnectionString = "Host=localhost;Database=src")
    {
        await app.Store.Maintenance.ForDatabase(app.Slug).SendAsync(
            new PutConnectionStringOperation<SqlConnectionString>(new SqlConnectionString
            {
                Name = SourceConnectionStringName,
                FactoryName = "Npgsql",
                ConnectionString = sourceConnectionString,
            }));

        config.ConnectionStringName = SourceConnectionStringName;
        config.Disabled = disabled;
        config.SkipInitialLoad = skipInitialLoad;
        await app.Store.Maintenance.ForDatabase(app.Slug).SendAsync(new AddCdcSinkOperation(config));
    }
}
