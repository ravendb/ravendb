using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.RelationalWriters;
using Raven.Server.Documents.ETL.Providers.Snowflake.RelationalWriters;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Snowflake;

public sealed class SnowflakeEtl(Transformation transformation, SnowflakeEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
    : RelationalDatabaseEtlBase<SnowflakeEtlConfiguration, SnowflakeConnectionString>(transformation, configuration, database, serverStore, SnowflakeEtlTag)
{
    public const string SnowflakeEtlTag = "Snowflake ETL";

    public override EtlType EtlType => EtlType.Snowflake;

    protected override EtlTransformer<ToRelationalDatabaseItem, RelationalDatabaseTableWithRecords, EtlStatsScope, EtlPerformanceOperation> GetTransformer(DocumentsOperationContext context)
    {
        return new SnowflakeDocumentTransformer(Transformation, Database, context, Configuration);
    }

    protected override RelationalDatabaseWriterBase<SnowflakeConnectionString, SnowflakeEtlConfiguration> GetRelationalDatabaseWriterInstance()
    {
        return new SnowflakeDatabaseWriter(Database, Configuration, RelationalMetrics, Statistics);
    }

    protected override RelationalDatabaseWriterSimulator GetWriterSimulator()
    {
        return new RelationalDatabaseWriterSimulator(GetRelationalDatabaseWriterInstance());
    }
}
