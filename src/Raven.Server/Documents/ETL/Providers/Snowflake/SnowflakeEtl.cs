using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Server.Documents.ETL.Providers.Snowflake.RelationalWriters;
using Raven.Server.Documents.ETL.Relational;
using Raven.Server.Documents.ETL.Relational.RelationalWriters;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Snowflake;

public sealed class SnowflakeEtl : RelationalDatabaseEtlBase<SnowflakeEtlConfiguration, SnowflakeConnectionString>
{
    public const string SnowflakeEtlTag = "Snowflake ETL";
    
    public SnowflakeEtl(Transformation transformation, SnowflakeEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore) : base(transformation, configuration, database, serverStore, SnowflakeEtlTag)
    {
        EtlType = EtlType.Snowflake;
    }

    public override EtlType EtlType { get; }
    
    protected override EtlTransformer<ToRelationalDatabaseItem, RelationalDatabaseTableWithRecords, EtlStatsScope, EtlPerformanceOperation> GetTransformer(DocumentsOperationContext context)
    {
        return new SnowflakeDocumentTransformer(Transformation, Database, context, Configuration);
    }

    protected override RelationalDatabaseWriterBase<SnowflakeConnectionString, SnowflakeEtlConfiguration> GetRelationalDatabaseWriterInstance()
    {
        return new SnowflakeDatabaseWriter(Database, Configuration, RelationalMetrics, Statistics);
    }

    protected override RelationalDatabaseWriterSimulatorBase<SnowflakeConnectionString, SnowflakeEtlConfiguration> GetWriterSimulator()
    {
        return new SnowflakeDatabaseWriterSimulator(Configuration, Database, RelationalMetrics, Statistics);
    }
}
