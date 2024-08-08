using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.RelationalWriters;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.SQL.RelationalWriters;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.SQL;

public sealed class SqlEtl(Transformation transformation, SqlEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
    : RelationalDatabaseEtlBase<SqlEtlConfiguration, SqlConnectionString>(transformation, configuration, database, serverStore, SqlEtlTag)
{
    public const string SqlEtlTag = "SQL ETL";

    public override EtlType EtlType => EtlType.Sql;

    protected override EtlTransformer<ToRelationalDatabaseItem, RelationalDatabaseTableWithRecords, EtlStatsScope, EtlPerformanceOperation> GetTransformer(DocumentsOperationContext context)
    {
        return new SqlDocumentTransformer(Transformation, Database, context, Configuration);
    }

    protected override RelationalDatabaseWriterBase<SqlConnectionString, SqlEtlConfiguration> GetRelationalDatabaseWriterInstance()
    {
        return new SqlDatabaseWriter(Database, Configuration, RelationalMetrics, Statistics);
    }

    protected override RelationalDatabaseWriterSimulator GetWriterSimulator()
    {
        return new RelationalDatabaseWriterSimulator(GetRelationalDatabaseWriterInstance(), Configuration.ParameterizeDeletes);
    }
}
