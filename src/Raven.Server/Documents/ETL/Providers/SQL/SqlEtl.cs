using System;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.ETL.Relational;
using Raven.Server.Documents.ETL.Relational.RelationalWriters;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    public sealed class SqlEtl : RelationalDatabaseEtlBase<SqlEtlConfiguration, SqlConnectionString>
    {
        public const string SqlEtlTag = "SQL ETL";
    
        public SqlEtl(Transformation transformation, SqlEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore) : base(transformation, configuration, database, serverStore, SqlEtlTag)
        {
            EtlType = EtlType.Sql;
        }

        public override EtlType EtlType { get; }
        
        protected override EtlTransformer<ToRelationalDatabaseItem, RelationalDatabaseTableWithRecords, EtlStatsScope, EtlPerformanceOperation> GetTransformer(DocumentsOperationContext context)
        {
            return new SqlDocumentTransformer(Transformation, Database, context, Configuration);
        }

        protected override RelationalDatabaseWriterBase<SqlConnectionString, SqlEtlConfiguration> GetRelationalDatabaseWriterInstance()
        {
            return new SqlDatabaseWriter(Database, Configuration, RelationalMetrics, Statistics);
        }   

        protected override RelationalDatabaseWriterSimulatorBase<SqlEtlConfiguration, SqlConnectionString> GetWriterSimulator()
        {
            return new SqlDatabaseWriterSimulator(Configuration);
        }
    }
}
