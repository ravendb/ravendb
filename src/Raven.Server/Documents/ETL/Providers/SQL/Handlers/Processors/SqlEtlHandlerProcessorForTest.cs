using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.ETL.Relational;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.SQL.Handlers.Processors
{
    internal sealed class SqlEtlHandlerProcessorForTest : AbstractDatabaseEtlHandlerProcessorForTest<TestRelationalEtlScript<SqlConnectionString, SqlEtlConfiguration>, SqlEtlConfiguration, SqlConnectionString>
    {
        public SqlEtlHandlerProcessorForTest([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override TestRelationalEtlScript<SqlConnectionString, SqlEtlConfiguration> GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestRelationalEtlScriptSql(json);
    }
}
