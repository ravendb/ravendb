using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.SQL.Handlers.Processors
{
    internal class SqlEtlHandlerProcessorForTest : AbstractDatabaseEtlHandlerProcessorForTest<TestSqlEtlScript, SqlEtlConfiguration, SqlConnectionString>
    {
        public SqlEtlHandlerProcessorForTest([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override TestSqlEtlScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestSqlEtlScript(json);
    }
}
