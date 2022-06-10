using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL
{
    internal class ShardedSqlEtlHandlerProcessorForTestEtl : AbstractShardedEtlHandlerProcessorForTestEtl<TestSqlEtlScript, SqlEtlConfiguration, SqlConnectionString>
    {
        public ShardedSqlEtlHandlerProcessorForTestEtl([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override TestSqlEtlScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestSqlEtlScript(json);

        protected override RavenCommand CreateCommand(BlittableJsonReaderObject json) => new SqlEtlTestCommand(json);
    }
}
