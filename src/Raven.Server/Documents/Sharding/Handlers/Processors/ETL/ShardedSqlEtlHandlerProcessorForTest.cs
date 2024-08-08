using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Documents.ETL.Relational;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL
{
    internal sealed class ShardedSqlEtlHandlerProcessorForTest : AbstractShardedEtlHandlerProcessorForTest<TestRelationalDatabaseEtlScript<SqlConnectionString, SqlEtlConfiguration>, SqlEtlConfiguration, SqlConnectionString>
    {
        public ShardedSqlEtlHandlerProcessorForTest([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override TestRelationalDatabaseEtlScript<SqlConnectionString, SqlEtlConfiguration> GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestRelationalEtlScriptSql(json);

        protected override RavenCommand CreateCommand(BlittableJsonReaderObject json) => new SqlEtlTestCommand(RequestHandler.ShardExecutor.Conventions, json);
    }
}
