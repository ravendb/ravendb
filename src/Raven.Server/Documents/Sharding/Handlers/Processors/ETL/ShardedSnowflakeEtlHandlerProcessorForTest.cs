using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Relational;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL;

internal sealed class ShardedSnowflakeEtlHandlerProcessorForTest([NotNull] ShardedDatabaseRequestHandler requestHandler)
    : AbstractShardedEtlHandlerProcessorForTest<TestRelationalDatabaseEtlScript<SnowflakeConnectionString, SnowflakeEtlConfiguration>, SnowflakeEtlConfiguration,
        SnowflakeConnectionString>(requestHandler)
{
    protected override TestRelationalDatabaseEtlScript<SnowflakeConnectionString, SnowflakeEtlConfiguration> GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestRelationalEtlScriptSnowflake(json);

    protected override RavenCommand CreateCommand(BlittableJsonReaderObject json) => new SnowflakeEtlTestCommand(RequestHandler.ShardExecutor.Conventions, json);
}
