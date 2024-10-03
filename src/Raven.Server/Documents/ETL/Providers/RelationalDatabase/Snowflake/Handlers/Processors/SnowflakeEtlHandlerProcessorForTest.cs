using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Snowflake.Handlers.Processors;

internal sealed class SnowflakeEtlHandlerProcessorForTest([NotNull] DatabaseRequestHandler requestHandler)
    : AbstractDatabaseEtlHandlerProcessorForTest<TestRelationalDatabaseEtlScript<SnowflakeConnectionString, SnowflakeEtlConfiguration>, SnowflakeEtlConfiguration,
        SnowflakeConnectionString>(requestHandler)
{
    protected override TestRelationalDatabaseEtlScript<SnowflakeConnectionString, SnowflakeEtlConfiguration> GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestRelationalEtlScriptSnowflake(json);
}
