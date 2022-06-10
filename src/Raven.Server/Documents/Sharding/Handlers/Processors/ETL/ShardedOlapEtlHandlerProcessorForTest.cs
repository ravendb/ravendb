using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Providers.OLAP.Test;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL;

internal class ShardedOlapEtlHandlerProcessorForTest : AbstractShardedEtlHandlerProcessorForTest<TestOlapEtlScript, OlapEtlConfiguration, OlapConnectionString>
{
    public ShardedOlapEtlHandlerProcessorForTest([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TestOlapEtlScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestOlapEtlScript(json);

    protected override RavenCommand CreateCommand(BlittableJsonReaderObject json) => new OlapEtlTestCommand(json);
}
