using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Providers.AI.Test;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL;

internal sealed class ShardedAiEtlHandlerProcessorForPostScriptTest : AbstractShardedEtlHandlerProcessorForTest<TestAiEtlScript, AiEtlConfiguration, AiConnectionString>
{
    public ShardedAiEtlHandlerProcessorForPostScriptTest([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
        throw new NotSupportedInShardingException("AI ETLs are currently not supported in sharding");
    }

    protected override TestAiEtlScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestAiEtlScript(json);

    protected override RavenCommand CreateCommand(BlittableJsonReaderObject json) => new AiEtlTestCommand(RequestHandler.ShardExecutor.Conventions, json);
}
