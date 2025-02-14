using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Providers.AI.Test;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL;

internal sealed class ShardedAiIntegrationHandlerProcessorForPostScriptTest : AbstractShardedEtlHandlerProcessorForTest<TestAiIntegrationScript, AiIntegrationConfiguration, AiConnectionString>
{
    public ShardedAiIntegrationHandlerProcessorForPostScriptTest([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
        throw new NotSupportedInShardingException("AI Integrations are currently not supported in sharding");
    }

    protected override TestAiIntegrationScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestAiIntegrationScript(json);

    protected override RavenCommand CreateCommand(BlittableJsonReaderObject json) => new AiIntegrationTestCommand(RequestHandler.ShardExecutor.Conventions, json);
}
