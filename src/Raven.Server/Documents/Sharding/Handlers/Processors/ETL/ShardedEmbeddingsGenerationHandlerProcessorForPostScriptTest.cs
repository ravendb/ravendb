using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL;

internal sealed class ShardedEmbeddingsGenerationHandlerProcessorForPostScriptTest : AbstractShardedEtlHandlerProcessorForTest<TestEmbeddingsGenerationScript, EmbeddingsGenerationConfiguration, AiConnectionString>
{
    public ShardedEmbeddingsGenerationHandlerProcessorForPostScriptTest([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
        throw new NotSupportedInShardingException("AI Embeddings Generation is currently not supported in sharding");
    }

    protected override TestEmbeddingsGenerationScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestEmbeddingsGenerationScript(json);

    protected override RavenCommand CreateCommand(BlittableJsonReaderObject json) => new EmbeddingsGenerationTestCommand(RequestHandler.ShardExecutor.Conventions, json);
}
