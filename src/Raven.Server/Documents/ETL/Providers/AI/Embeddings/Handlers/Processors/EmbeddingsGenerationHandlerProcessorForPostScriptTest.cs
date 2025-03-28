using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test;
using Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.Embeddings.Handlers.Processors;

internal sealed class EmbeddingsGenerationHandlerProcessorForPostScriptTest : AbstractDatabaseEtlHandlerProcessorForTest<TestEmbeddingsGenerationScript, EmbeddingsGenerationConfiguration, AiConnectionString>
{
    public EmbeddingsGenerationHandlerProcessorForPostScriptTest([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TestEmbeddingsGenerationScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestEmbeddingsGenerationScript(json);
}
