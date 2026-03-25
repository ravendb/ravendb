using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.Handlers.Processors.Studio;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Studio;

internal class ShardedStudioQueryingAssistantProcessorForEmbeddingsGenerationTasks([NotNull] ShardedDatabaseRequestHandler requestHandler)
    : AbstractStudioQueryingAssistantProcessorForEmbeddingsGenerationTasks<ShardedDatabaseRequestHandler, TransactionOperationContext>(requestHandler)
{
    protected override IEnumerable<EmbeddingsGenerationConfiguration> GetEmbeddingsTasks(TransactionOperationContext context)
    {
        return RequestHandler
            .DatabaseContext
            .DatabaseRecord
            .EmbeddingsGenerations;
    }
}
