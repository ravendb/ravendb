using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Studio;

internal class StudioQueryingAssistantProcessorForEmbeddingsGenerationTasks([NotNull] DatabaseRequestHandler requestHandler)
    : AbstractStudioQueryingAssistantProcessorForEmbeddingsGenerationTasks<DatabaseRequestHandler, DocumentsOperationContext>(requestHandler)
{
    protected override IEnumerable<EmbeddingsGenerationConfiguration> GetEmbeddingsTasks(DocumentsOperationContext context)
    {
        return context
            .DocumentDatabase
            .ReadDatabaseRecord()
            .EmbeddingsGenerations;
    }
}
