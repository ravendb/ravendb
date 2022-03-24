using JetBrains.Annotations;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForSetPriority : AbstractIndexHandlerProcessorForSetPriority<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForSetPriority([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override AbstractIndexPriorityController GetIndexPriorityProcessor()
    {
        return RequestHandler.Database.IndexStore.Priority;
    }
}
