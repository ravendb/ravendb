using JetBrains.Annotations;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForDelete : AbstractIndexHandlerProcessorForDelete<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForDelete([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractIndexDeleteController GetIndexDeleteProcessor() => RequestHandler.Database.IndexStore.Delete;
}
