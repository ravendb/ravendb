using JetBrains.Annotations;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForHasChanged : AbstractIndexHandlerProcessorForHasChanged<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForHasChanged([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractIndexHasChangedController GetHasChangedController() => RequestHandler.Database.IndexStore.HasChanged;
}
