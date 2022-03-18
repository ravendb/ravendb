using JetBrains.Annotations;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors;

internal class IndexHandlerProcessorForSetLockMode : AbstractIndexHandlerProcessorForSetLockMode<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForSetLockMode([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override AbstractIndexLockModeProcessor GetIndexLockModeProcessor()
    {
        return RequestHandler.Database.IndexStore.LockMode;
    }
}
