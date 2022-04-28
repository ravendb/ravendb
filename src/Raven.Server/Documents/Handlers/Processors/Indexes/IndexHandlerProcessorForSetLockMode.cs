using JetBrains.Annotations;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForSetLockMode : AbstractIndexHandlerProcessorForSetLockMode<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForSetLockMode([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractIndexLockModeController GetIndexLockModeProcessor()
    {
        return RequestHandler.Database.IndexStore.LockMode;
    }
}
