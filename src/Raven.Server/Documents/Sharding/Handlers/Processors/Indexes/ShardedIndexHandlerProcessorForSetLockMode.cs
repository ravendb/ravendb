using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;

internal class ShardedIndexHandlerProcessorForSetLockMode : AbstractIndexHandlerProcessorForSetLockMode<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForSetLockMode([NotNull] ShardedDatabaseRequestHandler requestHandler) 
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override AbstractIndexLockModeController GetIndexLockModeProcessor()
    {
        return RequestHandler.DatabaseContext.Indexes.LockMode;
    }
}
