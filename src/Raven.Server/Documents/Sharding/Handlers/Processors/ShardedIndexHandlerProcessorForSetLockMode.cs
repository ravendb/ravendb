using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors;

internal class ShardedIndexHandlerProcessorForSetLockMode : AbstractIndexHandlerProcessorForSetLockMode<ShardedRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForSetLockMode([NotNull] ShardedRequestHandler requestHandler) 
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override AbstractIndexLockModeProcessor GetIndexLockModeProcessor()
    {
        return RequestHandler.ShardedContext.Indexes.LockMode;
    }
}
