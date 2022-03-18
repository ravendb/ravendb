using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;

internal class ShardedIndexHandlerProcessorForSetPriority : AbstractIndexHandlerProcessorForSetPriority<ShardedRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForSetPriority([NotNull] ShardedRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override AbstractIndexPriorityProcessor GetIndexPriorityProcessor()
    {
        return RequestHandler.ShardedContext.Indexes.Priority;
    }
}
