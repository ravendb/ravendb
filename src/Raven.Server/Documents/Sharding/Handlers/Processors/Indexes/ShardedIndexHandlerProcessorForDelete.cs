using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;

internal class ShardedIndexHandlerProcessorForDelete : AbstractIndexHandlerProcessorForDelete<ShardedRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForDelete([NotNull] ShardedRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override AbstractIndexDeleteProcessor GetIndexDeleteProcessor() => RequestHandler.ShardedContext.Indexes.Delete;

    protected override string GetDatabaseName() => RequestHandler.ShardedContext.DatabaseName;
}
