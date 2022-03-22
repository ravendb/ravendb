using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;

internal class ShardedIndexHandlerProcessorForGetIndexHistory : AbstractIndexHandlerProcessorForGetIndexHistory<ShardedRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForGetIndexHistory([NotNull] ShardedRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.ShardedContext.DatabaseName;
}
