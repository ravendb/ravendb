using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;

internal class ShardedIndexHandlerProcessorForGetIndexHistory : AbstractIndexHandlerProcessorForGetIndexHistory<ShardedRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForGetIndexHistory([NotNull] ShardedRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TransactionOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.ShardedContext.DatabaseName;
}
