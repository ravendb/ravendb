using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Sorters;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Sorters;

internal class ShardedSortersHandlerProcessorForGet : AbstractSortersHandlerProcessorForGet<TransactionOperationContext>
{
    public ShardedSortersHandlerProcessorForGet([NotNull] ShardedDatabaseRequestHandler requestHandler) 
        : base(requestHandler)
    {
    }
}
