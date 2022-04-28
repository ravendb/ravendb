using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System.Processors.CompareExchange;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.CompareExchange;

internal class ShardedCompareExchangeHandlerProcessorForGetCompareExchangeValues : AbstractCompareExchangeHandlerProcessorForGetCompareExchangeValues<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedCompareExchangeHandlerProcessorForGetCompareExchangeValues([NotNull] ShardedDatabaseRequestHandler requestHandler) 
        : base(requestHandler)
    {
    }
}
