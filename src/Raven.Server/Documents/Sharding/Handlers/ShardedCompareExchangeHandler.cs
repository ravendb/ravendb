using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers.Processors.CompareExchange;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System.Processors;
using Raven.Server.Web.System.Processors.CompareExchange;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedCompareExchangeHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/cmpxchg", "GET")]
    public async Task GetCompareExchangeValues()
    {
        using (var processor = new ShardedCompareExchangeHandlerProcessorForGetCompareExchangeValues(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/cmpxchg", "PUT")]
    public async Task PutCompareExchangeValue()
    {
        using (var processor = new CompareExchangeHandlerProcessorForPutCompareExchangeValue(this, DatabaseContext.DatabaseName))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/cmpxchg", "DELETE")]
    public async Task DeleteCompareExchangeValue()
    {
        using (var processor = new CompareExchangeHandlerProcessorForDeleteCompareExchangeValue<TransactionOperationContext>(this))
            await processor.ExecuteAsync();
    }
}
