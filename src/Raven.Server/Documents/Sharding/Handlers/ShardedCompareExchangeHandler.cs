using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.Web.System.Processors;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedCompareExchangeHandler : ShardedRequestHandler
{
    [RavenShardedAction("/databases/*/cmpxchg", "GET")]
    public async Task GetCompareExchangeValues()
    {
        using (var processor = new ShardedCompareExchangeHandlerProcessorForGetCompareExchangeValues(this, ShardedContext.DatabaseName))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/cmpxchg", "PUT")]
    public async Task PutCompareExchangeValue()
    {
        using (var processor = new CompareExchangeHandlerProcessorForPutCompareExchangeValue(this, ShardedContext.DatabaseName))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/cmpxchg", "DELETE")]
    public async Task DeleteCompareExchangeValue()
    {
        using (var processor = new CompareExchangeHandlerProcessorForDeleteCompareExchangeValue(this, ShardedContext.DatabaseName))
            await processor.ExecuteAsync();
    }
}
