using System.Threading.Tasks;
using Raven.Server.Documents.ShardedHandlers.Processors;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ShardedHandlers;

public class ShardedCompareExchangeHandler : ShardedRequestHandler
{
    [RavenShardedAction("/databases/*/cmpxchg", "GET")]
    public async Task GetCompareExchangeValues()
    {
        using (var processor = new ShardedCompareExchangeHandlerProcessorForGetCompareExchangeValues(this, ShardedContext.DatabaseName))
            await processor.ExecuteAsync();
    }
}
