using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Analyzers;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedAnalyzersHandler : ShardedRequestHandler
{
    [RavenShardedAction("/databases/*/analyzers", "GET")]
    public async Task Get()
    {
        using (var processor = new AnalyzersHandlerProcessorForGet<TransactionOperationContext>(this, ContextPool, ShardedContext.DatabaseName))
            await processor.ExecuteAsync();
    }
}
