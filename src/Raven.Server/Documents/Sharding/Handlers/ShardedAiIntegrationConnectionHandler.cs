using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedAiIntegrationConnectionHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/ai/test-connection", "POST")]
    public async Task TestAiConnection()
    {
        using (var processor = new AiIntegrationHandlerProcessorForTestAiConnection<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
            await processor.ExecuteAsync();
    }
}
