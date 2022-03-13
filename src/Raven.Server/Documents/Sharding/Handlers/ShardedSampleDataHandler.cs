using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedSampleDataHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/studio/sample-data", "POST")]
        public async Task CreateSampleData()
        {
            using (var processor = new ShardedSampleDataHandlerProcessorForPostSampleData<ShardedRequestHandler, TransactionOperationContext>(this))
            {
                await processor.ExecuteAsync();
            }
        }
    }
}
