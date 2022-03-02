using System.Threading.Tasks;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ShardedHandlers
{
    internal class ShardedGetStudioConfigurationHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/configuration/studio", "GET")]
        public async Task GetStudioConfiguration()
        {
            var studioConfiguration = ShardedContext.DatabaseRecord.Studio;
            using (var processor = new ShardedStudioConfigurationHandlerProcessor(this, ContextPool))
            {
                await processor.WriteStudioConfiguration(studioConfiguration);
            }
        }

        internal class ShardedStudioConfigurationHandlerProcessor : ConfigurationHandler.AbstractStudioConfigurationHandlerProcessor<ShardedRequestHandler, TransactionOperationContext>
        {
            public ShardedStudioConfigurationHandlerProcessor(ShardedRequestHandler requestHandler, TransactionContextPool transactionContextPool) : base(requestHandler, transactionContextPool) { }
        }
    }
}
