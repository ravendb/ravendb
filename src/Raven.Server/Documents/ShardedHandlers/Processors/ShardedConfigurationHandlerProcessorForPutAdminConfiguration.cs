using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ShardedHandlers.Processors
{
    internal class ShardedConfigurationHandlerProcessorForPutAdminConfiguration : AbstractConfigurationHandlerProcessorForPutAdminConfiguration<ShardedRequestHandler, TransactionOperationContext>
    {
        public ShardedConfigurationHandlerProcessorForPutAdminConfiguration(ShardedRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask WaitForIndexNotificationAsync(long index)
        {
            await RequestHandler.ServerStore.Cluster.WaitForIndexNotification(index, RequestHandler.ServerStore.Engine.OperationTimeout);
        }

        protected override string GetDatabaseName()
        {
            return RequestHandler.ShardedContext.DatabaseName;
        }
    }
}
