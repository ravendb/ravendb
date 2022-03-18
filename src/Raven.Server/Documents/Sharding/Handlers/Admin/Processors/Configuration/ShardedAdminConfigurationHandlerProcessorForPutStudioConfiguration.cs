using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin.Processors.Configuration;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Configuration
{
    internal class ShardedAdminConfigurationHandlerProcessorForPutStudioConfiguration : AbstractAdminConfigurationHandlerProcessorForPutStudioConfiguration<ShardedRequestHandler, TransactionOperationContext>
    {
        public ShardedAdminConfigurationHandlerProcessorForPutStudioConfiguration(ShardedRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
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
