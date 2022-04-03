using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Expiration;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Expiration
{
    internal class ShardedExpirationHandlerProcessorForPost : AbstractExpirationHandlerProcessorForPost<ShardedDatabaseRequestHandler>
    {
        public ShardedExpirationHandlerProcessorForPost([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;

        protected override async ValueTask WaitForIndexNotificationAsync(long index)
        {
            await RequestHandler.ServerStore.Cluster.WaitForIndexNotification(index, RequestHandler.ServerStore.Engine.OperationTimeout);
        }
    }
}
