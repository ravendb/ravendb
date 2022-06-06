using JetBrains.Annotations;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Notifications
{
    internal class ShardedDatabaseNotificationCenterHandlerProcessorForWatch : AbstractDatabaseNotificationCenterHandlerProcessorForWatch<ShardedDatabaseRequestHandler, TransactionOperationContext, ShardedOperation>
    {
        public ShardedDatabaseNotificationCenterHandlerProcessorForWatch([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override AbstractDatabaseNotificationCenter GetNotificationCenter() => RequestHandler.DatabaseContext.NotificationCenter;

        protected override AbstractOperations<ShardedOperation> GetOperations() => RequestHandler.DatabaseContext.Operations;

        protected override bool SupportsCurrentNode => true;

        protected override string GetDatabaseName()
        {
            return TryGetShardNumber(out var shardNumber) == false 
                ? RequestHandler.DatabaseName 
                : ShardHelper.ToShardName(RequestHandler.DatabaseName, shardNumber);
        }
    }
}
