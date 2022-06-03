using System.Threading;
using JetBrains.Annotations;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Handlers.Processors;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Notifications
{
    internal class ShardedDatabaseNotificationCenterHandlerProcessorForGet : AbstractDatabaseNotificationCenterHandlerProcessorForGet<ShardedDatabaseRequestHandler, TransactionOperationContext, ShardedOperation>
    {
        public ShardedDatabaseNotificationCenterHandlerProcessorForGet([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override AbstractDatabaseNotificationCenter GetNotificationCenter() => RequestHandler.DatabaseContext.NotificationCenter;

        protected override AbstractOperations<ShardedOperation> GetOperations() => RequestHandler.DatabaseContext.Operations;

        protected override CancellationToken GetShutdownToken() => RequestHandler.DatabaseContext.DatabaseShutdown;
    }
}
