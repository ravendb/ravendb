using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Handlers.Processors.Rachis;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Rachis
{
    internal class ShardedRachisHandlerProcessorForWaitForIndexNotifications : AbstractRachisHandlerProcessorForWaitForIndexNotifications<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedRachisHandlerProcessorForWaitForIndexNotifications([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask WaitForCommandsAsync(TransactionOperationContext context, WaitForIndexNotificationRequest commands)
        {
            foreach (var index in commands.RaftCommandIndexes)
            {
                await RequestHandler.WaitForIndexToBeAppliedAsync(context, index);
            }
        }
    }
}
