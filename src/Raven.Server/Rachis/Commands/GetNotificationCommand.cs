using System;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis.Commands
{
    public class GetNotificationCommand(string id, NotificationsStorage storage) : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        public  NotificationTableValue NotificationTableValue;

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            NotificationTableValue = storage.Get(id, context, context.Transaction);
            return 1;
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
        {
            throw new NotImplementedException();
        }
    }
}
