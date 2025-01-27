using System;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.NotificationCenter;

namespace Raven.Server.Rachis.Commands
{
    public class DeleteNotificationCommand(string notificationId, NotificationsStorage storage) : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        public bool Deleted;

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            Deleted = storage.DeleteFromTable(notificationId, context.Transaction);
            return Deleted ? 1 : 0;
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
        {
            throw new NotImplementedException();
        }
    }
}
