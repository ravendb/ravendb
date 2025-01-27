using System;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Rachis.Commands
{
    public class StoreNotificationCommand(LazyStringValue id, DateTime createdAt, DateTime? postponedUntil, BlittableJsonReaderObject bjro, NotificationsStorage storage)
        : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        private readonly NotificationsStorage _storage = storage ?? throw new ArgumentNullException(nameof(storage));

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            _storage.Store(id, createdAt, postponedUntil,  bjro, context.Transaction);
            return 1;
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
        {
            throw new NotImplementedException();
        }
    }
}
