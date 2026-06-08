using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Replication.Incoming
{
    public abstract partial class IncomingPullReplicationHandler
    {
        private static ChangeVector CreateReceiverLocalOrderAndApplyIncomingVersion(DocumentsOperationContext context, ReplicationBatchItem item)
        {
            var incomingChangeVector = context.GetChangeVector(item.ChangeVector);
            var etag = context.DocumentDatabase.DocumentsStorage.GenerateNextEtag();
            var receiverLocalOrder = context.DocumentDatabase.DocumentsStorage.GetNewChangeVector(context, etag);

            // Store receiver-local order with the incoming version lineage.
            item.ChangeVector = context.GetChangeVector(incomingChangeVector.Version, receiverLocalOrder);
            return receiverLocalOrder;
        }

        internal sealed class MergedCompositePullReplicationOnSinkCommand : MergedDocumentReplicationCommand
        {
            public MergedCompositePullReplicationOnSinkCommand(DataForReplicationCommand replicationInfo, long lastEtag) : base(replicationInfo, lastEtag)
            {
            }

            protected override ChangeVector PreProcessItem(DocumentsOperationContext context, ReplicationBatchItem item)
            {
                return CreateReceiverLocalOrderAndApplyIncomingVersion(context, item);
            }
        }

        internal sealed class MergedCompositePullReplicationOnHubCommand : MergedDocumentReplicationCommand
        {
            private readonly bool _preventIncomingSinkDeletions;

            public MergedCompositePullReplicationOnHubCommand(DataForReplicationCommand replicationInfo, long lastEtag, bool preventIncomingSinkDeletions) : base(replicationInfo, lastEtag)
            {
                _preventIncomingSinkDeletions = preventIncomingSinkDeletions;
            }

            protected override ChangeVector PreProcessItem(DocumentsOperationContext context, ReplicationBatchItem item)
            {
                RemoveExpiresFromSinkBatchItem(context, item, _preventIncomingSinkDeletions);
                return CreateReceiverLocalOrderAndApplyIncomingVersion(context, item);
            }
        }
    }
}
