using System;
using System.IO;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication.Senders
{
    public class ExternalReplicationDocumentSender : ReplicationDocumentSenderBase
    {
        public ExternalReplicationDocumentSender(Stream stream, DatabaseOutgoingReplicationHandler parent, Logger log) : base(stream, parent, log)
        {
        }

        protected override TimeSpan GetDelayReplication()
        {
            if (_parent.Destination is ExternalReplication external == false)
                return TimeSpan.Zero;

            _parent._parent._server.LicenseManager.AssertCanDelayReplication(external.DelayReplicationFor);
            return external.DelayReplicationFor;
        }

        protected override bool ShouldSkip(DocumentsOperationContext context, ReplicationBatchItem item, OutgoingReplicationStatsScope stats, SkippedReplicationItemsInfo skippedReplicationItemsInfo)
        {
            item.ChangeVector = context.GetChangeVector(item.ChangeVector).Order.StripMoveTag(context);
            return base.ShouldSkip(context, item, stats, skippedReplicationItemsInfo);
        }
    }
}
