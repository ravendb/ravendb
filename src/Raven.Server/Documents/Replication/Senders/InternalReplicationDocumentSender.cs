using System.IO;
using Raven.Server.Documents.Replication.Outgoing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication.Senders
{
    public sealed class InternalReplicationDocumentSender : ReplicationDocumentSenderBase
    {
        public InternalReplicationDocumentSender(Stream stream, DatabaseOutgoingReplicationHandler parent, Logger log) : base(stream, parent, log)
        {
        }

        protected override bool ShouldUseLastEtagFromDestinationChangeVector()
        {
            // This is not related to failover. Filtered-pull items can be stored under a local storage etag,
            // but their replication order is FLTR, not this database id. If internal replication jumps its
            // scan start by the destination DB CV, it can skip those locally stored FLTR-ordered items before
            // ShouldSkip gets a chance to compare the actual item order.
            return false;
        }
    }
}
