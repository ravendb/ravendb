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
            // TODO: Temporary solution
            // Filtered-pull items can have a local storage etag whose order is FLTR, not the local database id.
            // Scanning by destination DB CV can skip those items before ordinary internal replication sends them.
            return false;
        }
    }
}
