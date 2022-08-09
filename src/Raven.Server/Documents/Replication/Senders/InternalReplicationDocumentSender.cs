using System.IO;
using Raven.Server.Documents.Replication.Outgoing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication.Senders
{
    public class InternalReplicationDocumentSender : ReplicationDocumentSenderBase
    {
        public InternalReplicationDocumentSender(Stream stream, DatabaseOutgoingReplicationHandler parent, Logger log) : base(stream, parent, log)
        {
        }
    }
}
