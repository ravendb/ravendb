using System.IO;
using Raven.Server.Documents.Replication.Outgoing;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.Replication.Senders
{
    public sealed class InternalReplicationDocumentSender : ReplicationDocumentSenderBase
    {
        public InternalReplicationDocumentSender(Stream stream, DatabaseOutgoingReplicationHandler parent, RavenLogger log) : base(stream, parent, log)
        {
        }
    }
}
