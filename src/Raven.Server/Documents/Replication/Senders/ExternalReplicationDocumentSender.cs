using System;
using System.Diagnostics;
using System.IO;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Documents.Replication.Outgoing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication.Senders
{
    public class ExternalReplicationDocumentSender : ReplicationDocumentSenderBase
    {
        public ExternalReplicationDocumentSender(Stream stream, OutgoingReplicationHandlerBase parent, Logger log) : base(stream, parent, log)
        {
        }

        protected override TimeSpan GetDelayReplication()
        {
            if (_parent.Destination is ExternalReplication external == false)
                return TimeSpan.Zero;

            _parent._parent._server.LicenseManager.AssertCanDelayReplication(external.DelayReplicationFor);
            return external.DelayReplicationFor;
        }
    }
}
