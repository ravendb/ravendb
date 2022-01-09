using System.IO;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Documents.Replication.Senders;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication.Outgoing
{
    public abstract class OutgoingPullReplicationHandler : OutgoingReplicationHandlerBase
    {
        public string[] PathsToSend;
        private string[] _destinationAcceptablePaths;
        public ReplicationLoader.PullReplicationParams OutgoingPullReplicationParams;

        public string CertificateThumbprint;

        protected OutgoingPullReplicationHandler(ReplicationLoader parent, DocumentDatabase database, ReplicationNode node, TcpConnectionInfo connectionInfo) : 
            base(parent, database, node, connectionInfo)
        {
        }

        public override ReplicationDocumentSenderBase CreateDocumentSender(Stream stream, Logger logger)
        {
            return new FilteredReplicationDocumentSender(stream, this, logger, PathsToSend, _destinationAcceptablePaths);
        }

        protected override void ProcessHandshakeResponse((ReplicationMessageReply.ReplyType ReplyType, ReplicationMessageReply Reply) response)
        {
            base.ProcessHandshakeResponse(response);
            // this is used when the other side lets us know what paths it is going to accept from us
            // it supplements (but does not extend) what we are willing to send out 
            _destinationAcceptablePaths = response.Reply.AcceptablePaths;
        }
    }

    public class OutgoingPullReplicationHandlerAsHub : OutgoingPullReplicationHandler
    {
        // In case this is an outgoing pull replication from the hub
        // we need to associate this instance to the replication definition.
        public string PullReplicationDefinitionName;

        public OutgoingPullReplicationHandlerAsHub(ReplicationLoader parent, DocumentDatabase database, ExternalReplication node, TcpConnectionInfo connectionInfo) : 
            base(parent, database, node, connectionInfo)
        {
        }
        public override string FromToString => $"{base.FromToString} (pull definition: {PullReplicationDefinitionName})";
    }

    public class OutgoingPullReplicationHandlerAsSink : OutgoingPullReplicationHandler
    {
        public OutgoingPullReplicationHandlerAsSink(ReplicationLoader parent, DocumentDatabase database, PullReplicationAsSink node, TcpConnectionInfo connectionInfo) : 
            base(parent, database, node, connectionInfo)
        {
            CertificateThumbprint = _parent.GetCertificateForReplication(node, out _)?.Thumbprint;
        }

        protected override void ProcessHandshakeResponse((ReplicationMessageReply.ReplyType ReplyType, ReplicationMessageReply Reply) response)
        {
            base.ProcessHandshakeResponse(response);
            OutgoingPullReplicationParams = new ReplicationLoader.PullReplicationParams
            {
                PreventDeletionsMode = response.Reply.PreventDeletionsMode,
                Type = ReplicationLoader.PullReplicationParams.ConnectionType.Outgoing
            };
        }
    }
}
