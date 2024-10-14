using System.IO;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Commands;
using Raven.Client.Util;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication.Outgoing
{
    public sealed class OutgoingExternalReplicationHandler : DatabaseOutgoingReplicationHandler
    {
        private long? _taskId;

        public OutgoingExternalReplicationHandler(ReplicationLoader parent, DocumentDatabase database, ExternalReplication node, TcpConnectionInfo connectionInfo) : 
            base(parent, database, node, connectionInfo)
        {
            _taskId = node.TaskId;
            DocumentsSend += (_) => UpdateExternalReplicationInfo();
        }

        protected override DynamicJsonValue GetInitialHandshakeRequest()
        {
            var json = base.GetInitialHandshakeRequest();
            if (_parent.Database is ShardedDocumentDatabase shardedDatabase)
                json[nameof(ReplicationLatestEtagRequest.ShardedDatabaseId)] = shardedDatabase.ShardedDatabaseId;

            return json;
        }

        public override ReplicationDocumentSenderBase CreateDocumentSender(Stream stream, Logger logger)
        {
            return new ExternalReplicationDocumentSender(stream, this, logger);
        }

        private void UpdateExternalReplicationInfo()
        {
            if (_taskId == null)
                return;

            var command = new UpdateExternalReplicationStateCommand(_database.Name, RaftIdGenerator.NewId())
            {
                ExternalReplicationState = new ExternalReplicationState
                {
                    TaskId = _taskId.Value,
                    NodeTag = _parent._server.NodeTag,
                    FromToString = FromToString,
                    LastSentEtag = _lastSentDocumentEtag,
                    SourceChangeVector = LastSentChangeVector,
                    DestinationChangeVector = LastAcceptedChangeVector
                }
            };

            // we don't wait to see if the command was applied on purpose
            _parent._server.SendToLeaderAsync(command)
                .IgnoreUnobservedExceptions();
        }
    }
}
