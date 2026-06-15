using System;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication.Incoming
{
    public sealed class IncomingPullReplicationHandlerAsHub : IncomingPullReplicationHandler
    {
        private readonly PullReplicationBatchHistory _sinkBatchHistory;

        public IncomingPullReplicationHandlerAsHub(
            TcpConnectionOptions options,
            ReplicationLatestEtagRequest sourceHandshakeRequest,
            ReplicationLoader parent,
            JsonOperationContext.MemoryBuffer bufferToCopy,
            ReplicationLatestEtagRequest.ReplicationType replicationType,
            ReplicationLoader.PullReplicationParams incomingPullReplicationParams) : base(options, sourceHandshakeRequest, parent, bufferToCopy, replicationType, incomingPullReplicationParams)
        {
            _sinkBatchHistory = new PullReplicationBatchHistory(parent);
        }

        protected override bool PreventIncomingSinkDeletions =>
            IncomingPullReplicationParams.PreventDeletionsMode?.HasFlag(PreventDeletionsMode.PreventSinkToHubDeletions) == true;

        protected override void MergeSourceChangeVectorFromHeartbeat(DocumentsOperationContext documentsContext, string changeVector)
        {
            // do nothing
        }

        protected override DynamicJsonValue GetHeartbeatStatusMessage(DocumentsOperationContext documentsContext, long lastDocumentEtag, string handledMessageType)
        {
            var heartbeat = base.GetHeartbeatStatusMessage(documentsContext, lastDocumentEtag, handledMessageType);

            switch (handledMessageType)
            {
                case ReplicationMessageType.Documents:
                case ReplicationMessageType.Heartbeat:
                    if (string.IsNullOrEmpty(_lastBatchChangeVector) == false)
                    {
                        long hubEtag = (long)heartbeat[nameof(ReplicationMessageReply.CurrentEtag)];
                        _sinkBatchHistory.Add(hubEtag, _lastBatchChangeVector);
                    }

                    break;
            }

            // Here we report to the sink about the last sink change vector that was replicated to all the nodes in the hub cluster
            heartbeat[nameof(ReplicationMessageReply.LastConfirmedChangeVector)] = _sinkBatchHistory.ComputeConfirmedChangeVector(_lastBatchChangeVector);

            return heartbeat;
        }

        protected override DocumentMergedTransactionCommand GetMergeDocumentsCommand(DocumentsOperationContext context, DataForReplicationCommand data, long lastDocumentEtag)
        {
            return ChangeVectorShape switch
            {
                PullReplicationChangeVectorShape.Flat => new MergedFlatPullReplicationOnHubCommand(data, lastDocumentEtag, PreventIncomingSinkDeletions),
                PullReplicationChangeVectorShape.Composite => new MergedCompositePullReplicationOnHubCommand(data, lastDocumentEtag, PreventIncomingSinkDeletions),
                _ => throw new ArgumentOutOfRangeException(nameof(ChangeVectorShape), ChangeVectorShape, "Unknown pull replication change-vector shape.")
            };
        }
    }
}
