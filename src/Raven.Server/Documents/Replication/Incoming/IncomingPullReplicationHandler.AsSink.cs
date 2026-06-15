using System;
using Raven.Client.Extensions;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Util;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication.Incoming
{
    public sealed class IncomingPullReplicationHandlerAsSink : IncomingPullReplicationHandler
    {
        private readonly PullReplicationBatchHistory _hubBatchHistory;

        public IncomingPullReplicationHandlerAsSink(
            TcpConnectionOptions options,
            ReplicationLatestEtagRequest sourceHandshakeRequest,
            ReplicationLoader parent,
            JsonOperationContext.MemoryBuffer bufferToCopy,
            ReplicationLatestEtagRequest.ReplicationType replicationType,
            ReplicationLoader.PullReplicationParams incomingPullReplicationParams) : base(options, sourceHandshakeRequest, parent, bufferToCopy, replicationType, incomingPullReplicationParams)
        {
            _hubBatchHistory = new PullReplicationBatchHistory(parent);
        }

        protected override bool PreventIncomingSinkDeletions => false;

        protected override DynamicJsonValue GetHeartbeatStatusMessage(DocumentsOperationContext documentsContext, long lastDocumentEtag, string handledMessageType)
        {
            var heartbeat = base.GetHeartbeatStatusMessage(documentsContext, lastDocumentEtag, handledMessageType);

            switch (handledMessageType)
            {
                case ReplicationMessageType.Documents:
                case ReplicationMessageType.Heartbeat:
                    if (string.IsNullOrEmpty(_lastBatchChangeVector) == false)
                    {
                        long sinkEtag = (long)heartbeat[nameof(ReplicationMessageReply.CurrentEtag)];
                        _hubBatchHistory.Add(sinkEtag, _lastBatchChangeVector);
                    }

                    break;
            }

            // Here we check *locally* in the sink what is the last hub change vector that was replicated to all the nodes in the sink cluster
            // and persist it as the hub cursor for failover.
            if (_hubBatchHistory.ComputeConfirmedChangeVector(_lastBatchChangeVector) is { } confirmedHubCv)
            {
                var command = new UpdateExternalReplicationStateCommand(ReplicationLoaderParent.Database.Name, RaftIdGenerator.NewId())
                {
                    ExternalReplicationState = new ExternalReplicationState
                    {
                        TaskId = IncomingPullReplicationParams.TaskId,
                        NodeTag = ReplicationLoaderParent._server.NodeTag,
                        SourceChangeVector = confirmedHubCv,
                        Type = ExternalReplicationState.ReplicationStateType.HubCursor
                    }
                };
                ReplicationLoaderParent._server.SendToLeaderAsync(command).IgnoreUnobservedExceptions();
            }

            return heartbeat;
        }

        protected override DocumentMergedTransactionCommand GetMergeDocumentsCommand(DocumentsOperationContext context, DataForReplicationCommand data, long lastDocumentEtag)
        {
            return ChangeVectorShape switch
            {
                PullReplicationChangeVectorShape.Flat => new MergedFlatPullReplicationOnSinkCommand(data, lastDocumentEtag),
                PullReplicationChangeVectorShape.Composite => new MergedCompositePullReplicationOnSinkCommand(data, lastDocumentEtag),
                _ => throw new ArgumentOutOfRangeException(nameof(ChangeVectorShape), ChangeVectorShape, "Unknown pull replication change-vector shape.")
            };
        }

        protected override void MergeSourceChangeVectorFromHeartbeat(DocumentsOperationContext documentsContext, string changeVector)
        {
            if (ChangeVectorShape == PullReplicationChangeVectorShape.Composite)
                return;

            if (string.IsNullOrEmpty(changeVector))
                return;

            RestoreKnownSinkEntriesFromLocalChangeVector(documentsContext, ref changeVector);
            base.MergeSourceChangeVectorFromHeartbeat(documentsContext, changeVector);
        }
    }
}
