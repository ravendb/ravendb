using System;
using Raven.Client.Extensions;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Util;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Utils;
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

        internal bool MatchesHubToSinkTask(PullReplicationAsSink destination)
        {
            if (IncomingPullReplicationParams.Mode != PullReplicationMode.HubToSink ||
                destination.Mode != PullReplicationMode.HubToSink)
                return false;

            return IncomingPullReplicationParams.TaskId == destination.TaskId;
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
                        long sinkEtag = (long)heartbeat[nameof(ReplicationMessageReply.CurrentEtag)];
                        _hubBatchHistory.Add(sinkEtag, _lastBatchChangeVector);
                    }

                    break;
            }

            // Here we check *locally* in the sink what is the last hub change vector that was replicated to all the nodes in the sink cluster
            // and persist it as the hub cursor for failover.
            // _lastBatchChangeVector is filled only when we send a batch of items OR we are skipping items through a heartbeat
            if (_hubBatchHistory.ComputeConfirmedChangeVector(_lastBatchChangeVector) is { } confirmedHubCv)
                PersistHubCursor(confirmedHubCv);

            return heartbeat;
        }

        private void PersistHubCursor(string confirmedHubCv)
        {
            var existingCv = ReplicationUtils.ReadCursorFromClusterFor(ReplicationLoaderParent.Server, ReplicationLoaderParent.Database.Name, IncomingPullReplicationParams.TaskId, ExternalReplicationState.ReplicationStateType.HubCursor);
            if (existingCv == confirmedHubCv)
                return;

            var command = new UpdateExternalReplicationStateCommand(ReplicationLoaderParent.Database.Name, RaftIdGenerator.NewId())
            {
                ExternalReplicationState = new ExternalReplicationState
                {
                    TaskId = IncomingPullReplicationParams.TaskId,
                    NodeTag = ReplicationLoaderParent._server.NodeTag,
                    SourceChangeVector = confirmedHubCv,
                    Type = ExternalReplicationState.ReplicationStateType.HubCursor,
                    FromToString = FromToString
                }
            };
            ReplicationLoaderParent._server.SendToLeaderAsync(command).IgnoreUnobservedExceptions();
        }

        protected override DocumentMergedTransactionCommand GetMergeDocumentsCommand(DocumentsOperationContext context, DataForReplicationCommand data, long lastDocumentEtag)
        {
            return ChangeVectorWireMode switch
            {
                PullReplicationChangeVectorWireMode.SendLegacyCompatible => new MergedLegacyPullReplicationOnSinkCommand(data, lastDocumentEtag),
                PullReplicationChangeVectorWireMode.SendAsIs => new MergedPullReplicationOnSinkCommand(data, lastDocumentEtag),
                _ => throw new ArgumentOutOfRangeException(nameof(ChangeVectorWireMode), ChangeVectorWireMode, "Unknown pull replication change-vector wire mode.")
            };
        }

        protected override void MergeSourceChangeVectorFromHeartbeat(DocumentsOperationContext documentsContext, string changeVector)
        {
            // SendAsIs keeps receiver-local Order; source progress is tracked by pull cursors, not by absorbing the source DB CV.
            if (ChangeVectorWireMode == PullReplicationChangeVectorWireMode.SendAsIs)
                return;

            if (string.IsNullOrEmpty(changeVector))
                return;

            RestoreKnownSinkEntriesFromLocalChangeVector(documentsContext, ref changeVector);
            base.MergeSourceChangeVectorFromHeartbeat(documentsContext, changeVector);
        }
    }
}
