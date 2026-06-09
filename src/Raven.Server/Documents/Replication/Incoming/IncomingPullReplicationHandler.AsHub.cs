using System;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication.Incoming
{
    public sealed class IncomingPullReplicationHandlerAsHub : IncomingPullReplicationHandler
    {
        public IncomingPullReplicationHandlerAsHub(
            TcpConnectionOptions options,
            ReplicationLatestEtagRequest sourceHandshakeRequest,
            ReplicationLoader parent,
            JsonOperationContext.MemoryBuffer bufferToCopy,
            ReplicationLatestEtagRequest.ReplicationType replicationType,
            ReplicationLoader.PullReplicationParams incomingPullReplicationParams) : base(options, sourceHandshakeRequest, parent, bufferToCopy, replicationType, incomingPullReplicationParams)
        {
        }

        protected override bool PreventIncomingSinkDeletions =>
            IncomingPullReplicationParams.PreventDeletionsMode?.HasFlag(PreventDeletionsMode.PreventSinkToHubDeletions) == true;

        protected override void HandleHeartbeatMessage(DocumentsOperationContext documentsContext, string changeVector)
        {
            // TODO RavenDB-26295 / #22885: advance heartbeat progress (LastReplicatedEtagFrom) in the failover-cursor work.
            // This no-op also covers the flat SinkToHub lane, which previously advanced from heartbeat frames.
        }

        protected override DocumentMergedTransactionCommand GetMergeDocumentsCommand(DocumentsOperationContext context, DataForReplicationCommand data, long lastDocumentEtag)
        {
            return PullReplicationChangeVectorShape switch
            {
                ChangeVectorShape.Flat => new MergedFlatPullReplicationOnHubCommand(data, lastDocumentEtag, PreventIncomingSinkDeletions),
                ChangeVectorShape.Composite => new MergedCompositePullReplicationOnHubCommand(data, lastDocumentEtag, PreventIncomingSinkDeletions),
                _ => throw new ArgumentOutOfRangeException(nameof(PullReplicationChangeVectorShape), PullReplicationChangeVectorShape, "Unknown pull replication change-vector shape.")
            };
        }
    }
}
