using System;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication.Incoming
{
    public sealed class IncomingPullReplicationHandlerAsSink : IncomingPullReplicationHandler
    {
        public IncomingPullReplicationHandlerAsSink(
            TcpConnectionOptions options,
            ReplicationLatestEtagRequest sourceHandshakeRequest,
            ReplicationLoader parent,
            JsonOperationContext.MemoryBuffer bufferToCopy,
            ReplicationLatestEtagRequest.ReplicationType replicationType,
            ReplicationLoader.PullReplicationParams incomingPullReplicationParams) : base(options, sourceHandshakeRequest, parent, bufferToCopy, replicationType, incomingPullReplicationParams)
        {
        }

        protected override bool PreventIncomingSinkDeletions => false;

        protected override DocumentMergedTransactionCommand GetMergeDocumentsCommand(DocumentsOperationContext context, DataForReplicationCommand data, long lastDocumentEtag)
        {
            return PullReplicationChangeVectorShape switch
            {
                ChangeVectorShape.Flat => new MergedFlatPullReplicationOnSinkCommand(data, lastDocumentEtag),
                ChangeVectorShape.Composite => new MergedCompositePullReplicationOnSinkCommand(data, lastDocumentEtag),
                _ => throw new ArgumentOutOfRangeException(nameof(PullReplicationChangeVectorShape), PullReplicationChangeVectorShape, "Unknown pull replication change-vector shape.")
            };
        }

        protected override void HandleHeartbeatMessage(DocumentsOperationContext documentsContext, string changeVector)
        {
            if (PullReplicationChangeVectorShape == ChangeVectorShape.Composite)
                return;

            if (string.IsNullOrEmpty(changeVector))
                return;

            RestoreKnownSinkEntriesFromLocalChangeVector(documentsContext, ref changeVector);
            base.HandleHeartbeatMessage(documentsContext, changeVector);
        }
    }
}
