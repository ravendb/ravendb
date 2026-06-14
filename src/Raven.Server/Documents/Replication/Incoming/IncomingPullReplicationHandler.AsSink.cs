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
