using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server;

namespace Raven.Server.Documents.Replication.Incoming
{
    public class IncomingMigrationReplicationHandler : IncomingReplicationHandler
    {
        public IncomingMigrationReplicationHandler(TcpConnectionOptions options, ReplicationLatestEtagRequest replicatedLastEtag, ReplicationLoader parent, JsonOperationContext.MemoryBuffer bufferToCopy, ReplicationLatestEtagRequest.ReplicationType replicationType) : base(options, replicatedLastEtag, parent, bufferToCopy, replicationType)
        {
        }

        protected override TransactionOperationsMerger.MergedTransactionCommand GetMergeDocumentsCommand(DataForReplicationCommand data, long lastDocumentEtag)
        {
            return new MergedIncomingMigrationCommand(data, lastDocumentEtag);
        }

        protected override void HandleHeartbeatMessage(DocumentsOperationContext documentsContext, BlittableJsonReaderObject message)
        {
            // do nothing
        }

        internal class MergedIncomingMigrationCommand : MergedDocumentReplicationCommand
        {
            public MergedIncomingMigrationCommand(DataForReplicationCommand replicationInfo, long lastEtag) : base(replicationInfo, lastEtag)
            {
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                // TODO: delete current items in the bucket?
                // TODO: handle the incoming properly
                return base.ExecuteCmd(context);
            }

            protected override void SaveSourceEtag(DocumentsOperationContext context)
            {
                // do nothing
            }
        }
    }
}
