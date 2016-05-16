using System;
using System.Collections.Generic;
using Raven.Server.ReplicationUtil;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication
{
    //TODO : add DocumentReplicationStatistics that will track operational data
    public class DocumentReplicationExecuter: BaseReplicationExecuter
    {
        private readonly ReplicationWebSocket _socket;
        private readonly DocumentReplicationDestination _config;

        public DocumentReplicationExecuter(DocumentDatabase database, DocumentReplicationDestination config) : base(database)
        {
            _config = config;
            _socket = new ReplicationWebSocket(config.Url);
        }

        public override string Name => _config.Url;

        //by design this method won't handle opening and commit of the transaction
        public void ReceiveReplicatedDocuments(DocumentsOperationContext context,
            IEnumerable<BlittableJsonReaderObject> docs)
        {
            //TODO: finish this
            var dbChangeVector = _database.DocumentsStorage.GetChangeVector(context);
            foreach (var doc in docs)
            {
                UpdateDbChangeVectorIfNeededFrom(doc);
                ReceiveReplicated(context, doc);
            }
        }

        private void UpdateDbChangeVectorIfNeededFrom(BlittableJsonReaderObject doc)
        {
            
        }

        protected override void ExecuteReplicationOnce()
        {
            throw new NotImplementedException();
        }

        private void ReceiveReplicated(DocumentsOperationContext context, BlittableJsonReaderObject doc)
        {

        }

        public override void Dispose()
        {
            _socket.Dispose();
            base.Dispose();
        }
    }
}
