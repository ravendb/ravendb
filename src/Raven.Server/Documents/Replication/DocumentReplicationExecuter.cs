using System;
using System.Collections.Generic;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Server.ReplicationUtil;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication
{
    //TODO : add DocumentReplicationStatistics that will track operational data
    public class DocumentReplicationExecuter: BaseReplicationExecuter
    {
        private readonly DocumentReplicationTransport _transport;
        private readonly IncomingDocumentReplication _incoming;
        private readonly OutgoingDocumentReplication _outgoing;
        private readonly ReplicationDestination _destination;

        public DocumentReplicationExecuter(DocumentDatabase database, string url, ReplicationDestination destination) : base(database)
        {
            _incoming = new IncomingDocumentReplication(database);
            Url = url;
            _destination = destination;
            if (_destination != null)
            {
                DocumentsOperationContext context;
                database.DocumentsStorage.ContextPool.AllocateOperationContext(out context);
                _transport = new DocumentReplicationTransport(
                    _destination.Url,
                    _database.DbId,
                    _database.Name, 
                    _destination.Database,
                    _database.DatabaseShutdown,
                    context);
                _outgoing = new OutgoingDocumentReplication(database, _transport);
            }
        }

        public override string ReplicationUniqueName => _database.DbId.ToString();

        public string Url { get; }
        public Guid DbId => _database.DbId;
        public string DbName => _database.Name;

        public void ReceiveReplicatedDocuments(DocumentsOperationContext context, 
            List<BlittableJsonReaderObject> docs) => _incoming.ReceiveReplicatedDocuments(context, docs);

        protected override void ExecuteReplicationOnce() => _outgoing?.ExecuteReplicationOnce();

        protected override bool HasMoreDocumentsToSend() => _outgoing?.HasMoreDocumentsToSend ?? true;

        public bool HasOutgoingReplication => _outgoing != null;

        public override void Dispose()
        {
            _outgoing?.Dispose();
            _transport?.Dispose();		    
            base.Dispose();
        }
    }
}
