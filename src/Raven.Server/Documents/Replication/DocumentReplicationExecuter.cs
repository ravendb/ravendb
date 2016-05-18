using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Linq;
using Raven.Imports.Newtonsoft.Json.Utilities;
using Raven.Server.ReplicationUtil;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication
{
    //TODO : add DocumentReplicationStatistics that will track operational data
    //TODO : do not forget to handle authentication stuff
    public class DocumentReplicationExecuter: BaseReplicationExecuter
    {
        private readonly ReplicationWebSocket _socket;
        private readonly Guid _srcDbId;
        private readonly ReplicationDestination _destination;
        private long _lastSentEtag;

        public DocumentReplicationExecuter(DocumentDatabase database, 
            Guid srcDbId, 
            ReplicationDestination destination, 
            long lastSentEtag) : base(database)
        {
            _srcDbId = srcDbId;
            _destination = destination;
            _lastSentEtag = lastSentEtag;
            _socket = new ReplicationWebSocket(destination.Url);		
        }

        //if _destination == null --> the replication is only incoming, otherwise it is a two-way one
        public override string Name => _srcDbId + (_destination != null ? $"/{_destination.Url}" : string.Empty);

        //by design this method won't handle opening and commit of the transaction
        //(that should happen at the calling code)
        public void ReceiveReplicatedDocuments(DocumentsOperationContext context,
            List<BlittableJsonReaderObject> docs)
        {
            var dbChangeVector = _database.DocumentsStorage.GetChangeVector(context);
            var changeVectorUpdated = false;
            for(int i = 0; i < docs.Count; i++)
            {
                var doc = docs[i];
                changeVectorUpdated = UpdateDbChangeVectorIfNeededFrom(doc,dbChangeVector);
                ReceiveReplicated(context, doc);
            }

            if(changeVectorUpdated)
                _database.DocumentsStorage.SetChangeVector(context,dbChangeVector);
        }

        private bool UpdateDbChangeVectorIfNeededFrom(BlittableJsonReaderObject doc,ChangeVectorEntry[] dbChangeVector)
        {
            var docChangeVector = doc.EnumerateChangeVector();
            var wasUpdated = false;
            foreach (var entry in docChangeVector)
            {
                var indexOfDbEntry = dbChangeVector.IndexOf(e => e.DbId == entry.DbId);
                if (indexOfDbEntry != -1 && dbChangeVector[indexOfDbEntry].Etag < entry.Etag)
                {
                    dbChangeVector[indexOfDbEntry].Etag = entry.Etag;
                    wasUpdated = true;
                }
            }

            return wasUpdated;
        }

        protected override void ExecuteReplicationOnce()
        {
            
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReceiveReplicated(DocumentsOperationContext context, BlittableJsonReaderObject doc)
        {
            var idAsObject = doc[Constants.DocumentIdFieldName];
            if(idAsObject == null)
                throw new InvalidDataException($"Missing {Constants.DocumentIdFieldName} field from a document; this is not something that should happen...");

            var id = idAsObject.ToString();
            _database.DocumentsStorage.Put(context, id, null ,doc);
        }

        public override void Dispose()
        {
            _socket.Dispose();
            base.Dispose();
        }
    }
}
