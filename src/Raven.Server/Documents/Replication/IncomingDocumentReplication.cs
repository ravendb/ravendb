using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Raven.Abstractions.Extensions;

namespace Raven.Server.Documents.Replication
{
    public class IncomingDocumentReplication
    {
        private readonly DocumentDatabase _database;

        public IncomingDocumentReplication(DocumentDatabase database)
        {
            _database = database;
        }

        //by design this method won't handle opening and commit of the transaction
        //(that should happen at the calling code)
        public void ReceiveReplicatedDocuments(DocumentsOperationContext context,
            List<BlittableJsonReaderObject> docs)
        {
            var dbChangeVector = _database.DocumentsStorage.GetChangeVector(context);
            var changeVectorUpdated = false;
            var maxReceivedChangeVector = new Dictionary<Guid, long>();
            for (int i = 0; i < docs.Count; i++)
            {
                var doc = docs[i];
                var changeVector = doc.EnumerateChangeVector();
                for (int j = 0; j < changeVector.Length; j++)
                {
                    var currentEntry = changeVector[j];
                    Debug.Assert(currentEntry.DbId != Guid.Empty); //should never happen, but..

                    long existingValue;
                    if (!maxReceivedChangeVector.TryGetValue(currentEntry.DbId, out existingValue) ||
                        existingValue < currentEntry.Etag)
                        maxReceivedChangeVector[currentEntry.DbId] = currentEntry.Etag;
                    else
                        maxReceivedChangeVector.Add(currentEntry.DbId, currentEntry.Etag);
                }

                ReceiveReplicated(context, doc);
            }

            for (int i = 0; i < dbChangeVector.Length; i++)
            {
                var receivedVal = maxReceivedChangeVector.GetOrAdd(dbChangeVector[i].DbId, 0);
                if (receivedVal > dbChangeVector[i].Etag)
                {
                    changeVectorUpdated = true;
                    dbChangeVector[i].Etag = receivedVal;
                }
            }

            if (changeVectorUpdated)
                _database.DocumentsStorage.SetChangeVector(context, dbChangeVector);
        }

        private void ReceiveReplicated(DocumentsOperationContext context, BlittableJsonReaderObject doc)
        {
            var id = doc.GetIdFromMetadata();
            if (id == null)
                throw new InvalidDataException($"Missing {Constants.DocumentIdFieldName} field from a document; this is not something that should happen...");

            _database.DocumentsStorage.Put(context, id, null, doc);
        }
    }
}
