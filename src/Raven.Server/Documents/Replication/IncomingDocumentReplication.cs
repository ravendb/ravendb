using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

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
        public void ReceiveDocuments(DocumentsOperationContext context,
            BlittableJsonReaderArray docs)
        {
            var dbChangeVector = _database.DocumentsStorage.GetChangeVector(context);
            var changeVectorUpdated = false;
            var maxReceivedChangeVector = new Dictionary<Guid, long>();
            foreach (var member in docs)
            {
                var doc = member as BlittableJsonReaderObject;
                Debug.Assert(doc != null);

                var changeVector = doc.EnumerateChangeVector();
                foreach (var currentEntry in changeVector)
                {
                    Debug.Assert(currentEntry.DbId != Guid.Empty); //should never happen, but..
                    maxReceivedChangeVector[currentEntry.DbId] = currentEntry.Etag;
                }
                ReceiveReplicated(context, doc);
            }

            for (int i = 0; i < dbChangeVector.Length; i++)
            {
                long dbEtag;
                if (maxReceivedChangeVector.TryGetValue(dbChangeVector[i].DbId, out dbEtag) == false)
                    continue;
                maxReceivedChangeVector.Remove(dbChangeVector[i].DbId);
                if (dbEtag > dbChangeVector[i].Etag)
                {
                    changeVectorUpdated = true;
                    dbChangeVector[i].Etag = dbEtag;
                }
            }

            if (maxReceivedChangeVector.Count > 0)
            {
                changeVectorUpdated = true;
                var oldSize = dbChangeVector.Length;
                Array.Resize(ref dbChangeVector,oldSize + maxReceivedChangeVector.Count);

                foreach (var kvp in maxReceivedChangeVector)
                {
                    dbChangeVector[oldSize++] = new ChangeVectorEntry
                    {
                        DbId = kvp.Key,
                        Etag = kvp.Value,
                    };
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

            // we need to split this document to an independent blittable document
            // and this time, we'll prepare it for disk.
            doc.PrepareForStorage();
            _database.DocumentsStorage.Put(context, id, null, doc);
        }
    }
}
