using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Linq;
using Raven.Imports.Newtonsoft.Json.Utilities;
using Raven.Server.ReplicationUtil;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication
{
    //TODO : add DocumentReplicationStatistics that will track operational data
    public class DocumentReplicationExecuter: BaseReplicationExecuter
    {
        private readonly DocumentReplicationTransport _transport;
        private readonly Guid _srcDbId;
        private readonly ReplicationDestination _destination;
        private long _lastSentEtag;

        public DocumentReplicationExecuter(DocumentDatabase database, 
            Guid srcDbId, 
            ReplicationDestination destination) : base(database)
        {
            _srcDbId = srcDbId;
            _destination = destination;
            _transport = new DocumentReplicationTransport(destination.Url, _srcDbId,  _database.DatabaseShutdown);
            DocumentsOperationContext context;
            _database.DocumentsStorage.ContextPool.AllocateOperationContext(out context);

            //not sure about getting latest etag scheme; this probably needs more discussion
            if (_destination != null) //not null means that there is outgoing replication
                _lastSentEtag = _transport.GetLatestEtag();
        }

        //if _destination == null --> the replication is only incoming, otherwise it is a two-way one
        public override string Name => _srcDbId + (_destination != null ? $"/{_destination.Url}" : string.Empty);

        public Guid SrcDatabasebId => _srcDbId;

        //by design this method won't handle opening and commit of the transaction
        //(that should happen at the calling code)
        public void ReceiveReplicatedDocuments(DocumentsOperationContext context,
            List<BlittableJsonReaderObject> docs)
        {
            var dbChangeVector = _database.DocumentsStorage.GetChangeVector(context);
            var changeVectorUpdated = false;
            var maxReceivedChangeVector = new Dictionary<Guid,long>();
            for(int i = 0; i < docs.Count; i++)
            {
                var doc = docs[i];
                var changeVector = doc.EnumerateChangeVector();
                for (int j = 0; j < changeVector.Length; j++)
                {
                    var currentEntry = changeVector[j];
                    Debug.Assert(currentEntry.DbId != Guid.Empty); //should never happen, but..

                    long existingValue;
                    if(!maxReceivedChangeVector.TryGetValue(currentEntry.DbId,out existingValue) ||
                        existingValue < currentEntry.Etag)
                            maxReceivedChangeVector[currentEntry.DbId] = currentEntry.Etag;
                    else
                        maxReceivedChangeVector.Add(currentEntry.DbId,currentEntry.Etag);
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

            if(changeVectorUpdated)
                _database.DocumentsStorage.SetChangeVector(context,dbChangeVector);
        }

        public override int GetHashCode()
        {
            return _srcDbId.GetHashCode() ^ _destination.Url.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            var otherExecuter = obj as DocumentReplicationExecuter;
            if (otherExecuter == null)
                return false;
            return _srcDbId == otherExecuter._srcDbId &&
                _destination.Url.Equals(otherExecuter._destination.Url, StringComparison.OrdinalIgnoreCase);
        }

        private bool _shouldWaitForChanges;

        protected override bool ShouldWaitForChanges()
        {
            return _shouldWaitForChanges;
        }

        protected override void ExecuteReplicationOnce()
        {
            var lastSendEtag = _lastSentEtag;

            //just for shorter code
            var documentStorage = _database.DocumentsStorage;
            //TODO: handle here properly last etag
            //either add here negotiation for the etag, 
            //or add etag tracking
            DocumentsOperationContext context;			
            using (documentStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                //TODO: make replication batch size configurable
                //also, perhaps there should be timers/heuristics
                //that would dynamically resize batch size
                var replicationBatch =
                    documentStorage
                        .GetDocumentsAfter(context, lastSendEtag, 0, 1024)
                        .ToArray();

                //TODO : consider changing SendDocumentBatchAsync to sync version
                // (not sure it will be a bottleneck)
                AsyncHelpers.RunSync(() => _transport.SendDocumentBatchAsync(replicationBatch, context));
                _lastSentEtag = replicationBatch.Max(x => x.Etag);
                var lastExistingEtag = DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction);
                _shouldWaitForChanges = lastExistingEtag <= _lastSentEtag;
            }
        }

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
            _transport.Dispose();
            base.Dispose();
        }
    }
}
