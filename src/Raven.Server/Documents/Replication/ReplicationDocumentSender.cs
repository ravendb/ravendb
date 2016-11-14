using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client.Replication.Messages;
using Raven.Server.Extensions;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationDocumentSender
    {
        private readonly Logger _log;
        private long _lastEtag;
        private readonly SortedList<long, ReplicationBatchDocumentItem> _orderedReplicaItems;
        private readonly byte[] _tempBuffer = new byte[32 * 1024];
        private readonly Stream _stream;
        private readonly OutgoingReplicationHandler _parent;

        public ReplicationDocumentSender(Stream stream, OutgoingReplicationHandler parent, Logger log)
        {
            _log = log;
            _orderedReplicaItems = new SortedList<long, ReplicationBatchDocumentItem>();
            _stream = stream;
            _parent = parent;
        }

        public bool ExecuteReplicationOnce()
        {            
            var readTx = _parent._context.OpenReadTransaction();
            try
            {
                // we scan through the documents to send to the other side, we need to be careful about
                // filtering a lot of documents, because we need to let the other side know about this, and 
                // at the same time, we need to send a heartbeat to keep the tcp connection alive
                var sp = Stopwatch.StartNew();
                var timeout = Debugger.IsAttached ? 60*1000 : 1000;
                while (sp.ElapsedMilliseconds < timeout)
                {
                    _lastEtag = _parent._lastSentDocumentEtag;

                    _parent.CancellationToken.ThrowIfCancellationRequested();

                    var docs =
                        _parent._database.DocumentsStorage.GetDocumentsFrom(_parent._context,
                                _lastEtag + 1, 0, 1024)
                            .ToList();
                    var tombstones =
                        _parent._database.DocumentsStorage.GetTombstonesFrom(_parent._context,
                                _lastEtag + 1, 0, 1024)
                            .ToList();

                    long maxEtag;
                    maxEtag = _lastEtag;
                    if (docs.Count > 0)
                    {
                        maxEtag = docs[docs.Count - 1].Etag;
                    }

                    if (tombstones.Count > 0)
                    {
                        maxEtag = Math.Max(maxEtag, tombstones[tombstones.Count - 1].Etag);
                    }

                    foreach (var doc in docs)
                    {
                        if (doc.Etag > maxEtag)
                            break;
                        AddReplicationItemToBatch(new ReplicationBatchDocumentItem
                        {
                            Etag = doc.Etag,
                            ChangeVector = doc.ChangeVector,
                            Data = doc.Data,
                            Key = doc.Key
                        });
                    }

                    foreach (var tombstone in tombstones)
                    {
                        if (tombstone.Etag > maxEtag)
                            break;
                        AddReplicationItemToBatch(new ReplicationBatchDocumentItem
                        {
                            Etag = tombstone.Etag,
                            ChangeVector = tombstone.ChangeVector,
                            Collection = tombstone.Collection,
                            Key = tombstone.Key
                        });
                    }

                    // if we are at the end, we are done
                    if (_lastEtag <=
                        DocumentsStorage.ReadLastEtag(_parent._context.Transaction.InnerTransaction))
                    {
                        break;
                    }
                }
                if (_log.IsInfoEnabled)
                {
                    _log.Info(
                        $"Found {_orderedReplicaItems.Count:#,#;;0} documents to replicate to {_parent.Destination.Database} @ {_parent.Destination.Url} in {sp.ElapsedMilliseconds:#,#;;0} ms.");
                }

                if (_orderedReplicaItems.Count == 0)
                {
                    var hasModification = _lastEtag != _parent._lastSentDocumentEtag;
                    _parent._lastSentDocumentEtag = _lastEtag;
                    // ensure that the other server is aware that we skipped 
                    // on (potentially a lot of) documents to send, and we update
                    // the last etag they have from us on the other side
                    _parent.SendHeartbeat();
                    return hasModification;
                }

                _parent.CancellationToken.ThrowIfCancellationRequested();
                SendDocumentsBatch();
                return true;
            }
            finally
            {
                foreach(var item in _orderedReplicaItems)
                    item.Value.Data?.Dispose(); //item.Value.Data is null if tombstone
                _orderedReplicaItems.Clear();

                if (readTx.Disposed == false)
                    readTx.Dispose();
            }
        }


        private void AddReplicationItemToBatch(ReplicationBatchDocumentItem item)
        {
            if (ShouldSkipReplication(item.Key))
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info($"Skipping replication of {item.Key} because it is a system document");
                }
                return;
            }
            // destination already has it
            if (item.ChangeVector.GreaterThan(_parent._destinationLastKnownDocumentChangeVector) == false)
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info(
                        $"Skipping replication of {item.Key} because destination has a higher change vector. Doc: {item.ChangeVector.Format()} < Dest: {_parent._destinationLastKnownDocumentChangeVectorAsString} ");
                }
                return;
            }
            _lastEtag = Math.Max(_lastEtag, item.Etag);
            _orderedReplicaItems.Add(item.Etag, item);
        }


        private void SendDocumentsBatch()
        {
            if (_log.IsInfoEnabled)
                _log.Info(
                    $"Starting sending replication batch ({_parent._database.Name}) with {_orderedReplicaItems.Count:#,#;;0} docs, and last etag {_lastEtag}");

            var sw = Stopwatch.StartNew();
            try
            {
                var headerJson = new DynamicJsonValue
                {
                    ["Type"] = ReplicationMessageType.Documents,
                    ["LastDocumentEtag"] = _lastEtag,
                    ["ItemCount"] = _orderedReplicaItems.Count
                };
                _parent.WriteToServerAndFlush(headerJson);
                foreach (var item in _orderedReplicaItems)
                {
                    WriteDocumentToServer(item.Value.Key, item.Value.ChangeVector, item.Value.Data,
                        item.Value.Collection);
                }
            }
            finally //do try-finally as precaution
            {
                // we can release the read transaction while we are waiting for 
                // reply from the server and not hold it for a long time
                _parent._context.Transaction.Dispose();
            }
            _stream.Flush();
            sw.Stop();

            _parent._lastSentDocumentEtag = _lastEtag;

            if (_log.IsInfoEnabled && _orderedReplicaItems.Count > 0)
                _log.Info(
                    $"Finished sending replication batch. Sent {_orderedReplicaItems.Count:#,#;;0} documents in {sw.ElapsedMilliseconds:#,#;;0} ms. First sent etag = {_orderedReplicaItems[0].Etag}, last sent etag = {_lastEtag}");

            _parent._lastDocumentSentTime = DateTime.UtcNow;
            using (_parent._context.OpenReadTransaction())
            {
                _parent.HandleServerResponse();
            }
        }

        private unsafe void WriteDocumentToServer(
            LazyStringValue key,
            ChangeVectorEntry[] changeVector,
            BlittableJsonReaderObject data,
            LazyStringValue collection)
        {
            var changeVectorSize = changeVector.Length * sizeof(ChangeVectorEntry);
            var requiredSize = changeVectorSize +
                               sizeof(int) + // # of change vectors
                               sizeof(int) + // size of document key
                               key.Size +
                               sizeof(int) // size of document
                ;
            if (requiredSize > _tempBuffer.Length)
                ThrowTooManyChangeVectorEntries(key, changeVector);

            fixed (byte* pTemp = _tempBuffer)
            {
                int tempBufferPos = 0;
                fixed (ChangeVectorEntry* pChangeVectorEntries = changeVector)
                {
                    *(int*)pTemp = changeVector.Length;
                    tempBufferPos += sizeof(int);
                    Memory.Copy(pTemp + tempBufferPos, (byte*)pChangeVectorEntries, changeVectorSize);
                    tempBufferPos += changeVectorSize;
                }
                *(int*)(pTemp + tempBufferPos) = key.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, key.Buffer, key.Size);
                tempBufferPos += key.Size;

                //if data == null --> this is a tombstone, and a document otherwise
                if (data != null)
                {
                    *(int*)(pTemp + tempBufferPos) = data.Size;
                    tempBufferPos += sizeof(int);

                    var docReadPos = 0;
                    while (docReadPos < data?.Size)
                    {
                        var sizeToCopy = Math.Min(data.Size - docReadPos, _tempBuffer.Length - tempBufferPos);
                        if (sizeToCopy == 0) // buffer is full, need to flush it
                        {
                            _stream.Write(_tempBuffer, 0, tempBufferPos);
                            tempBufferPos = 0;
                            continue;
                        }
                        Memory.Copy(pTemp + tempBufferPos, data.BasePointer + docReadPos, sizeToCopy);
                        tempBufferPos += sizeToCopy;
                        docReadPos += sizeToCopy;
                    }
                }
                else
                {
                    //tombstone have size == -1
                    *(int*)(pTemp + tempBufferPos) = -1;
                    tempBufferPos += sizeof(int);

                    if (collection == null) //precaution
                    {
                        throw new InvalidDataException("Cannot write tombstone with empty collection name...");
                    }

                    *(int*)(pTemp + tempBufferPos) = collection.Size;
                    tempBufferPos += sizeof(int);
                    Memory.Copy(pTemp + tempBufferPos, collection.Buffer, collection.Size);
                    tempBufferPos += collection.Size;
                }
                _stream.Write(_tempBuffer, 0, tempBufferPos);
            }
        }

        private unsafe bool ShouldSkipReplication(LazyStringValue str)
        {
            if (str.Length < 6)
                return false;

            // case insensitive 'Raven/' match without doing allocations

            if ((str.Buffer[0] != (byte)'R' && str.Buffer[0] != (byte)'r') ||
                (str.Buffer[1] != (byte)'A' && str.Buffer[1] != (byte)'a') ||
                (str.Buffer[2] != (byte)'V' && str.Buffer[2] != (byte)'v') ||
                (str.Buffer[3] != (byte)'E' && str.Buffer[3] != (byte)'e') ||
                (str.Buffer[4] != (byte)'N' && str.Buffer[4] != (byte)'n') ||
                str.Buffer[5] != (byte)'/')
                return false;

            if (str.Length < 11)
                return true;

            // Now need to find if the next bits are 'hilo/'
            if ((str.Buffer[6] == (byte)'H' || str.Buffer[0] == (byte)'h') &&
                (str.Buffer[7] == (byte)'I' || str.Buffer[1] == (byte)'i') &&
                (str.Buffer[8] == (byte)'L' || str.Buffer[2] == (byte)'l') &&
                (str.Buffer[9] == (byte)'O' || str.Buffer[3] == (byte)'o') &&
                str.Buffer[10] == (byte)'/')
                return false;

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ThrowTooManyChangeVectorEntries(LazyStringValue key, ChangeVectorEntry[] changeVector)
        {
            throw new ArgumentOutOfRangeException("doc",
                "Document " + key + " has too many change vector entries to replicate: " +
                changeVector.Length);
        }

    }
}