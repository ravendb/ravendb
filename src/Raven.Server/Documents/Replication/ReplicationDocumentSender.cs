using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client.Replication.Messages;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationDocumentSender : IDisposable
    {
        public class ReplicationContext
        {
            public DocumentsOperationContext OperationContext;

            public Reference<long> LastSentEtag;

            public CancellationToken Token;

            public DocumentsStorage DocumentsStorage;

            public Action SendHeartbeat;

            public Dictionary<Guid, long> LastKnownChangeVector;

            public Reference<string> LastKnownChangeVectorAsString;

            public Action HandleServerResponse;

            public Action<DynamicJsonValue> WriteToServerAndFlush;

            public ReplicationDestination Destination;

            public string DatabaseName;
            public Reference<DateTime> LastSentTime;
        }

        private readonly Logger _log;
        private long _lastEtag;
        private readonly SortedList<long, ReplicationBatchItem> _orderedReplicaItems;
        private readonly byte[] _tempBuffer = new byte[32 * 1024];
        private readonly Stream _stream;
        private readonly ReplicationContext _replicationContext;

        public ReplicationDocumentSender(Stream stream, ReplicationContext replicationContext, Logger log)
        {
            _log = log;
            _orderedReplicaItems = new SortedList<long, ReplicationBatchItem>();
            _stream = stream;
            _replicationContext = replicationContext;
        }

        public Stream Stream => _stream;

        public bool ExecuteReplicationOnce()
        {
            _orderedReplicaItems.Clear();
            var readTx = _replicationContext.OperationContext.OpenReadTransaction();
            try
            {

                // we scan through the documents to send to the other side, we need to be careful about
                // filtering a lot of documents, because we need to let the other side know about this, and 
                // at the same time, we need to send a heartbeat to keep the tcp connection alive
                var sp = Stopwatch.StartNew();
                var timeout = Debugger.IsAttached ? 60*1000 : 1000;
                while (sp.ElapsedMilliseconds < timeout)
                {
                    _lastEtag = _replicationContext.LastSentEtag.Value;

                    _replicationContext.Token.ThrowIfCancellationRequested();

                    var docs =
                        _replicationContext.DocumentsStorage.GetDocumentsFrom(_replicationContext.OperationContext,
                                _lastEtag + 1, 0, 1024)
                            .ToList();
                    var tombstones =
                        _replicationContext.DocumentsStorage.GetTombstonesFrom(_replicationContext.OperationContext,
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
                        AddReplicationItemToBatch(new ReplicationBatchItem
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
                        AddReplicationItemToBatch(new ReplicationBatchItem
                        {
                            Etag = tombstone.Etag,
                            ChangeVector = tombstone.ChangeVector,
                            Collection = tombstone.Collection,
                            Key = tombstone.Key
                        });
                    }

                    // if we are at the end, we are done
                    if (_lastEtag <=
                        DocumentsStorage.ReadLastEtag(_replicationContext.OperationContext.Transaction.InnerTransaction))
                    {
                        break;
                    }
                }

                if (_log.IsInfoEnabled)
                {
                    _log.Info(
                        $"Found {_orderedReplicaItems.Count:#,#;;0} documents to replicate to {_replicationContext.Destination.Database} @ {_replicationContext.Destination.Url} in {sp.ElapsedMilliseconds:#,#;;0} ms.");
                }

                if (_orderedReplicaItems.Count == 0)
                {
                    var hasModification = _lastEtag != _replicationContext.LastSentEtag.Value;
                    _replicationContext.LastSentEtag.Value = _lastEtag;
                    // ensure that the other server is aware that we skipped 
                    // on (potentially a lot of) documents to send, and we update
                    // the last etag they have from us on the other side
                    _replicationContext.SendHeartbeat();
                    return hasModification;
                }

                _replicationContext.Token.ThrowIfCancellationRequested();

                SendDocuments();
                return true;
            }
            finally
            {
                if (readTx.Disposed == false)
                    readTx.Dispose();
            }
        }


        private void AddReplicationItemToBatch(ReplicationBatchItem item)
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
            if (item.ChangeVector.GreaterThan(_replicationContext.LastKnownChangeVector) == false)
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info(
                        $"Skipping replication of {item.Key} because destination has a higher change vector. Doc: {item.ChangeVector.Format()} < Dest: {_replicationContext.LastKnownChangeVectorAsString} ");
                }
                return;
            }
            _lastEtag = Math.Max(_lastEtag, item.Etag);
            _orderedReplicaItems.Add(item.Etag, item);
        }


        private void SendDocuments()
        {
            if (_log.IsInfoEnabled)
                _log.Info(
                    $"Starting sending replication batch ({_replicationContext.DatabaseName}) with {_orderedReplicaItems.Count:#,#;;0} docs, and last etag {_lastEtag}");

            var sw = Stopwatch.StartNew();
            var headerJson = new DynamicJsonValue
            {
                ["Type"] = "ReplicationBatch",
                ["LastEtag"] = _lastEtag,
                ["Documents"] = _orderedReplicaItems.Count
            };
            _replicationContext.WriteToServerAndFlush(headerJson);

            foreach (var item in _orderedReplicaItems)
            {
                WriteDocumentToServer(item.Value.Key, item.Value.ChangeVector, item.Value.Data, item.Value.Collection);
            }

            // we can release the read transaction while we are waiting for 
            // reply from the server and not hold it for a long time
            _replicationContext.OperationContext.Transaction.Dispose();

            _stream.Flush();
            sw.Stop();

            _replicationContext.LastSentEtag.Value = _lastEtag;

            if (_log.IsInfoEnabled && _orderedReplicaItems.Count > 0)
                _log.Info(
                    $"Finished sending replication batch. Sent {_orderedReplicaItems.Count:#,#;;0} documents in {sw.ElapsedMilliseconds:#,#;;0} ms. First sent etag = {_orderedReplicaItems[0].Etag}, last sent etag = {_lastEtag}");

            _replicationContext.LastSentTime.Value = DateTime.UtcNow;
            using (_replicationContext.OperationContext.OpenReadTransaction())
            {
                _replicationContext.HandleServerResponse();
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

        public void Dispose()
        {
            
        }
    }
}