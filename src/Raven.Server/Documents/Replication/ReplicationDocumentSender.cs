using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Extensions;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Raven.Server.ServerWide.Context;

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

        public class MergedReplicationBatchEnumerator : IEnumerator<ReplicationBatchDocumentItem>
        {
            private readonly List<IEnumerator<ReplicationBatchDocumentItem>> _workEnumerators = new List<IEnumerator<ReplicationBatchDocumentItem>>();
            private ReplicationBatchDocumentItem _currentItem;
            public void AddEnumerator(IEnumerator<ReplicationBatchDocumentItem> enumerator)
            {
                if(enumerator == null)
                    return;

                if (enumerator.MoveNext())
                {
                    _workEnumerators.Add(enumerator);
                }              
            }

            public bool MoveNext()
            {
                if (_workEnumerators.Count == 0)
                    return false;

                var current = _workEnumerators[0];
                for (var index = 1; index < _workEnumerators.Count; index++)
                {
                    if (_workEnumerators[index].Current.Etag < current.Current.Etag)
                    {
                        current = _workEnumerators[index];
                    }
                }
                _currentItem = current.Current;
                if (current.MoveNext() == false)
                {
                    _workEnumerators.Remove(current);
                }
                return true;
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            public ReplicationBatchDocumentItem Current => _currentItem;

            object IEnumerator.Current => Current;

            public void Dispose()
            {
            }
        }
        
        private IEnumerable<ReplicationBatchDocumentItem> GetDocsConflictsAndTombstonesAfter(DocumentsOperationContext ctx, long etag)
        {
            var docs = _parent._database.DocumentsStorage.GetDocumentsFrom(ctx, etag + 1);
            var tombs = _parent._database.DocumentsStorage.GetTombstonesFrom(ctx, etag + 1);
            var conflicts = _parent._database.DocumentsStorage.GetConflictsFrom(ctx, etag + 1);
            var versions = _parent._database.BundleLoader?.VersioningStorage?.GetRevisionsAfter(ctx, etag + 1);

            using (var docsIt = docs.GetEnumerator())
            using (var tombsIt = tombs.GetEnumerator())
            using (var conflictsIt = conflicts.GetEnumerator())
            using (var versionsIt = versions?.GetEnumerator())
            {
                var mergedInEnumerator = new MergedReplicationBatchEnumerator();

                mergedInEnumerator.AddEnumerator(docsIt);
                mergedInEnumerator.AddEnumerator(tombsIt);
                mergedInEnumerator.AddEnumerator(conflictsIt);
                mergedInEnumerator.AddEnumerator(versionsIt);
                
                while (mergedInEnumerator.MoveNext())
                {
                    yield return mergedInEnumerator.Current;
                }
            }
        }

        public bool ExecuteReplicationOnce()
        {
            DocumentsOperationContext documentsContext;
            using (_parent._database.DocumentsStorage.ContextPool.AllocateOperationContext(out documentsContext))
            using (documentsContext.OpenReadTransaction())
            {
                try
                {
                    // we scan through the documents to send to the other side, we need to be careful about
                    // filtering a lot of documents, because we need to let the other side know about this, and 
                    // at the same time, we need to send a heartbeat to keep the tcp connection alive
                    _lastEtag = _parent._lastSentDocumentEtag;
                    _parent.CancellationToken.ThrowIfCancellationRequested();

                    const int batchSize = 1024;//TODO: Make batchSize & maxSizeToSend configurable
                    const int maxSizeToSend = 16 * 1024 * 1024;
                    long size = 0;
                    int numberOfItemsSent = 0;
                    short lastTransactionMarker = -1;
                    foreach (var item in GetDocsConflictsAndTombstonesAfter(documentsContext, _lastEtag))
                    {
                        if (lastTransactionMarker != item.TransactionMarker)
                        // TODO: add a configuration option to disable this check
                        {
                            // we want to limit batch sizes to reasonable limits
                            if (size > maxSizeToSend || numberOfItemsSent > batchSize)
                                break;

                            lastTransactionMarker = item.TransactionMarker;
                        }

                        _lastEtag = item.Etag;

                        if (item.Data != null)
                            size += item.Data.Size;

                        AddReplicationItemToBatch(item);

                        numberOfItemsSent++;
                    }

                    if (_log.IsInfoEnabled)
                    {
                        _log.Info($"Found {_orderedReplicaItems.Count:#,#;;0} documents to replicate to {_parent.Destination.Database} @ {_parent.Destination.Url}");
                    }

                    if (_orderedReplicaItems.Count == 0)
                    {
                        var hasModification = _lastEtag != _parent._lastSentDocumentEtag;

                        // ensure that the other server is aware that we skipped 
                        // on (potentially a lot of) documents to send, and we update
                        // the last etag they have from us on the other side
                        _parent._lastSentDocumentEtag = _lastEtag;
                        _parent._lastDocumentSentTime = DateTime.UtcNow;
                        _parent.SendHeartbeat();
                        return hasModification;
                    }

                    _parent.CancellationToken.ThrowIfCancellationRequested();
                    
                    try
                    {
                        SendDocumentsBatch(documentsContext);
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Failed to send document replication batch", e);
                        throw;
                    }
                    return true;
                }
                finally
                {
                    foreach (var item in _orderedReplicaItems)
                        item.Value.Data?.Dispose(); //item.Value.Data is null if tombstone
                    _orderedReplicaItems.Clear();
                }
            }
        }


        private unsafe void AddReplicationItemToBatch(ReplicationBatchDocumentItem item)
        {
            if ((item.Flags & DocumentFlags.Artificial) == DocumentFlags.Artificial)
            {
                if (_log.IsInfoEnabled)
                {
                    _log.Info($"Skipping replication of {item.Key} because it is an artificial document");
                }
                return;
            }
            if (CollectionName.IsSystemDocument(item.Key.Buffer, item.Key.Size))
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
            _orderedReplicaItems.Add(item.Etag, item);
        }


        private void SendDocumentsBatch(DocumentsOperationContext documentsContext)
        {
            if (_log.IsInfoEnabled)
                _log.Info(
                    $"Starting sending replication batch ({_parent._database.Name}) with {_orderedReplicaItems.Count:#,#;;0} docs, and last etag {_lastEtag}");

            var stats = new ReplicationStatistics.OutgoingBatchStats
            {
                Status = ReplicationStatus.Sending,
                DocumentsCount = _orderedReplicaItems.Count,
                StartSendingTime = DateTime.UtcNow,
                SentEtagMin = _parent._lastSentDocumentEtag + 1,
                SentEtagMax = _lastEtag,
                Destination = _parent.FromToString
            };
            var sw = Stopwatch.StartNew();
            var defaultResolver = _parent._parent.ReplicationDocument?.DefaultResolver;
            var headerJson = new DynamicJsonValue
            {
                [nameof(ReplicationMessageHeader.Type)] = ReplicationMessageType.Documents,
                [nameof(ReplicationMessageHeader.LastDocumentEtag)] = _lastEtag,
                [nameof(ReplicationMessageHeader.LastIndexOrTransformerEtag)] = _parent._lastSentIndexOrTransformerEtag,
                [nameof(ReplicationMessageHeader.ItemCount)] = _orderedReplicaItems.Count,
                [nameof(ReplicationMessageHeader.ResolverId)] = defaultResolver?.ResolvingDatabaseId,
                [nameof(ReplicationMessageHeader.ResolverVersion)] = defaultResolver?.Version
            };

            _parent.WriteToServer(headerJson);            
            foreach (var item in _orderedReplicaItems)
            {
                WriteDocumentToServer(item.Value);
            }
            // close the transaction as early as possible, and before we wait for reply
            // from other side
            documentsContext.Transaction.Dispose();
            _stream.Flush();
            sw.Stop();

            stats.EndSendingTime = DateTime.UtcNow;
            _parent.ReplicationStats.Add(stats);

            _parent._lastSentDocumentEtag = _lastEtag;

            if (_log.IsInfoEnabled && _orderedReplicaItems.Count > 0)
                _log.Info(
                    $"Finished sending replication batch. Sent {_orderedReplicaItems.Count:#,#;;0} documents in {sw.ElapsedMilliseconds:#,#;;0} ms. Last sent etag = {_lastEtag}");

            _parent._lastDocumentSentTime = DateTime.UtcNow;
            _parent.HandleServerResponse();            
            
        }

        private unsafe void WriteDocumentToServer(ReplicationBatchDocumentItem item)
        {
            var changeVectorSize = item.ChangeVector.Length * sizeof(ChangeVectorEntry);
            var requiredSize = changeVectorSize +
                               sizeof(int) + // # of change vectors
                               sizeof(int) + // size of document key
                               item.Key.Size +
                               sizeof(int) + // size of document
                               sizeof(short) // transaction marker
                ;
            if (requiredSize > _tempBuffer.Length)
                ThrowTooManyChangeVectorEntries(item.Key, item.ChangeVector);

            fixed (byte* pTemp = _tempBuffer)
            {
                int tempBufferPos = 0;
                fixed (ChangeVectorEntry* pChangeVectorEntries = item.ChangeVector)
                {
                    *(int*)pTemp = item.ChangeVector.Length;
                    tempBufferPos += sizeof(int);
                    Memory.Copy(pTemp + tempBufferPos, (byte*)pChangeVectorEntries, changeVectorSize);
                    tempBufferPos += changeVectorSize;
                }

                *(short*)(pTemp + tempBufferPos) = item.TransactionMarker;
                tempBufferPos += sizeof(short);

                *(long*)(pTemp + tempBufferPos) = item.LastModifiedTicks;
                tempBufferPos += sizeof(long);

                *(DocumentFlags*)(pTemp + tempBufferPos) = item.Flags;
                tempBufferPos += sizeof(DocumentFlags);

                *(int*)(pTemp + tempBufferPos) = item.Key.Size;
                tempBufferPos += sizeof(int);

                Memory.Copy(pTemp + tempBufferPos, item.Key.Buffer, item.Key.Size);
                tempBufferPos += item.Key.Size;

                //if data == null --> this is a tombstone, and a document otherwise
                if (item.Data != null)
                {
                    *(int*)(pTemp + tempBufferPos) = item.Data.Size;
                    tempBufferPos += sizeof(int);

                    var docReadPos = 0;
                    while (docReadPos < item.Data?.Size)
                    {
                        var sizeToCopy = Math.Min(item.Data.Size - docReadPos, _tempBuffer.Length - tempBufferPos);
                        if (sizeToCopy == 0) // buffer is full, need to flush it
                        {
                            _stream.Write(_tempBuffer, 0, tempBufferPos);
                            tempBufferPos = 0;
                            continue;
                        }
                        Memory.Copy(pTemp + tempBufferPos, item.Data.BasePointer + docReadPos, sizeToCopy);
                        tempBufferPos += sizeToCopy;
                        docReadPos += sizeToCopy;
                    }
                }
                else
                {
                    //tombstone have size == -1
                    *(int*)(pTemp + tempBufferPos) = -1;
                    tempBufferPos += sizeof(int);

                    if (item.Collection == null) //precaution
                    {
                        throw new InvalidDataException("Cannot write tombstone with empty collection name...");
                    }

                    *(int*)(pTemp + tempBufferPos) = item.Collection.Size;
                    tempBufferPos += sizeof(int);
                    Memory.Copy(pTemp + tempBufferPos, item.Collection.Buffer, item.Collection.Size);
                    tempBufferPos += item.Collection.Size;
                }

                _stream.Write(_tempBuffer, 0, tempBufferPos);
            }
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