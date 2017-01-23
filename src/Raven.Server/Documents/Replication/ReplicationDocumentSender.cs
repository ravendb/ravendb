using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Raven.Client.Replication.Messages;
using Raven.Server.Extensions;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Raven.Abstractions.Data;
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

        [Flags]
        private enum CurrentEnumerationState
        {
            None = 0,
            HasDocs = 1,
            HasTombs = 2,
            HasConflicts = 4,
            HasDocsAndTombsAndConflicts = HasDocs | HasTombs | HasConflicts,
            HasDocsAndTombs = HasDocs | HasTombs,
            HasConflictsAndTombs = HasConflicts | HasTombs,
            HasDocsAndConflicts = HasConflicts | HasDocs
        }

        private IEnumerable<ReplicationBatchDocumentItem> GetDocsConflictsAndTombstonesAfter(DocumentsOperationContext ctx, long etag)
        {
            var docs = _parent._database.DocumentsStorage.GetDocumentsFrom(ctx, etag + 1);
            var tombs = _parent._database.DocumentsStorage.GetTombstonesFrom(ctx, etag + 1, 0, int.MaxValue);
            var conflicts = _parent._database.DocumentsStorage.GetConflictsFrom(ctx, etag + 1);

            using (var docsIt = docs.GetEnumerator())
            using (var tombsIt = tombs.GetEnumerator())
            using (var conflictsIt = conflicts.GetEnumerator())
            {
                var state = CurrentEnumerationState.None;

                if (docsIt.MoveNext())
                    state |= CurrentEnumerationState.HasDocs;
                if (tombsIt.MoveNext())
                    state |= CurrentEnumerationState.HasTombs;
                if (conflictsIt.MoveNext())
                    state |= CurrentEnumerationState.HasConflicts;
                while (true)
                {
                    switch (state)
                    {
                        case CurrentEnumerationState.None:
                            yield break;
                        case CurrentEnumerationState.HasDocs:
                            var docsItCurrent = docsIt.Current;
                            yield return new ReplicationBatchDocumentItem
                            {
                                Etag = docsItCurrent.Etag,
                                ChangeVector = docsItCurrent.ChangeVector,
                                Data = docsItCurrent.Data,
                                Key = docsItCurrent.Key,
                                TransactionMarker = docsItCurrent.TransactionMarker
                            };
                            if (docsIt.MoveNext() == false)
                                state &= ~CurrentEnumerationState.HasDocs;
                            break;
                        case CurrentEnumerationState.HasTombs:
                            var tombsItCurrent = tombsIt.Current;
                            yield return new ReplicationBatchDocumentItem
                            {
                                Etag = tombsItCurrent.Etag,
                                ChangeVector = tombsItCurrent.ChangeVector,
                                Collection = tombsItCurrent.Collection,
                                Key = tombsItCurrent.Key,
                                TransactionMarker = tombsItCurrent.TransactionMarker
                            };
                            if (tombsIt.MoveNext() == false)
                                state &= ~CurrentEnumerationState.HasTombs;
                            break;
                        case CurrentEnumerationState.HasConflicts:
                            var conflictCurrent = conflictsIt.Current;
                            yield return new ReplicationBatchDocumentItem
                            {
                                Etag = conflictCurrent.Etag,
                                ChangeVector = conflictCurrent.ChangeVector,
                                Collection = conflictCurrent.Collection,
                                Data = conflictCurrent.Doc,
                                Key = conflictCurrent.Key,
                                TransactionMarker = -1// not relevant for conflicts since they are already resolved in separate tx
                            };
                            if (conflictsIt.MoveNext() == false)
                                state &= ~CurrentEnumerationState.HasConflicts;
                            break;

                        case CurrentEnumerationState.HasDocsAndTombs:
                            if (docsIt.Current.Etag > tombsIt.Current.Etag)
                                goto case CurrentEnumerationState.HasTombs;
                            goto case CurrentEnumerationState.HasDocs;

                        case CurrentEnumerationState.HasConflictsAndTombs:
                            if (conflictsIt.Current.Etag > tombsIt.Current.Etag)
                                goto case CurrentEnumerationState.HasTombs;
                            goto case CurrentEnumerationState.HasConflicts;

                        case CurrentEnumerationState.HasDocsAndConflicts:
                            if (conflictsIt.Current.Etag > docsIt.Current.Etag)
                                goto case CurrentEnumerationState.HasDocs;
                            goto case CurrentEnumerationState.HasConflicts;

                        case CurrentEnumerationState.HasDocsAndTombsAndConflicts:
                            if (docsIt.Current.Etag > tombsIt.Current.Etag)
                                goto case CurrentEnumerationState.HasDocsAndConflicts;
                            goto case CurrentEnumerationState.HasConflictsAndTombs;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }
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
            _orderedReplicaItems.Add(item.Etag, item);
        }


        private void SendDocumentsBatch(DocumentsOperationContext documentsContext)
        {
            if (_log.IsInfoEnabled)
                _log.Info(
                    $"Starting sending replication batch ({_parent._database.Name}) with {_orderedReplicaItems.Count:#,#;;0} docs, and last etag {_lastEtag}");

            var sw = Stopwatch.StartNew();
            var headerJson = new DynamicJsonValue
            {
                [nameof(ReplicationMessageHeader.Type)] = ReplicationMessageType.Documents,
                [nameof(ReplicationMessageHeader.LastDocumentEtag)] = _lastEtag,
                [nameof(ReplicationMessageHeader.LastIndexOrTransformerEtag)] = _parent._lastSentIndexOrTransformerEtag,
                [nameof(ReplicationMessageHeader.ItemCount)] = _orderedReplicaItems.Count,
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
            if ((str.Buffer[6] == (byte)'H' || str.Buffer[6] == (byte)'h') &&
                (str.Buffer[7] == (byte)'I' || str.Buffer[7] == (byte)'i') &&
                (str.Buffer[8] == (byte)'L' || str.Buffer[8] == (byte)'l') &&
                (str.Buffer[9] == (byte)'O' || str.Buffer[9] == (byte)'o') &&
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