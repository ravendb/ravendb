using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Extensions;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationDocumentSender
    {
        private readonly Logger _log;
        private long _lastEtag;

        private readonly SortedList<long, ReplicationBatchItem> _orderedReplicaItems = new SortedList<long, ReplicationBatchItem>();
        private readonly Dictionary<Slice, ReplicationBatchItem> _replicaAttachmentStreams = new Dictionary<Slice, ReplicationBatchItem>();
        private readonly byte[] _tempBuffer = new byte[32 * 1024];
        private readonly Stream _stream;
        private readonly OutgoingReplicationHandler _parent;
        private OutgoingReplicationStatsScope _statsInstance;
        private readonly ReplicationStats _stats = new ReplicationStats();

        public ReplicationDocumentSender(Stream stream, OutgoingReplicationHandler parent, Logger log)
        {
            _log = log;
            _stream = stream;
            _parent = parent;
        }

        public class MergedReplicationBatchEnumerator : IEnumerator<ReplicationBatchItem>
        {
            private readonly List<IEnumerator<ReplicationBatchItem>> _workEnumerators = new List<IEnumerator<ReplicationBatchItem>>();
            private ReplicationBatchItem _currentItem;
            private readonly OutgoingReplicationStatsScope _documentRead;
            private readonly OutgoingReplicationStatsScope _attachmentRead;
            private readonly OutgoingReplicationStatsScope _tombstoneRead;

            public MergedReplicationBatchEnumerator(OutgoingReplicationStatsScope documentRead, OutgoingReplicationStatsScope attachmentRead, OutgoingReplicationStatsScope tombstoneRead)
            {
                _documentRead = documentRead;
                _attachmentRead = attachmentRead;
                _tombstoneRead = tombstoneRead;
            }

            public void AddEnumerator(ReplicationBatchItem.ReplicationItemType type, IEnumerator<ReplicationBatchItem> enumerator)
            {
                if (enumerator == null)
                    return;

                if (enumerator.MoveNext())
                {
                    using (GetStatsFor(type).Start())
                    {
                        _workEnumerators.Add(enumerator);
                    }
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private OutgoingReplicationStatsScope GetStatsFor(ReplicationBatchItem.ReplicationItemType type)
            {
                switch (type)
                {
                    case ReplicationBatchItem.ReplicationItemType.Document:
                        return _documentRead;
                    case ReplicationBatchItem.ReplicationItemType.Attachment:
                        return _attachmentRead;
                    case ReplicationBatchItem.ReplicationItemType.DocumentTombstone:
                    case ReplicationBatchItem.ReplicationItemType.AttachmentTombstone:
                        return _tombstoneRead;
                    default:
                        throw new ArgumentOutOfRangeException();
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
                using (GetStatsFor(_currentItem.Type).Start())
                {
                    if (current.MoveNext() == false)
                    {
                        _workEnumerators.Remove(current);
                    }

                    return true;
                }
            }

            public void Reset()
            {
                throw new NotSupportedException();
            }

            public ReplicationBatchItem Current => _currentItem;

            object IEnumerator.Current => Current;

            public void Dispose()
            {
                foreach (var workEnumerator in _workEnumerators)
                {
                    workEnumerator.Dispose();
                }
                _workEnumerators.Clear();
            }
        }

        private IEnumerable<ReplicationBatchItem> GetDocsConflictsTombstonesRevisionsAndAttachmentsAfter(DocumentsOperationContext ctx, long etag, ReplicationStats stats)
        {
            var docs = _parent._database.DocumentsStorage.GetDocumentsFrom(ctx, etag + 1);
            var tombs = _parent._database.DocumentsStorage.GetTombstonesFrom(ctx, etag + 1);
            var conflicts = _parent._database.DocumentsStorage.ConflictsStorage.GetConflictsFrom(ctx, etag + 1);
            var revisions = _parent._database.BundleLoader?.VersioningStorage?.GetRevisionsAfter(ctx, etag + 1);
            var attachments = _parent._database.DocumentsStorage.AttachmentsStorage.GetAttachmentsFrom(ctx, etag + 1);

            using (var docsIt = docs.GetEnumerator())
            using (var tombsIt = tombs.GetEnumerator())
            using (var conflictsIt = conflicts.GetEnumerator())
            using (var versionsIt = revisions?.GetEnumerator())
            using (var attachmentsIt = attachments.GetEnumerator())
            using (var mergedInEnumerator = new MergedReplicationBatchEnumerator(stats.DocumentRead, stats.AttachmentRead, stats.TombstoneRead))
            {
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Document, docsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.DocumentTombstone, tombsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Document, conflictsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Document, versionsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Attachment, attachmentsIt);

                while (mergedInEnumerator.MoveNext())
                {
                    yield return mergedInEnumerator.Current;
                }
            }
        }

        public bool ExecuteReplicationOnce(OutgoingReplicationStatsScope stats)
        {
            EnsureValidStats(stats);

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

                    var batchSize = _parent._database.Configuration.Replication.MaxItemsCount;
                    var maxSizeToSend = _parent._database.Configuration.Replication.MaxSizeToSend;
                    long size = 0;
                    int numberOfItemsSent = 0;
                    short lastTransactionMarker = -1;
                    using (_stats.Storage.Start())
                    {
                        foreach (var item in GetDocsConflictsTombstonesRevisionsAndAttachmentsAfter(documentsContext, _lastEtag, _stats))
                        {
                            if (lastTransactionMarker != item.TransactionMarker)
                            {
                                lastTransactionMarker = item.TransactionMarker;

                                // Include the attachment's document which is right after its latest attachment.
                                if ((item.Type == ReplicationBatchItem.ReplicationItemType.Document ||
                                     item.Type == ReplicationBatchItem.ReplicationItemType.DocumentTombstone) &&
                                    // We want to limit batch sizes to reasonable limits.
                                    ((maxSizeToSend.HasValue && size > maxSizeToSend.Value) ||
                                     (batchSize.HasValue && numberOfItemsSent > batchSize.Value)))
                                    break;
                            }

                            _stats.Storage.RecordInputAttempt();

                            _lastEtag = item.Etag;

                            if (item.Data != null)
                                size += item.Data.Size;
                            else if (item.Type == ReplicationBatchItem.ReplicationItemType.Attachment)
                                size += item.Stream.Length;

                            if (AddReplicationItemToBatch(item, _stats.Storage))
                                numberOfItemsSent++;
                        }
                    }

                    if (_log.IsInfoEnabled)
                        _log.Info($"Found {_orderedReplicaItems.Count:#,#;;0} documents and {_replicaAttachmentStreams.Count} attachment's streams to replicate to {_parent.Node.NodeTag} @ {_parent.Node.Url}.");

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
                        using (_stats.Network.Start())
                        {
                            SendDocumentsBatch(documentsContext, _stats.Network);
                        }
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
                    {
                        var value = item.Value;
                        if (value.Type == ReplicationBatchItem.ReplicationItemType.Attachment)
                        {
                            // TODO: Why are we disposing here?
                            // Shouldn't the all context be disposed here?
                            // If not, should we dispose all strings here?
                            value.Stream.Dispose();
                        }
                        else
                        {
                            value.Data?.Dispose(); //item.Value.Data is null if tombstone
                        }
                    }
                    _orderedReplicaItems.Clear();
                    _replicaAttachmentStreams.Clear();
                }
            }
        }

        private unsafe bool AddReplicationItemToBatch(ReplicationBatchItem item, OutgoingReplicationStatsScope stats)
        {
            if (item.Type == ReplicationBatchItem.ReplicationItemType.Attachment)
            {
                _replicaAttachmentStreams[item.Base64Hash] = item;
            }
            else if (item.Type == ReplicationBatchItem.ReplicationItemType.Document ||
                     item.Type == ReplicationBatchItem.ReplicationItemType.DocumentTombstone)
            {
                if ((item.Flags & DocumentFlags.Artificial) == DocumentFlags.Artificial)
                {
                    stats.RecordArtificialDocumentSkip();

                    if (_log.IsInfoEnabled)
                        _log.Info($"Skipping replication of {item.Key} because it is an artificial document");
                    return false;
                }

                bool isHiLo;
                if (CollectionName.IsSystemDocument(item.Key.Buffer, item.Key.Size, out isHiLo) && isHiLo == false)
                {
                    stats.RecordSystemDocumentSkip();

                    if (_log.IsInfoEnabled)
                        _log.Info($"Skipping replication of {item.Key} because it is a system document");
                    return false;
                }

                // destination already has it
                if (item.ChangeVector.GreaterThan(_parent._destinationLastKnownDocumentChangeVector) == false)
                {
                    stats.RecordDocumentChangeVectorSkip();

                    if (_log.IsInfoEnabled)
                        _log.Info($"Skipping replication of {item.Key} because destination has a higher change vector. Doc: {item.ChangeVector.Format()} < Dest: {_parent._destinationLastKnownDocumentChangeVectorAsString} ");
                    return false;
                }
            }

            Debug.Assert(item.Flags.HasFlag(DocumentFlags.Artificial) == false);
            _orderedReplicaItems.Add(item.Etag, item);
            return true;
        }

        private void SendDocumentsBatch(DocumentsOperationContext documentsContext, OutgoingReplicationStatsScope stats)
        {
            if (_log.IsInfoEnabled)
                _log.Info($"Starting sending replication batch ({_parent._database.Name}) with {_orderedReplicaItems.Count:#,#;;0} docs, and last etag {_lastEtag}");

            var sw = Stopwatch.StartNew();
            var headerJson = new DynamicJsonValue
            {
                [nameof(ReplicationMessageHeader.Type)] = ReplicationMessageType.Documents,
                [nameof(ReplicationMessageHeader.LastDocumentEtag)] = _lastEtag,
                [nameof(ReplicationMessageHeader.ItemsCount)] = _orderedReplicaItems.Count,
                [nameof(ReplicationMessageHeader.AttachmentStreamsCount)] = _replicaAttachmentStreams.Count
            };

            stats.RecordLastEtag(_lastEtag);

            _parent.WriteToServer(headerJson);
            foreach (var item in _orderedReplicaItems)
            {
                var value = item.Value;
                WriteItemToServer(value, stats);
            }

            foreach (var item in _replicaAttachmentStreams)
            {
                var value = item.Value;
                WriteAttachmentStreamToServer(value);

                stats.RecordAttachmentOutput(value.Stream.Length);
            }

            // close the transaction as early as possible, and before we wait for reply
            // from other side
            documentsContext.Transaction.Dispose();
            _stream.Flush();
            sw.Stop();

            _parent._lastSentDocumentEtag = _lastEtag;

            if (_log.IsInfoEnabled && _orderedReplicaItems.Count > 0)
                _log.Info($"Finished sending replication batch. Sent {_orderedReplicaItems.Count:#,#;;0} documents and {_replicaAttachmentStreams.Count:#,#;;0} attachment streams in {sw.ElapsedMilliseconds:#,#;;0} ms. Last sent etag = {_lastEtag}");

            _parent._lastDocumentSentTime = DateTime.UtcNow;
            _parent.HandleServerResponse();

        }

        private void WriteItemToServer(ReplicationBatchItem item, OutgoingReplicationStatsScope stats)
        {
            if (item.Type == ReplicationBatchItem.ReplicationItemType.Attachment)
            {
                WriteAttachmentToServer(item);
                return;
            }

            if (item.Type == ReplicationBatchItem.ReplicationItemType.AttachmentTombstone)
            {
                WriteAttachmentTombstoneToServer(item);
                stats.RecordAttachmentTombstoneOutput();
                return;
            }

            if (item.Type == ReplicationBatchItem.ReplicationItemType.DocumentTombstone)
            {
                WriteDocumentToServer(item);
                stats.RecordDocumentTombstoneOutput();
                return;
            }

            WriteDocumentToServer(item);
            stats.RecordDocumentOutput(item.Data?.Size ?? 0);
        }

        private unsafe void WriteDocumentToServer(ReplicationBatchItem item)
        {
            var changeVectorSize = item.ChangeVector.Length * sizeof(ChangeVectorEntry);
            var requiredSize = sizeof(byte) + // type
                               sizeof(int) + // # of change vectors
                               changeVectorSize +
                               sizeof(short) + // transaction marker
                               sizeof(long) + // Last modified ticks
                               sizeof(DocumentFlags) +
                               sizeof(int) + // size of document key
                               item.Key.Size +
                               sizeof(int); // size of document

            if (requiredSize > _tempBuffer.Length)
                ThrowTooManyChangeVectorEntries(item.Key, item.ChangeVector);

            fixed (byte* pTemp = _tempBuffer)
            {
                int tempBufferPos = 0;
                pTemp[tempBufferPos++] = (byte)item.Type;

                fixed (ChangeVectorEntry* pChangeVectorEntries = item.ChangeVector)
                {
                    *(int*)(pTemp + tempBufferPos) = item.ChangeVector.Length;
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
                    while (docReadPos < item.Data.Size)
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

        private unsafe void WriteAttachmentToServer(ReplicationBatchItem item)
        {
            var requiredSize = sizeof(byte) + // type
                               sizeof(short) + // transaction marker
                               sizeof(int) + // size of key
                               item.Key.Size +
                               sizeof(int) + // size of name
                               item.Name.Size +
                               sizeof(int) + // size of ContentType
                               item.ContentType.Size +
                               sizeof(byte) + // size of Base64Hash
                               item.Base64Hash.Size;

            if (requiredSize > _tempBuffer.Length)
                throw new ArgumentOutOfRangeException(nameof(item),
                    $"Attachment name {item.Name} or content type {item.ContentType} or the key ({item.Key.Size} - {item.Key}) " +
                    $"(which might include the change vector for revisions or conflicts) is too big.");

            fixed (byte* pTemp = _tempBuffer)
            {
                int tempBufferPos = 0;
                pTemp[tempBufferPos++] = (byte)item.Type;

                *(short*)(pTemp + tempBufferPos) = item.TransactionMarker;
                tempBufferPos += sizeof(short);

                *(int*)(pTemp + tempBufferPos) = item.Key.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, item.Key.Buffer, item.Key.Size);
                tempBufferPos += item.Key.Size;

                *(int*)(pTemp + tempBufferPos) = item.Name.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, item.Name.Buffer, item.Name.Size);
                tempBufferPos += item.Name.Size;

                *(int*)(pTemp + tempBufferPos) = item.ContentType.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, item.ContentType.Buffer, item.ContentType.Size);
                tempBufferPos += item.ContentType.Size;

                pTemp[tempBufferPos++] = (byte)item.Base64Hash.Size;
                item.Base64Hash.CopyTo(pTemp + tempBufferPos);
                tempBufferPos += item.Base64Hash.Size;

                _stream.Write(_tempBuffer, 0, tempBufferPos);
            }
        }

        private unsafe void WriteAttachmentTombstoneToServer(ReplicationBatchItem item)
        {
            var requiredSize = sizeof(byte) + // type
                               sizeof(short) + // transaction marker
                               sizeof(int) + // size of key
                               item.Key.Size;

            if (requiredSize > _tempBuffer.Length)
                throw new ArgumentOutOfRangeException(nameof(item), $"Attachment key ({item.Key.Size} - {item.Key}) " +
                                                              "(which might include the change vector for revisions or conflicts) is too big.");

            fixed (byte* pTemp = _tempBuffer)
            {
                int tempBufferPos = 0;
                pTemp[tempBufferPos++] = (byte)item.Type;

                *(short*)(pTemp + tempBufferPos) = item.TransactionMarker;
                tempBufferPos += sizeof(short);

                *(int*)(pTemp + tempBufferPos) = item.Key.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, item.Key.Buffer, item.Key.Size);
                tempBufferPos += item.Key.Size;

                _stream.Write(_tempBuffer, 0, tempBufferPos);
            }
        }

        private unsafe void WriteAttachmentStreamToServer(ReplicationBatchItem item)
        {
            fixed (byte* pTemp = _tempBuffer)
            {
                int tempBufferPos = 0;
                pTemp[tempBufferPos++] = (byte)ReplicationBatchItem.ReplicationItemType.AttachmentStream;

                // Hash size is 32, but it might be changed in the future
                pTemp[tempBufferPos++] = (byte)item.Base64Hash.Size;
                item.Base64Hash.CopyTo(pTemp + tempBufferPos);
                tempBufferPos += item.Base64Hash.Size;

                *(long*)(pTemp + tempBufferPos) = item.Stream.Length;
                tempBufferPos += sizeof(long);

                long readPos = 0;
                while (readPos < item.Stream.Length)
                {
                    var sizeToCopy = (int)Math.Min(item.Stream.Length - readPos, _tempBuffer.Length - tempBufferPos);
                    if (sizeToCopy == 0) // buffer is full, need to flush it
                    {
                        _stream.Write(_tempBuffer, 0, tempBufferPos);
                        tempBufferPos = 0;
                        continue;
                    }
                    var readCount = item.Stream.Read(_tempBuffer, tempBufferPos, sizeToCopy);
                    tempBufferPos += readCount;
                    readPos += readCount;
                }

                _stream.Write(_tempBuffer, 0, tempBufferPos);
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ThrowTooManyChangeVectorEntries(LazyStringValue key, ChangeVectorEntry[] changeVector)
        {
            throw new ArgumentOutOfRangeException("doc",
                "Document " + key + " has too many change vector entries to replicate: " +
                changeVector.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureValidStats(OutgoingReplicationStatsScope stats)
        {
            if (_statsInstance == stats)
                return;

            _statsInstance = stats;
            _stats.Storage = stats.For(ReplicationOperation.Outgoing.Storage, start: false);
            _stats.Network = stats.For(ReplicationOperation.Outgoing.Network, start: false);

            _stats.DocumentRead = _stats.Storage.For(ReplicationOperation.Outgoing.DocumentRead, start: false);
            _stats.TombstoneRead = _stats.Storage.For(ReplicationOperation.Outgoing.TombstoneRead, start: false);
            _stats.AttachmentRead = _stats.Storage.For(ReplicationOperation.Outgoing.AttachmentRead, start: false);
        }

        private class ReplicationStats
        {
            public OutgoingReplicationStatsScope Network;
            public OutgoingReplicationStatsScope Storage;
            public OutgoingReplicationStatsScope DocumentRead;
            public OutgoingReplicationStatsScope TombstoneRead;
            public OutgoingReplicationStatsScope AttachmentRead;
        }
    }
}