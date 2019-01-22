using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions;
using Sparrow;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationDocumentSender
    {
        private readonly Logger _log;
        private long _lastEtag;

        private readonly SortedList<long, ReplicationBatchItem> _orderedReplicaItems = new SortedList<long, ReplicationBatchItem>();
        private readonly Dictionary<Slice, ReplicationBatchItem> _replicaAttachmentStreams = new Dictionary<Slice, ReplicationBatchItem>();
        private readonly List<ReplicationBatchItem> _countersToReplicate = new List<ReplicationBatchItem>();
        private readonly byte[] _tempBuffer = new byte[32 * 1024];
        private readonly Stream _stream;
        private readonly OutgoingReplicationHandler _parent;
        private OutgoingReplicationStatsScope _statsInstance;
        private readonly ReplicationStats _stats = new ReplicationStats();
        public bool MissingAttachmentsInLastBatch { get; private set; }

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

            private readonly OutgoingReplicationStatsScope _countersRead;


            public MergedReplicationBatchEnumerator(OutgoingReplicationStatsScope documentRead, OutgoingReplicationStatsScope attachmentRead, OutgoingReplicationStatsScope tombstoneRead, OutgoingReplicationStatsScope counterRead)
            {
                _documentRead = documentRead;
                _attachmentRead = attachmentRead;
                _tombstoneRead = tombstoneRead;
                _countersRead = counterRead;
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
                    case ReplicationBatchItem.ReplicationItemType.Counter:
                        return _countersRead;
                    case ReplicationBatchItem.ReplicationItemType.DocumentTombstone:
                    case ReplicationBatchItem.ReplicationItemType.AttachmentTombstone:
                    case ReplicationBatchItem.ReplicationItemType.RevisionTombstone:
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

        private IEnumerable<ReplicationBatchItem> GetReplicationItems(DocumentsOperationContext ctx, long etag, ReplicationStats stats)
        {
            var docs = _parent._database.DocumentsStorage.GetDocumentsFrom(ctx, etag + 1);
            var tombs = _parent._database.DocumentsStorage.GetTombstonesFrom(ctx, etag + 1);
            var conflicts = _parent._database.DocumentsStorage.ConflictsStorage.GetConflictsFrom(ctx, etag + 1).Select(ReplicationBatchItem.From);
            var revisionsStorage = _parent._database.DocumentsStorage.RevisionsStorage;
            var revisions = revisionsStorage.GetRevisionsFrom(ctx, etag + 1, int.MaxValue).Select(ReplicationBatchItem.From);
            var attachments = _parent._database.DocumentsStorage.AttachmentsStorage.GetAttachmentsFrom(ctx, etag + 1);
            var counters = _parent._database.DocumentsStorage.CountersStorage.GetCountersFrom(ctx, etag + 1);


            using (var docsIt = docs.GetEnumerator())
            using (var tombsIt = tombs.GetEnumerator())
            using (var conflictsIt = conflicts.GetEnumerator())
            using (var versionsIt = revisions.GetEnumerator())
            using (var attachmentsIt = attachments.GetEnumerator())
            using (var countersIt = counters.GetEnumerator())
            using (var mergedInEnumerator = new MergedReplicationBatchEnumerator(stats.DocumentRead, stats.AttachmentRead, stats.TombstoneRead, stats.CounterRead))
            {
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Document, docsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.DocumentTombstone, tombsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Document, conflictsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Document, versionsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Attachment, attachmentsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Counter, countersIt);

                while (mergedInEnumerator.MoveNext())
                {
                    yield return mergedInEnumerator.Current;
                }
            }
        }

        public bool ExecuteReplicationOnce(OutgoingReplicationStatsScope stats, ref long next)
        {
            EnsureValidStats(stats);
            var wasInterrupted = false;
            var delay = GetDelayReplication();
            var currentNext = next;
            using (_parent._database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
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
                    var numberOfItemsSent = 0;
                    var skippedReplicationItemsInfo = new SkippedReplicationItemsInfo();
                    short lastTransactionMarker = -1;
                    long prevLastEtag = _lastEtag;

                    using (_stats.Storage.Start())
                    {                        
                        foreach (var item in GetReplicationItems(documentsContext, _lastEtag, _stats))
                        {
                            if (lastTransactionMarker != item.TransactionMarker)
                            {
                                if (delay.Ticks > 0)
                                {
                                    var nextReplication = item.LastModifiedTicks + delay.Ticks;
                                    if (_parent._database.Time.GetUtcNow().Ticks < nextReplication)
                                    {
                                        if (Interlocked.CompareExchange(ref next, nextReplication, currentNext) == currentNext)
                                        {
                                            wasInterrupted = true;
                                            break;
                                        }
                                    }
                                }
                                lastTransactionMarker = item.TransactionMarker;

                                if (_parent.SupportedFeatures.Replication.Counters == false)
                                {                                    
                                    AssertNotCounterForLegacyReplication(item);
                                }

                                if (_parent.SupportedFeatures.Replication.ClusterTransaction == false )
                                {
                                    AssertNotClusterTransactionDocumentForLegacyReplication(item);
                                }

                                // Include the attachment's document which is right after its latest attachment.
                                if ((item.Type == ReplicationBatchItem.ReplicationItemType.Document ||
                                     item.Type == ReplicationBatchItem.ReplicationItemType.DocumentTombstone) &&
                                    // We want to limit batch sizes to reasonable limits.
                                    ((maxSizeToSend.HasValue && size > maxSizeToSend.Value.GetValue(SizeUnit.Bytes)) ||
                                     (batchSize.HasValue && numberOfItemsSent > batchSize.Value)))
                                {
                                    wasInterrupted = true;
                                    break;
                                }

                                if (_stats.Storage.CurrentStats.InputCount % 16384 == 0)
                                {
                                    // ReSharper disable once PossibleLossOfFraction
                                    if ((_parent._parent.MinimalHeartbeatInterval / 2) < _stats.Storage.Duration.TotalMilliseconds)
                                    {
                                        wasInterrupted = true;
                                        break;
                                    }
                                }
                            }

                            _stats.Storage.RecordInputAttempt();

                            //Here we add missing attachments in the same batch as the document that contains them without modifying the last etag or transaction boundary
                            if (MissingAttachmentsInLastBatch && 
                                item.Type == ReplicationBatchItem.ReplicationItemType.Document &&
                                (item.Flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments)
                            {
                                var type = (item.Flags & DocumentFlags.Revision) == DocumentFlags.Revision ? AttachmentType.Revision: AttachmentType.Document;
                                foreach (var attachment in _parent._database.DocumentsStorage.AttachmentsStorage.GetAttachmentsForDocument(documentsContext, type, item.Id))
                                {
                                    //We need to filter attachments that are been sent in the same batch as the document
                                    if (attachment.Etag >= prevLastEtag)
                                        continue;
                                    var stream = _parent._database.DocumentsStorage.AttachmentsStorage.GetAttachmentStream(documentsContext, attachment.Base64Hash);
                                    attachment.Stream = stream;
                                    AddReplicationItemToBatch(ReplicationBatchItem.From(attachment), _stats.Storage, skippedReplicationItemsInfo);
                                    size += attachment.Stream.Length;
                                }
                                
                            }

                            _lastEtag = item.Etag;

                            if (item.Data != null)
                                size += item.Data.Size;
                            else if (item.Type == ReplicationBatchItem.ReplicationItemType.Attachment)
                                size += item.Stream.Length;

                            if (AddReplicationItemToBatch(item, _stats.Storage, skippedReplicationItemsInfo) == false)
                                continue;

                            numberOfItemsSent++;
                        }
                    }
                    
                    if (_log.IsInfoEnabled)
                    {
                        if (skippedReplicationItemsInfo.SkippedItems > 0)
                        {
                            var message = skippedReplicationItemsInfo.GetInfoForDebug(_parent.LastAcceptedChangeVector);
                            _log.Info(message);
                        }
                        
                        _log.Info($"Found {_orderedReplicaItems.Count:#,#;;0} documents and {_replicaAttachmentStreams.Count} attachment's streams to replicate to {_parent.Node.FromString()}.");
                    }

                    if (_orderedReplicaItems.Count == 0 && _countersToReplicate.Count == 0)
                    {
                        var hasModification = _lastEtag != _parent._lastSentDocumentEtag;

                        // ensure that the other server is aware that we skipped 
                        // on (potentially a lot of) documents to send, and we update
                        // the last etag they have from us on the other side
                        _parent._lastSentDocumentEtag = _lastEtag;
                        _parent._lastDocumentSentTime = DateTime.UtcNow;
                        var changeVector = wasInterrupted ? null : DocumentsStorage.GetDatabaseChangeVector(documentsContext);
                        _parent.SendHeartbeat(changeVector);
                        return hasModification;
                    }

                    _parent.CancellationToken.ThrowIfCancellationRequested();

                    try
                    {
                        using (_stats.Network.Start())
                        {
                            SendDocumentsBatch(documentsContext, _stats.Network);
                            if (MissingAttachmentsInLastBatch)
                                return false;
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Received cancellation notification while sending document replication batch.");
                        throw;
                    }
                    catch (Exception e)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("Failed to send document replication batch", e);
                        throw;
                    }

                    MissingAttachmentsInLastBatch = false;
                    

                    return true;
                }
                finally
                {
                    _orderedReplicaItems.Clear();
                    _replicaAttachmentStreams.Clear();
                    _countersToReplicate.Clear();
                }
            }
        }

        private void AssertNotCounterForLegacyReplication(ReplicationBatchItem item)
        {
            if (item.Type == ReplicationBatchItem.ReplicationItemType.Counter)
            {
                // the other side doesn't support counters, stopping replication
                var message = $"{_parent.Node.FromString()} found an item of type `Counter` to replicate to {_parent.Destination.FromString()}, " +
                              "while we are in legacy mode (downgraded our replication version to match the destination). " +
                              $"Can't send Counters in legacy mode, destination {_parent.Destination.FromString()} does not support Counters feature. " +
                              "Stopping replication. " + item;

                if (_log.IsInfoEnabled)
                    _log.Info(message);

                throw new LegacyReplicationViolationException(message);
            }
        }

        private void AssertNotClusterTransactionDocumentForLegacyReplication(ReplicationBatchItem item)
        {
            if (item.Type == ReplicationBatchItem.ReplicationItemType.Document &&
                item.Flags.HasFlag(DocumentFlags.FromClusterTransaction))
            {                
                // the other side doesn't support cluster transactions, stopping replication
                var message = $"{_parent.Node.FromString()} found a document {item.Id} with flag `FromClusterTransaction` to replicate to {_parent.Destination.FromString()}, " +
                              "while we are in legacy mode (downgraded our replication version to match the destination). " +
                              $"Can't use Cluster Transactions legacy mode, destination {_parent.Destination.FromString()} does not support this feature. " +
                              "Stopping replication.";

                if (_log.IsInfoEnabled)
                    _log.Info(message);

                throw new LegacyReplicationViolationException(message);
            }
        }

        private TimeSpan GetDelayReplication()
        {
            TimeSpan delayReplicationFor = TimeSpan.Zero;

            if (_parent.Destination is ExternalReplication external)
            {
                delayReplicationFor = external.DelayReplicationFor;

                if (delayReplicationFor.Ticks > 0)
                    _parent._parent._server.LicenseManager.AssertCanDelayReplication();
            }
            return delayReplicationFor;
        }

        private class SkippedReplicationItemsInfo
        {
            public long SkippedItems { get; private set; }

            private long _skippedArtificialDocuments;
            private long _startEtag;
            private long _endEtag;
            private string _startChangeVector;
            private string _endChangeVector;

            public void Update(ReplicationBatchItem item, bool isArtificial = false)
            {
                SkippedItems++;
                if (isArtificial)
                    _skippedArtificialDocuments++;

                if (_startChangeVector == null)
                {
                    _startChangeVector = item.ChangeVector;
                    _startEtag = item.Etag;
                }

                _endChangeVector = item.ChangeVector;
                _endEtag = item.Etag;
            }

            public string GetInfoForDebug(string destinationChangeVector)
            {
                var message = $"Skipped {SkippedItems:#,#;;0} items";
                if (_skippedArtificialDocuments > 0)
                    message += $" ({_skippedArtificialDocuments:#,#;;0} artificial documents)";

                message += $", start etag: {_startEtag:#,#;;0}, end etag: {_endEtag:#,#;;0}, " +
                           $"start change vector: {_startChangeVector}, end change vector: {_endChangeVector}, " +
                           $"destination change vector: {destinationChangeVector}";

                return message;
            }

            public void Reset()
            {
                SkippedItems = 0;
                _skippedArtificialDocuments = 0;
                _startEtag = 0;
                _endEtag = 0;
                _startChangeVector = null;
                _endChangeVector = null;
            }
        }
        private bool AddReplicationItemToBatch(ReplicationBatchItem item, OutgoingReplicationStatsScope stats, SkippedReplicationItemsInfo skippedReplicationItemsInfo)
        {
            if (item.Type == ReplicationBatchItem.ReplicationItemType.Document ||
                item.Type == ReplicationBatchItem.ReplicationItemType.DocumentTombstone)
            {
                if ((item.Flags & DocumentFlags.Artificial) == DocumentFlags.Artificial)
                {
                    stats.RecordArtificialDocumentSkip();
                    skippedReplicationItemsInfo.Update(item, isArtificial: true);
                    return false;
                }
            }

            if (item.Flags.Contain(DocumentFlags.Revision) || item.Flags.Contain(DocumentFlags.DeleteRevision))
            {
                // we let pass all the conflicted/resolved revisions, since we keep them with their original change vector which might be `AlreadyMerged` at the destination.
                if (item.Flags.Contain(DocumentFlags.Conflicted) || 
                    item.Flags.Contain(DocumentFlags.Resolved))
                {
                    _orderedReplicaItems.Add(item.Etag, item);
                    return true;
                }
            }

            // destination already has it
            if ( (MissingAttachmentsInLastBatch == false || item.Type != ReplicationBatchItem.ReplicationItemType.Attachment) && 
                ChangeVectorUtils.GetConflictStatus(item.ChangeVector, _parent.LastAcceptedChangeVector) == ConflictStatus.AlreadyMerged)
            {
                stats.RecordChangeVectorSkip();
                skippedReplicationItemsInfo.Update(item);
                return false;
            }

            if (skippedReplicationItemsInfo.SkippedItems > 0)
            {
                if (_log.IsInfoEnabled)
                {
                    var message = skippedReplicationItemsInfo.GetInfoForDebug(_parent.LastAcceptedChangeVector);
                    _log.Info(message);
                }

                skippedReplicationItemsInfo.Reset();
            }

            if (item.Type == ReplicationBatchItem.ReplicationItemType.Attachment)
                _replicaAttachmentStreams[item.Base64Hash] = item;
            if (item.Type == ReplicationBatchItem.ReplicationItemType.Counter)
            {
                _countersToReplicate.Add(item);
                return true;
            }

            Debug.Assert(item.Flags.Contain(DocumentFlags.Artificial) == false);
            _orderedReplicaItems.Add(item.Etag, item);
            return true;
        }

        private void SendDocumentsBatch(DocumentsOperationContext documentsContext, OutgoingReplicationStatsScope stats)
        {
            if (_log.IsInfoEnabled)
                _log.Info($"Starting sending replication batch ({_parent._database.Name}) with {_orderedReplicaItems.Count:#,#;;0} docs, and last etag {_lastEtag:#,#;;0}");

            var sw = Stopwatch.StartNew();
            var headerJson = new DynamicJsonValue
            {
                [nameof(ReplicationMessageHeader.Type)] = ReplicationMessageType.Documents,
                [nameof(ReplicationMessageHeader.LastDocumentEtag)] = _lastEtag,
                [nameof(ReplicationMessageHeader.ItemsCount)] = _orderedReplicaItems.Count + _countersToReplicate.Count,
                [nameof(ReplicationMessageHeader.AttachmentStreamsCount)] = _replicaAttachmentStreams.Count
            };

            stats.RecordLastEtag(_lastEtag);

            _parent.WriteToServer(headerJson);

            foreach (var item in _countersToReplicate)
            {
                WriteCountersToServer(documentsContext, item);

                stats.RecordCountersOutput(item.Values.Count -1); //?
            }

            foreach (var item in _orderedReplicaItems)
            {
                var value = item.Value;
                WriteItemToServer(documentsContext, value, stats);
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

            if (_log.IsInfoEnabled && _orderedReplicaItems.Count > 0)
                _log.Info($"Finished sending replication batch. Sent {_orderedReplicaItems.Count:#,#;;0} documents and {_replicaAttachmentStreams.Count:#,#;;0} attachment streams in {sw.ElapsedMilliseconds:#,#;;0} ms. Last sent etag = {_lastEtag:#,#;;0}");

            var (type, _) = _parent.HandleServerResponse();
            if (type == ReplicationMessageReply.ReplyType.MissingAttachments)
            {
                MissingAttachmentsInLastBatch = true;
                return;
            }
            _parent._lastSentDocumentEtag = _lastEtag;

            _parent._lastDocumentSentTime = DateTime.UtcNow;

        }

        private void WriteItemToServer(DocumentsOperationContext context, ReplicationBatchItem item, OutgoingReplicationStatsScope stats)
        {
            if (item.Type == ReplicationBatchItem.ReplicationItemType.Attachment)
            {
                WriteAttachmentToServer(context, item);
                return;
            }

            if (item.Type == ReplicationBatchItem.ReplicationItemType.AttachmentTombstone)
            {
                WriteAttachmentTombstoneToServer(context, item);
                stats.RecordAttachmentTombstoneOutput();
                return;
            }

            if (item.Type == ReplicationBatchItem.ReplicationItemType.RevisionTombstone)
            {
                WriteRevisionTombstoneToServer(context, item);
                stats.RecordRevisionTombstoneOutput();
                return;
            }

            if (item.Type == ReplicationBatchItem.ReplicationItemType.DocumentTombstone)
            {
                WriteDocumentToServer(context, item);
                stats.RecordDocumentTombstoneOutput();
                return;
            }

            if (item.Type == ReplicationBatchItem.ReplicationItemType.Counter)
            {
                WriteCounterToServer(context, item);
                stats.RecordCounterOutput();
                return;
            }

            WriteDocumentToServer(context, item);
            stats.RecordDocumentOutput(item.Data?.Size ?? 0);
        }

        private unsafe void WriteDocumentToServer(DocumentsOperationContext context, ReplicationBatchItem item)
        {
            using(Slice.From(context.Allocator, item.ChangeVector, out var cv))
            fixed (byte* pTemp = _tempBuffer)
            {
                var requiredSize = sizeof(byte) + // type
                                   sizeof(int) + //  size of change vector
                                   cv.Size +
                                   sizeof(short) + // transaction marker
                                   sizeof(long) + // Last modified ticks
                                   sizeof(DocumentFlags) +
                                   sizeof(int) + // size of document ID
                                   item.Id.Size +
                                   sizeof(int); // size of document
                
                if (item.Collection != null)
                {
                    requiredSize += item.Collection.Size + sizeof(int);
                }

                if (requiredSize > _tempBuffer.Length)
                    ThrowTooManyChangeVectorEntries(item);
                int tempBufferPos = 0;
                pTemp[tempBufferPos++] = (byte)item.Type;

                *(int*)(pTemp + tempBufferPos) = cv.Size;
                tempBufferPos += sizeof(int);

                Memory.Copy(pTemp + tempBufferPos, cv.Content.Ptr, cv.Size);
                tempBufferPos += cv.Size;

                *(short*)(pTemp + tempBufferPos) = item.TransactionMarker;
                tempBufferPos += sizeof(short);

                *(long*)(pTemp + tempBufferPos) = item.LastModifiedTicks;
                tempBufferPos += sizeof(long);

                *(DocumentFlags*)(pTemp + tempBufferPos) = item.Flags;
                tempBufferPos += sizeof(DocumentFlags);

                *(int*)(pTemp + tempBufferPos) = item.Id.Size;
                tempBufferPos += sizeof(int);

                Memory.Copy(pTemp + tempBufferPos, item.Id.Buffer, item.Id.Size);
                tempBufferPos += item.Id.Size;

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
                    int dataSize;
                    if (item.Type == ReplicationBatchItem.ReplicationItemType.DocumentTombstone)
                        dataSize = -1;
                    else if ((item.Flags & DocumentFlags.DeleteRevision) == DocumentFlags.DeleteRevision)
                        dataSize = -2;
                    else
                        throw new InvalidDataException("Cannot write document with empty data.");
                    *(int*)(pTemp + tempBufferPos) = dataSize;
                    tempBufferPos += sizeof(int);

                    if (item.Collection == null) //precaution
                        throw new InvalidDataException("Cannot write item with empty collection name...");

                    *(int*)(pTemp + tempBufferPos) = item.Collection.Size;
                    tempBufferPos += sizeof(int);
                    Memory.Copy(pTemp + tempBufferPos, item.Collection.Buffer, item.Collection.Size);
                    tempBufferPos += item.Collection.Size;
                }
                
                _stream.Write(_tempBuffer, 0, tempBufferPos);
            }
        }

        private unsafe void WriteAttachmentToServer(DocumentsOperationContext context, ReplicationBatchItem item)
        {
            using(Slice.From(context.Allocator, item.ChangeVector, out var cv))
            fixed (byte* pTemp = _tempBuffer)
            {
                var requiredSize = sizeof(byte) + // type
                              sizeof(int) + // # of change vectors
                              cv.Size +
                              sizeof(short) + // transaction marker
                              sizeof(int) + // size of ID
                              item.Id.Size +
                              sizeof(int) + // size of name
                              item.Name.Size +
                              sizeof(int) + // size of ContentType
                              item.ContentType.Size +
                              sizeof(byte) + // size of Base64Hash
                              item.Base64Hash.Size;

                if (requiredSize > _tempBuffer.Length)
                    ThrowTooManyChangeVectorEntries(item);
                var tempBufferPos = 0;
                pTemp[tempBufferPos++] = (byte)item.Type;

                *(int*)(pTemp + tempBufferPos) = cv.Size;
                tempBufferPos += sizeof(int);

                Memory.Copy(pTemp + tempBufferPos, cv.Content.Ptr, cv.Size);
                tempBufferPos += cv.Size;

                *(short*)(pTemp + tempBufferPos) = item.TransactionMarker;
                tempBufferPos += sizeof(short);

                *(int*)(pTemp + tempBufferPos) = item.Id.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, item.Id.Buffer, item.Id.Size);
                tempBufferPos += item.Id.Size;

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

        private unsafe void WriteAttachmentTombstoneToServer(DocumentsOperationContext context, ReplicationBatchItem item)
        {
            using(Slice.From(context.Allocator, item.ChangeVector, out var cv))
            fixed (byte* pTemp = _tempBuffer)
            {
                var requiredSize = sizeof(byte) + // type
                                   sizeof(int) + // # of change vectors
                                   cv.Size +
                                   sizeof(short) + // transaction marker
                                   sizeof(long) + // last modified
                                   sizeof(int) + // size of key
                                   item.Id.Size;

                if (requiredSize > _tempBuffer.Length)
                    ThrowTooManyChangeVectorEntries(item);

                var tempBufferPos = 0;
                pTemp[tempBufferPos++] = (byte)item.Type;

                *(int*)(pTemp + tempBufferPos) = cv.Size;
                tempBufferPos += sizeof(int);

                Memory.Copy(pTemp + tempBufferPos, cv.Content.Ptr, cv.Size);
                tempBufferPos += cv.Size;

                *(short*)(pTemp + tempBufferPos) = item.TransactionMarker;
                tempBufferPos += sizeof(short);
                
                *(long*)(pTemp + tempBufferPos) = item.LastModifiedTicks;
                tempBufferPos += sizeof(long);

                *(int*)(pTemp + tempBufferPos) = item.Id.Size;
                tempBufferPos += sizeof(int);

                Memory.Copy(pTemp + tempBufferPos, item.Id.Buffer, item.Id.Size);
                tempBufferPos += item.Id.Size;

                _stream.Write(_tempBuffer, 0, tempBufferPos);
            }
        }

        private unsafe void WriteRevisionTombstoneToServer(DocumentsOperationContext context,ReplicationBatchItem item)
        {
            using(Slice.From(context.Allocator, item.ChangeVector, out var cv))
            fixed (byte* pTemp = _tempBuffer)
            {
                var requiredSize = sizeof(byte) + // type
                                   sizeof(int) + // # of change vectors
                                   cv.Size +
                                   sizeof(short) + // transaction marker
                                   sizeof(long) + // last modified
                                   sizeof(int) + // size of key
                                   item.Id.Size +
                                   sizeof(int) + // size of collection
                                   item.Collection.Size;

                if (requiredSize > _tempBuffer.Length)
                    ThrowTooManyChangeVectorEntries(item);

                var tempBufferPos = 0;
                pTemp[tempBufferPos++] = (byte)item.Type;

                *(int*)(pTemp + tempBufferPos) = cv.Size;
                tempBufferPos += sizeof(int);

                Memory.Copy(pTemp + tempBufferPos, cv.Content.Ptr, cv.Size);
                tempBufferPos += cv.Size;

                *(short*)(pTemp + tempBufferPos) = item.TransactionMarker;
                tempBufferPos += sizeof(short);

                *(long*)(pTemp + tempBufferPos) = item.LastModifiedTicks;
                tempBufferPos += sizeof(long);

                *(int*)(pTemp + tempBufferPos) = item.Id.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, item.Id.Buffer, item.Id.Size);
                tempBufferPos += item.Id.Size;

                *(int*)(pTemp + tempBufferPos) = item.Collection.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, item.Collection.Buffer, item.Collection.Size);
                tempBufferPos += item.Collection.Size;

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

        private unsafe void WriteCounterToServer(DocumentsOperationContext context, ReplicationBatchItem item)
        {
            using (Slice.From(context.Allocator, item.ChangeVector, out var cv))
            fixed (byte* pTemp = _tempBuffer)
            {
                    var requiredSize = sizeof(byte) + // type
                                       sizeof(int) + // change vector size
                                       cv.Size + // change vector
                                       sizeof(short) + // transaction marker
                                       sizeof(int) + // size of doc id
                                       item.Id.Size +
                                       sizeof(int) + // size of doc collection
                                       item.Collection.Size + // doc collection
                                       sizeof(int) + // size of name
                                       item.Name.Size +
                                       sizeof(long); // value

                if (requiredSize > _tempBuffer.Length)
                    ThrowTooManyChangeVectorEntries(item);

                var tempBufferPos = 0;
                pTemp[tempBufferPos++] = (byte)item.Type;

                *(int*)(pTemp + tempBufferPos) = cv.Size;
                tempBufferPos += sizeof(int);

                Memory.Copy(pTemp + tempBufferPos, cv.Content.Ptr, cv.Size);
                tempBufferPos += cv.Size;

                *(short*)(pTemp + tempBufferPos) = item.TransactionMarker;
                tempBufferPos += sizeof(short);

                *(int*)(pTemp + tempBufferPos) = item.Id.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, item.Id.Buffer, item.Id.Size);
                tempBufferPos += item.Id.Size;

                *(int*)(pTemp + tempBufferPos) = item.Collection.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, item.Collection.Buffer, item.Collection.Size);
                tempBufferPos += item.Collection.Size;

                *(int*)(pTemp + tempBufferPos) = item.Name.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, item.Name.Buffer, item.Name.Size);
                tempBufferPos += item.Name.Size;

                *(long*)(pTemp + tempBufferPos) = item.Value;
                tempBufferPos += sizeof(long);

                _stream.Write(_tempBuffer, 0, tempBufferPos);
            }
        }

        private unsafe void WriteCountersToServer(DocumentsOperationContext context, ReplicationBatchItem item)
        {
            using (Slice.From(context.Allocator, item.ChangeVector, out var cv))
                fixed (byte* pTemp = _tempBuffer)
                {
                    var requiredSize = sizeof(byte) + // type
                                       sizeof(int) + // change vector size
                                       cv.Size + // change vector
                                       sizeof(short) + // transaction marker
                                       sizeof(int) + // size of doc id
                                       item.Id.Size +
                                       sizeof(int) + // size of doc collection
                                       item.Collection.Size + // doc collection
                                       sizeof(int) // size of data
                                       + item.Values.Size; // data

                    if (requiredSize > _tempBuffer.Length)
                        ThrowTooManyChangeVectorEntries(item);

                    var tempBufferPos = 0;
                    pTemp[tempBufferPos++] = (byte)item.Type;

                    *(int*)(pTemp + tempBufferPos) = cv.Size;
                    tempBufferPos += sizeof(int);

                    Memory.Copy(pTemp + tempBufferPos, cv.Content.Ptr, cv.Size);
                    tempBufferPos += cv.Size;

                    *(short*)(pTemp + tempBufferPos) = item.TransactionMarker;
                    tempBufferPos += sizeof(short);

                    *(int*)(pTemp + tempBufferPos) = item.Id.Size;
                    tempBufferPos += sizeof(int);
                    Memory.Copy(pTemp + tempBufferPos, item.Id.Buffer, item.Id.Size);
                    tempBufferPos += item.Id.Size;

                    *(int*)(pTemp + tempBufferPos) = item.Collection.Size;
                    tempBufferPos += sizeof(int);
                    Memory.Copy(pTemp + tempBufferPos, item.Collection.Buffer, item.Collection.Size);
                    tempBufferPos += item.Collection.Size;

                    *(int*)(pTemp + tempBufferPos) = item.Values.Size;
                    tempBufferPos += sizeof(int);

                    Memory.Copy(pTemp + tempBufferPos, item.Values.BasePointer, item.Values.Size);
                    tempBufferPos += item.Values.Size;

                    _stream.Write(_tempBuffer, 0, tempBufferPos);
                }
        }


        private unsafe void WriteCounterTombstoneToServer(DocumentsOperationContext context, ReplicationBatchItem item)
        {
            using (Slice.From(context.Allocator, item.ChangeVector, out var cv))
            fixed (byte* pTemp = _tempBuffer)
            {
                var requiredSize = sizeof(byte) + // type
                                   sizeof(int) + // change vector size
                                   cv.Size + // change vector size
                                   sizeof(short) + // transaction marker
                                   sizeof(int) + // size of tombstone key
                                   item.Id.Size +
                                   sizeof(int) + // size of tombstone collection
                                   item.Collection.Size + // tombstone collection
                                   sizeof(long); // last modified ticks

                if (requiredSize > _tempBuffer.Length)
                    ThrowTooManyChangeVectorEntries(item);

                var tempBufferPos = 0;
                pTemp[tempBufferPos++] = (byte)item.Type;

                *(int*)(pTemp + tempBufferPos) = cv.Size;
                tempBufferPos += sizeof(int);

                Memory.Copy(pTemp + tempBufferPos, cv.Content.Ptr, cv.Size);
                tempBufferPos += cv.Size;

                *(short*)(pTemp + tempBufferPos) = item.TransactionMarker;
                tempBufferPos += sizeof(short);

                *(int*)(pTemp + tempBufferPos) = item.Id.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, item.Id.Buffer, item.Id.Size);
                tempBufferPos += item.Id.Size;

                *(int*)(pTemp + tempBufferPos) = item.Collection.Size;
                tempBufferPos += sizeof(int);
                Memory.Copy(pTemp + tempBufferPos, item.Collection.Buffer, item.Collection.Size);
                tempBufferPos += item.Collection.Size;

                *(long*)(pTemp + tempBufferPos) = item.LastModifiedTicks;
                tempBufferPos += sizeof(long);

                _stream.Write(_tempBuffer, 0, tempBufferPos);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ThrowTooManyChangeVectorEntries(ReplicationBatchItem item)
        {
            throw new ArgumentOutOfRangeException(nameof(item),
                $"{item.Type} '{item.Id}' has too many change vector entries to replicate: {item.ChangeVector.Length}");
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
            _stats.CounterRead = _stats.Storage.For(ReplicationOperation.Outgoing.CounterRead, start: false);

        }

        private class ReplicationStats
        {
            public OutgoingReplicationStatsScope Network;
            public OutgoingReplicationStatsScope Storage;
            public OutgoingReplicationStatsScope DocumentRead;
            public OutgoingReplicationStatsScope TombstoneRead;
            public OutgoingReplicationStatsScope AttachmentRead;
            public OutgoingReplicationStatsScope CounterRead;

        }
    }
}
