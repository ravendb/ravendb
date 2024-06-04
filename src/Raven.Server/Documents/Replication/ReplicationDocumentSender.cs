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
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.Enumerators;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationDocumentSender : IDisposable
    {
        private readonly Logger _log;
        private long _lastEtag;

        private readonly SortedList<long, ReplicationBatchItem> _orderedReplicaItems = new SortedList<long, ReplicationBatchItem>();
        private readonly Dictionary<Slice, AttachmentReplicationItem> _replicaAttachmentStreams = new Dictionary<Slice, AttachmentReplicationItem>(SliceComparer.Instance);
        private readonly byte[] _tempBuffer = new byte[32 * 1024];
        private readonly Stream _stream;
        internal readonly OutgoingReplicationHandler _parent;
        private OutgoingReplicationStatsScope _statsInstance;
        internal readonly ReplicationStats _stats = new ReplicationStats();
        public bool MissingAttachmentsInLastBatch { get; private set; }
        private HashSet<Slice> _deduplicatedAttachmentHashes = new(SliceComparer.Instance);
        private Queue<Slice> _deduplicatedAttachmentHashesLru = new();
        private int _numberOfAttachmentsTrackedForDeduplication;
        private ByteStringContext _context; // required to clone the hashes 


        public ReplicationDocumentSender(Stream stream, OutgoingReplicationHandler parent, Logger log, string[] pathsToSend, string[] destinationAcceptablePaths)
        {
            _log = log;
            if (pathsToSend != null && pathsToSend.Length > 0)
                _pathsToSend = new AllowedPathsValidator(pathsToSend);
            if (destinationAcceptablePaths != null && destinationAcceptablePaths.Length > 0)
                _destinationAcceptablePaths = new AllowedPathsValidator(destinationAcceptablePaths);
            _stream = stream;
            _parent = parent;

            _shouldSkipSendingTombstones = _parent.Destination is PullReplicationAsSink sink && sink.Mode == PullReplicationMode.SinkToHub &&
                                           parent._outgoingPullReplicationParams?.PreventDeletionsMode?.HasFlag(PreventDeletionsMode.PreventSinkToHubDeletions) == true &&
                                           _parent._database.ForTestingPurposes?.ForceSendTombstones != true;

            _numberOfAttachmentsTrackedForDeduplication = parent._database.Configuration.Replication.MaxNumberOfAttachmentsTrackedForDeduplication;
            _context = new ByteStringContext(SharedMultipleUseFlag.None);

        }

        public class MergedReplicationBatchEnumerator : IEnumerator<ReplicationBatchItem>
        {
            private readonly List<IEnumerator<ReplicationBatchItem>> _workEnumerators = new List<IEnumerator<ReplicationBatchItem>>();
            private ReplicationBatchItem _currentItem;
            private readonly OutgoingReplicationStatsScope _documentRead;
            private readonly OutgoingReplicationStatsScope _attachmentRead;
            private readonly OutgoingReplicationStatsScope _tombstoneRead;
            private readonly OutgoingReplicationStatsScope _countersRead;
            private readonly OutgoingReplicationStatsScope _timeSeriesRead;

            public MergedReplicationBatchEnumerator(
                OutgoingReplicationStatsScope documentRead,
                OutgoingReplicationStatsScope attachmentRead,
                OutgoingReplicationStatsScope tombstoneRead,
                OutgoingReplicationStatsScope counterRead,
                OutgoingReplicationStatsScope timeSeriesRead
                )
            {
                _documentRead = documentRead;
                _attachmentRead = attachmentRead;
                _tombstoneRead = tombstoneRead;
                _countersRead = counterRead;
                _timeSeriesRead = timeSeriesRead;
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

                    case ReplicationBatchItem.ReplicationItemType.CounterGroup:
                        return _countersRead;

                    case ReplicationBatchItem.ReplicationItemType.DocumentTombstone:
                    case ReplicationBatchItem.ReplicationItemType.AttachmentTombstone:
                    case ReplicationBatchItem.ReplicationItemType.RevisionTombstone:
                        return _tombstoneRead;

                    case ReplicationBatchItem.ReplicationItemType.TimeSeriesSegment:
                    case ReplicationBatchItem.ReplicationItemType.DeletedTimeSeriesRange:
                        return _timeSeriesRead;

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

        internal static IEnumerable<ReplicationBatchItem> GetReplicationItems(DocumentDatabase database, DocumentsOperationContext ctx, long etag, ReplicationStats stats, bool caseInsensitiveCounters)
        {
            var docs = database.DocumentsStorage.GetDocumentsFrom(ctx, etag + 1, fields: DocumentFields.Id | DocumentFields.ChangeVector | DocumentFields.Data);
            var tombs = database.DocumentsStorage.GetTombstonesFrom(ctx, etag + 1);
            var conflicts = database.DocumentsStorage.ConflictsStorage.GetConflictsFrom(ctx, etag + 1).Select(DocumentReplicationItem.From);
            var revisionsStorage = database.DocumentsStorage.RevisionsStorage;
            var revisions = revisionsStorage.GetRevisionsFrom(ctx, etag + 1, long.MaxValue, fields: DocumentFields.Id | DocumentFields.ChangeVector | DocumentFields.Data).Select(x => DocumentReplicationItem.From(x, ctx));
            var attachments = database.DocumentsStorage.AttachmentsStorage.GetAttachmentsFrom(ctx, etag + 1);
            var counters = database.DocumentsStorage.CountersStorage.GetCountersFrom(ctx, etag + 1, caseInsensitiveCounters);
            var timeSeries = database.DocumentsStorage.TimeSeriesStorage.GetSegmentsFrom(ctx, etag + 1);
            var deletedTimeSeriesRanges = database.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesFrom(ctx, etag + 1);

            using (var docsIt = docs.GetEnumerator())
            using (var tombsIt = tombs.GetEnumerator())
            using (var conflictsIt = conflicts.GetEnumerator())
            using (var versionsIt = revisions.GetEnumerator())
            using (var attachmentsIt = attachments.GetEnumerator())
            using (var countersIt = counters.GetEnumerator())
            using (var timeSeriesIt = timeSeries.GetEnumerator())
            using (var deletedTimeSeriesRangesIt = deletedTimeSeriesRanges.GetEnumerator())
            using (var mergedInEnumerator = new MergedReplicationBatchEnumerator(stats.DocumentRead, stats.AttachmentRead, stats.TombstoneRead, stats.CounterRead, stats.TimeSeriesRead))
            {
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Document, docsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.DocumentTombstone, tombsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Document, conflictsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Document, versionsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.Attachment, attachmentsIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.CounterGroup, countersIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.TimeSeriesSegment, timeSeriesIt);
                mergedInEnumerator.AddEnumerator(ReplicationBatchItem.ReplicationItemType.DeletedTimeSeriesRange, deletedTimeSeriesRangesIt);

                while (mergedInEnumerator.MoveNext())
                {
                    yield return mergedInEnumerator.Current;
                }
            }
        }

        private class ReplicationBatchState : PulsedEnumerationState<ReplicationBatchItem>
        {
            private readonly ReplicationDocumentSender _parent;
            public bool WasInterrupted { get; private set; }

            private long _next;
            public long Next
            {
                get => _next;
                private set
                {
                    _next = value;
                }
            }

            public HashSet<Slice> MissingAttachmentBase64Hashes;
            public TimeSpan Delay;
            private ReplicationBatchItem _item;
            public long CurrentNext;
            public long Size;
            public int NumberOfItemsSent;
            public short LastTransactionMarker;
            public int? BatchSize;
            public Size? MaxSizeToSend;

            public ReplicationBatchState(DocumentsOperationContext context, Size pulseLimit, ReplicationDocumentSender parent, long next)
                : base(context, pulseLimit, parent._parent._parent._server.Configuration.Replication.NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded)
            {
                _parent = parent;
                WasInterrupted = false;
                Next = next;
            }

            public override void OnMoveNext(ReplicationBatchItem current)
            {
                ReadCount++;
                _parent._parent.ForTestingPurposes?.OnDocumentSenderFetchNewItem?.Invoke();
                _parent._parent.CancellationToken.ThrowIfCancellationRequested();
                _parent.AssertNoLegacyReplicationViolation(current);

                if (LastTransactionMarker != current.TransactionMarker)
                {
                    _item = current;
                    if (CanContinueBatch() == false)
                    {
                        WasInterrupted = true;
                        return;
                    }
                    LastTransactionMarker = current.TransactionMarker;
                }
            }

            private bool CanContinueBatch()
            {
                if (_parent.MissingAttachmentsInLastBatch)
                {
                    // we do have missing attachments but we haven't gathered yet any of the missing hashes
                    if (MissingAttachmentBase64Hashes == null)
                        return true;

                    // we do have missing attachments but we haven't included all of them in the batch yet
                    if (MissingAttachmentBase64Hashes.Count > 0)
                        return true;
                }

                if (Delay.Ticks > 0)
                {
                    var nextReplication = _item.LastModifiedTicks + Delay.Ticks;
                    if (_parent._parent._database.Time.GetUtcNow().Ticks < nextReplication)
                    {
                        if (Interlocked.CompareExchange(ref _next, nextReplication, CurrentNext) == CurrentNext)
                        {
                            return false;
                        }
                    }
                }

                if (NumberOfItemsSent == 0)
                {
                    // always send at least one item
                    return true;
                }

                // We want to limit batch sizes to reasonable limits.
                var totalSize =
                    Size + Context.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize.GetValue(SizeUnit.Bytes);

                if (MaxSizeToSend.HasValue && totalSize >= MaxSizeToSend.Value.GetValue(SizeUnit.Bytes) ||
                    BatchSize.HasValue && NumberOfItemsSent >= BatchSize.Value)
                {
                    return false;
                }

                var currentCount = _parent._stats.Storage.CurrentStats.InputCount;
                if (currentCount > 0 && currentCount % 16384 == 0)
                {
                    // ReSharper disable once PossibleLossOfFraction
                    if ((_parent._parent._parent.MinimalHeartbeatInterval / 2) < _parent._stats.Storage.Duration.TotalMilliseconds)
                    {
                        return false;
                    }
                }

                return true;
            }

            public override bool ShouldPulseTransaction()
            {
                if (NumberOfItemsSent == 0) // when we start to collect items to send (NumberOfItemsSent>0), we don't want to pulse transaction to prevent those items from being deleted before we send them in the batch.
                {
                    return base.ShouldPulseTransaction();
                }

                return false;
            }
        }

        public bool ExecuteReplicationOnce(TcpConnectionOptions tcpConnectionOptions, OutgoingReplicationStatsScope stats, ref long next)
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

                    var lastEtagFromDestinationChangeVector = ChangeVectorUtils.GetEtagById(_parent.LastAcceptedChangeVector, _parent._database.DbBase64Id);
                    if (lastEtagFromDestinationChangeVector > _lastEtag)
                    {
                        if (_log.IsInfoEnabled)
                        {
                            _log.Info($"We jump to get items from etag {lastEtagFromDestinationChangeVector} instead of {_lastEtag}, because we got a bigger etag for the destination database change vector ({_parent.LastAcceptedChangeVector})");
                        }
                        _lastEtag = lastEtagFromDestinationChangeVector;
                    }

                    _parent.CancellationToken.ThrowIfCancellationRequested();

                    var skippedReplicationItemsInfo = new SkippedReplicationItemsInfo();
                    long prevLastEtag = _lastEtag;
                    var state = new ReplicationBatchState(documentsContext, _parent._database.Configuration.Databases.PulseReadTransactionLimit, this, next)
                    {
                        BatchSize = _parent._database.Configuration.Replication.MaxItemsCount,
                        MaxSizeToSend = _parent._database.Configuration.Replication.MaxSizeToSend,
                        CurrentNext = currentNext,
                        Delay = delay,
                        LastTransactionMarker = -1,
                        NumberOfItemsSent = 0,
                        Size = 0L
                    };

                    using (_stats.Storage.Start())
                    {
                        using var enumerator = new PulsedTransactionEnumerator<ReplicationBatchItem, ReplicationBatchState>(documentsContext, _ => GetReplicationItems(_parent._database, documentsContext, this._lastEtag, _stats,
                            _parent.SupportedFeatures.Replication.CaseInsensitiveCounters), state);

                        while (enumerator.MoveNext())
                        {
                            next = state.Next;
                            if (state.WasInterrupted)
                            {
                                wasInterrupted = true;
                                break;
                            }

                            var item = enumerator.Current;

                            _stats.Storage.RecordInputAttempt();

                            // here we add missing attachments in the same batch as the document that contains them without modifying the last etag or transaction boundary
                            if (MissingAttachmentsInLastBatch &&
                                item.Type == ReplicationBatchItem.ReplicationItemType.Document &&
                                item is DocumentReplicationItem docItem &&
                                docItem.Flags.Contain(DocumentFlags.HasAttachments))
                            {
                                var missingAttachmentBase64Hashes = state.MissingAttachmentBase64Hashes ??= new HashSet<Slice>(SliceStructComparer.Instance);
                                var type = (docItem.Flags & DocumentFlags.Revision) == DocumentFlags.Revision ? AttachmentType.Revision : AttachmentType.Document;
                                foreach (var attachment in _parent._database.DocumentsStorage.AttachmentsStorage.GetAttachmentsForDocument(documentsContext, type, docItem.Id, docItem.ChangeVector))
                                {
                                    // we need to filter attachments that are been sent in the same batch as the document
                                    if (attachment.Etag >= prevLastEtag)
                                    {
                                        if (_replicaAttachmentStreams.ContainsKey(attachment.Base64Hash) == false)
                                            missingAttachmentBase64Hashes.Add(attachment.Base64Hash);

                                        continue;
                                    }

                                    var stream = _parent._database.DocumentsStorage.AttachmentsStorage.GetAttachmentStream(documentsContext, attachment.Base64Hash);
                                    attachment.Stream = stream;
                                    var attachmentItem = AttachmentReplicationItem.From(documentsContext, attachment);
                                    AddReplicationItemToBatch(attachmentItem, _stats.Storage, state, skippedReplicationItemsInfo);
                                    state.Size += attachmentItem.Size;
                                }
                            }

                            _lastEtag = item.Etag;

                            if (AddReplicationItemToBatch(item, _stats.Storage, state, skippedReplicationItemsInfo) == false)
                            {
                                // this item won't be needed anymore
                                item.Dispose();
                                continue;
                            }

                            state.Size += item.Size;

                            state.NumberOfItemsSent++;
                        }
                    }

                    if (_log.IsInfoEnabled)
                    {
                        if (skippedReplicationItemsInfo.SkippedItems > 0)
                        {
                            var message = skippedReplicationItemsInfo.GetInfoForDebug(_parent.LastAcceptedChangeVector);
                            _log.Info(message);
                        }

                        var msg = $"Found {_orderedReplicaItems.Count:#,#;;0} documents " +
                                  $"and {_replicaAttachmentStreams.Count} attachment's streams " +
                                  $"to replicate to {_parent.Node.FromString()}, ";

                        var encryptionSize = documentsContext.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize.GetValue(SizeUnit.Bytes);
                        if (encryptionSize > 0)
                        {
                            msg += $"encryption buffer overhead size is {new Size(encryptionSize, SizeUnit.Bytes)}, ";
                        }
                        msg += $"total size: {new Size(state.Size + encryptionSize, SizeUnit.Bytes)}";

                        _log.Info(msg);
                    }

                    if (_orderedReplicaItems.Count == 0)
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

                    MissingAttachmentsInLastBatch = false;

                    try
                    {
                        using (_stats.Network.Start())
                        {
                            SendDocumentsBatch(documentsContext, _stats.Network);
                            tcpConnectionOptions._lastEtagSent = _lastEtag;
                            tcpConnectionOptions.RegisterBytesSent(state.Size);
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

                    return true;
                }
                catch
                {
                    foreach (var item in _orderedReplicaItems)
                    {
                        item.Value.Dispose();
                    }

                    foreach (var item in _replicaAttachmentStreams)
                    {
                        item.Value.Dispose();
                    }

                    throw;
                }
                finally
                {
                    _orderedReplicaItems.Clear();
                    _replicaAttachmentStreams.Clear();
                }
            }
        }

        private void AssertNoLegacyReplicationViolation(ReplicationBatchItem item)
        {
            if (_parent.SupportedFeatures.Replication.CountersBatch == false)
            {
                AssertNotCounterForLegacyReplication(item);
            }

            if (_parent.SupportedFeatures.Replication.ClusterTransaction == false)
            {
                AssertNotClusterTransactionDocumentForLegacyReplication(item);
            }

            if (_parent.SupportedFeatures.Replication.TimeSeries == false)
            {
                AssertNotTimeSeriesForLegacyReplication(item);
            }

            if (_parent.SupportedFeatures.Replication.IncrementalTimeSeries == false)
            {
                AssertNotIncrementalTimeSeriesForLegacyReplication(item);
            }
        }

        private void AssertNotTimeSeriesForLegacyReplication(ReplicationBatchItem item)
        {
            if (item.Type == ReplicationBatchItem.ReplicationItemType.TimeSeriesSegment || item.Type == ReplicationBatchItem.ReplicationItemType.DeletedTimeSeriesRange)
            {
                // the other side doesn't support TimeSeries, stopping replication
                var message = $"{_parent.Node.FromString()} found an item of type 'TimeSeries' to replicate to {_parent.Destination.FromString()}, " +
                              $"while we are in legacy mode (downgraded our replication version to match the destination). " +
                              $"Can't send TimeSeries in legacy mode, destination {_parent.Destination.FromString()} does not support TimeSeries feature. Stopping replication. {item}";

                if (_log.IsInfoEnabled)
                    _log.Info(message);

                throw new LegacyReplicationViolationException(message);
            }
        }

        private void AssertNotCounterForLegacyReplication(ReplicationBatchItem item)
        {
            if (item.Type == ReplicationBatchItem.ReplicationItemType.CounterGroup)
            {
                // the other side doesn't support counters, stopping replication
                var message =
                    $"{_parent.Node.FromString()} found an item of type `{nameof(ReplicationBatchItem.ReplicationItemType.CounterGroup)}` " +
                    $"to replicate to {_parent.Destination.FromString()}, " +
                    "while we are in legacy mode (downgraded our replication version to match the destination). " +
                    $"Can't send Counters in legacy mode, destination {_parent.Destination.FromString()} ";

                message += _parent.SupportedFeatures.Replication.Counters == false
                    ? "does not support Counters feature. "
                    : "uses the old structure of counters (legacy counters). ";

                message += "Stopping replication. " + item;

                if (_log.IsInfoEnabled)
                    _log.Info(message);

                throw new LegacyReplicationViolationException(message);
            }
        }

        private void AssertNotClusterTransactionDocumentForLegacyReplication(ReplicationBatchItem item)
        {
            if (item.Type == ReplicationBatchItem.ReplicationItemType.Document &&
                item is DocumentReplicationItem doc &&
                doc.Flags.HasFlag(DocumentFlags.FromClusterTransaction))
            {
                // the other side doesn't support cluster transactions, stopping replication
                var message = $"{_parent.Node.FromString()} found a document {doc.Id} with flag `FromClusterTransaction` to replicate to {_parent.Destination.FromString()}, " +
                              "while we are in legacy mode (downgraded our replication version to match the destination). " +
                              $"Can't use Cluster Transactions legacy mode, destination {_parent.Destination.FromString()} does not support this feature. " +
                              "Stopping replication.";

                if (_log.IsInfoEnabled)
                    _log.Info(message);

                throw new LegacyReplicationViolationException(message);
            }
        }

        private void AssertNotIncrementalTimeSeriesForLegacyReplication(ReplicationBatchItem item)
        {
            if (item.Type == ReplicationBatchItem.ReplicationItemType.TimeSeriesSegment || item.Type == ReplicationBatchItem.ReplicationItemType.DeletedTimeSeriesRange)
            {
                using (_parent._database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    LazyStringValue name;
                    switch (item)
                    {
                        case TimeSeriesDeletedRangeItem timeSeriesDeletedRangeItem:
                            TimeSeriesValuesSegment.ParseTimeSeriesKey(timeSeriesDeletedRangeItem.Key, context, out _, out name);
                            break;
                        case TimeSeriesReplicationItem timeSeriesReplicationItem:
                            name = timeSeriesReplicationItem.Name;
                            break;
                        default:
                            return;
                    }

                    if (TimeSeriesHandler.CheckIfIncrementalTs(name) == false)
                        return;
                }

                // the other side doesn't support incremental time series, stopping replication
                var message = $"{_parent.Node.FromString()} found an item of type 'IncrementalTimeSeries' to replicate to {_parent.Destination.FromString()}, " +
                              $"while we are in legacy mode (downgraded our replication version to match the destination). " +
                              $"Can't send Incremental-TimeSeries in legacy mode, destination {_parent.Destination.FromString()} does not support Incremental-TimeSeries feature. Stopping replication.";

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
                _parent._parent._server.LicenseManager.AssertCanDelayReplication(delayReplicationFor);
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

        private bool AddReplicationItemToBatch(ReplicationBatchItem item, OutgoingReplicationStatsScope stats, ReplicationBatchState state, SkippedReplicationItemsInfo skippedReplicationItemsInfo)
        {
            if (ShouldSkip(item, stats, skippedReplicationItemsInfo))
                return false;

            if (skippedReplicationItemsInfo.SkippedItems > 0)
            {
                if (_log.IsInfoEnabled)
                {
                    var message = skippedReplicationItemsInfo.GetInfoForDebug(_parent.LastAcceptedChangeVector);
                    _log.Info(message);
                }

                skippedReplicationItemsInfo.Reset();
            }

            if (item is AttachmentReplicationItem attachment)
            {
                if (ShouldSendAttachmentStream(attachment))
                    _replicaAttachmentStreams[attachment.Base64Hash] = attachment;

                if (MissingAttachmentsInLastBatch)
                    state.MissingAttachmentBase64Hashes?.Remove(attachment.Base64Hash);
            }

            _orderedReplicaItems.Add(item.Etag, item);
            return true;
        }

        private bool ShouldSendAttachmentStream(AttachmentReplicationItem attachment)
        {
            if (MissingAttachmentsInLastBatch)
            {
                // we intentionally not trying to de-duplicate in this scenario
                // we may have _sent_ the attachment already, but it was deleted at 
                // destination, so we need to send it again
                return true;
            }

            if (_parent.SupportedFeatures.Replication.DeduplicatedAttachments == false)
                return true;

            // RavenDB does de-duplication of attachments on storage, but not over the wire
            // Here we implement the same idea, if (in the current connection), we already sent
            // an attachment, we will skip sending it to the other side since we _know_ it is 
            // already there.
            if (_deduplicatedAttachmentHashes.Contains(attachment.Base64Hash))
                return false; // we already sent it over during the current run

            var clone = attachment.Base64Hash.Clone(_context);
            _deduplicatedAttachmentHashes.Add(clone);
            _deduplicatedAttachmentHashesLru.Enqueue(clone);
            while (_deduplicatedAttachmentHashesLru.Count > _numberOfAttachmentsTrackedForDeduplication)
            {
                var cur = _deduplicatedAttachmentHashesLru.Dequeue();
                _deduplicatedAttachmentHashes.Remove(cur);
                _context.Release(ref cur.Content);
            }

            return true;
        }

        private bool ShouldSkip(ReplicationBatchItem item, OutgoingReplicationStatsScope stats, SkippedReplicationItemsInfo skippedReplicationItemsInfo)
        {
            if (ValidatorSaysToSkip(_pathsToSend) || ValidatorSaysToSkip(_destinationAcceptablePaths))
                return true;

            if (_shouldSkipSendingTombstones && ReplicationLoader.IsOfTypePreventDeletions(item))
            {
                return true;
            }

            switch (item)
            {
                case DocumentReplicationItem doc:
                    if (doc.Flags.Contain(DocumentFlags.Artificial))
                    {
                        stats.RecordArtificialDocumentSkip();
                        skippedReplicationItemsInfo.Update(item, isArtificial: true);
                        return true;
                    }

                    if (doc.Flags.Contain(DocumentFlags.Resolved))
                    {
                        // we let all resolved documents to pass to the other side, since we might resolved them to a merged change vector, and we want to ensure
                        // that this is properly synced between the nodes
                        return false;
                    }

                    if (doc.Flags.Contain(DocumentFlags.Revision) || doc.Flags.Contain(DocumentFlags.DeleteRevision))
                    {
                        // we let pass all the conflicted revisions, since we keep them with their original change vector which might be `AlreadyMerged` at the destination.
                        if (doc.Flags.Contain(DocumentFlags.Conflicted) ||
                            doc.Flags.Contain(DocumentFlags.FromClusterTransaction) ||
                            doc.Flags.Contain(DocumentFlags.FromOldDocumentRevision) ||
                            doc.Flags.Contain(DocumentFlags.ForceCreated))
                        {
                            return false;
                        }
                    }

                    break;

                case AttachmentReplicationItem attachment:
                    if (MissingAttachmentsInLastBatch)
                        return false;

                    var type = AttachmentsStorage.GetAttachmentTypeByKey(attachment.Key);
                    if (type == AttachmentType.Revision)
                    {
                        return false;
                    }
                    break;
            }

            // destination already has it
            if (_parent._database.DocumentsStorage.GetConflictStatus(item.ChangeVector, _parent.LastAcceptedChangeVector) == ConflictStatus.AlreadyMerged)
            {
                stats.RecordChangeVectorSkip();
                skippedReplicationItemsInfo.Update(item);
                return true;
            }

            return false;

            bool ValidatorSaysToSkip(AllowedPathsValidator validator)
            {
                if (validator == null)
                    return false;

                if (validator.ShouldAllow(item))
                    return false;

                stats.RecordArtificialDocumentSkip();
                skippedReplicationItemsInfo.Update(item);

                if (_log.IsInfoEnabled)
                {
                    string key = validator.GetItemInformation(item);
                    _log.Info($"Will skip sending {key} ({item.Type}) because it was not allowed according to the incoming .");
                }

                return true;
            }
        }

        private readonly AllowedPathsValidator _pathsToSend, _destinationAcceptablePaths;
        private readonly bool _shouldSkipSendingTombstones;

        private void SendDocumentsBatch(DocumentsOperationContext documentsContext, OutgoingReplicationStatsScope stats)
        {
            if (_log.IsInfoEnabled)
                _log.Info($"Starting sending replication batch ({_parent._database.Name}) with {_orderedReplicaItems.Count:#,#;;0} docs, and last etag {_lastEtag:#,#;;0}");

            if (_parent.ForTestingPurposes?.OnMissingAttachmentStream != null &&
                _parent.MissingAttachmentsRetries > 0)
            {
                var replicaAttachmentStreams = _replicaAttachmentStreams;
                _parent.ForTestingPurposes?.OnMissingAttachmentStream?.Invoke(replicaAttachmentStreams);
            }

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
                using (item.Value)
                using (Slice.From(documentsContext.Allocator, item.Value.ChangeVector, out var cv))
                {
                    item.Value.Write(cv, _stream, _tempBuffer, stats);
                }
            }

            foreach (var item in _replicaAttachmentStreams)
            {
                using (item.Value)
                {
                    item.Value.WriteStream(_stream, _tempBuffer);
                    stats.RecordAttachmentOutput(item.Value.Stream.Length);
                }
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
            _stats.TimeSeriesRead = _stats.Storage.For(ReplicationOperation.Outgoing.TimeSeriesRead, start: false);
        }

        internal class ReplicationStats
        {
            public OutgoingReplicationStatsScope Network;
            public OutgoingReplicationStatsScope Storage;
            public OutgoingReplicationStatsScope DocumentRead;
            public OutgoingReplicationStatsScope TombstoneRead;
            public OutgoingReplicationStatsScope AttachmentRead;
            public OutgoingReplicationStatsScope CounterRead;
            public OutgoingReplicationStatsScope TimeSeriesRead;
        }

        public void Dispose()
        {
            _pathsToSend?.Dispose();
            _destinationAcceptablePaths?.Dispose();
            _context.Dispose();
        }
    }
}
