using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Exceptions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Replication.Incoming
{
    public class IncomingReplicationHandler : AbstractIncomingReplicationHandler<DocumentsContextPool, DocumentsOperationContext>
    {
        private readonly DocumentDatabase _database;
        private readonly ReplicationLoader _parent;

        public long LastHeartbeatTicks;

        public event Action<IncomingReplicationHandler, Exception> Failed;
        public event Action<IncomingReplicationHandler> DocumentsReceived;
        public event Action<IncomingReplicationHandler, int> AttachmentStreamsReceived;

        public IncomingReplicationHandler(
            TcpConnectionOptions options,
            ReplicationLatestEtagRequest replicatedLastEtag,
            ReplicationLoader parent,
            JsonOperationContext.MemoryBuffer bufferToCopy,
            ReplicationLatestEtagRequest.ReplicationType replicationType) : base(options, bufferToCopy, parent._server, parent.Database.Name, replicationType, replicatedLastEtag,
            options.DocumentDatabase.DatabaseShutdown, options.DocumentDatabase.DocumentsStorage.ContextPool)
        {
            _database = options.DocumentDatabase;
            _replicationFromAnotherSource = new AsyncManualResetEvent(_database.DatabaseShutdown);
            _parent = parent;
            _attachmentStreamsTempFile = _database.DocumentsStorage.AttachmentsStorage.GetTempFile("replication");

            LastHeartbeatTicks = _database.Time.GetUtcNow().Ticks;
        }

        [ThreadStatic]
        public static bool IsIncomingReplication;

        static IncomingReplicationHandler()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () => IsIncomingReplication = false;
        }

        protected override ByteStringContext GetContextAllocator(DocumentsOperationContext context) => context.Allocator;

        protected override RavenConfiguration GetConfiguration() => _database.Configuration;

        public override void ClearEvents()
        {
            Failed = null;
            DocumentsReceived = null;
            base.ClearEvents();
        }

        public void OnReplicationFromAnotherSource()
        {
            _replicationFromAnotherSource.Set();
        }

        protected override void EnsureNotDeleted(string nodeTag)
        {
            _parent.EnsureNotDeleted(_parent._server.NodeTag);
        }

        private Task _prevChangeVectorUpdate;

        protected override int GetNextReplicationStatsId() => _parent.GetNextReplicationStatsId();

        protected virtual TransactionOperationsMerger.MergedTransactionCommand GetUpdateChangeVectorCommand(string changeVector, long lastDocumentEtag, string sourceDatabaseId, AsyncManualResetEvent trigger)
        {
            return new MergedUpdateDatabaseChangeVectorCommand(changeVector, lastDocumentEtag, sourceDatabaseId, trigger);
        }

        protected virtual TransactionOperationsMerger.MergedTransactionCommand GetMergeDocumentsCommand(DataForReplicationCommand data, long lastDocumentEtag)
        {
            return new MergedDocumentReplicationCommand(data, lastDocumentEtag);
        }

        internal class MergedUpdateDatabaseChangeVectorCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly string _changeVector;
            private readonly long _lastDocumentEtag;
            private readonly string _sourceDatabaseId;
            private readonly AsyncManualResetEvent _trigger;

            public MergedUpdateDatabaseChangeVectorCommand(string changeVector, long lastDocumentEtag, string sourceDatabaseId, AsyncManualResetEvent trigger)
            {
                _changeVector = changeVector;
                _lastDocumentEtag = lastDocumentEtag;
                _sourceDatabaseId = sourceDatabaseId;
                _trigger = trigger;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var operationsCount = 0;
                var lastReplicatedEtag = DocumentsStorage.GetLastReplicatedEtagFrom(context, _sourceDatabaseId);
                if (_lastDocumentEtag > lastReplicatedEtag)
                {
                    DocumentsStorage.SetLastReplicatedEtagFrom(context, _sourceDatabaseId, _lastDocumentEtag);
                    operationsCount++;
                }

                var current = context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(context);
                var conflictStatus = ChangeVectorUtils.GetConflictStatus(_changeVector, current);
                if (conflictStatus != ConflictStatus.Update)
                    return operationsCount;

                if (TryUpdateChangeVector(context))
                    operationsCount++;

                return operationsCount;
            }

            protected virtual bool TryUpdateChangeVector(DocumentsOperationContext context)
            {
                var current = context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(context);
                var conflictStatus = ChangeVectorUtils.GetConflictStatus(_changeVector, current);
                if (conflictStatus != ConflictStatus.Update)
                    return false;

                context.LastDatabaseChangeVector = current.MergeWith(_changeVector, context);
                context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += _ =>
                {
                    try
                    {
                        _trigger.Set();
                    }
                    catch
                    {
                        //
                    }
                };

                return true;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new MergedUpdateDatabaseChangeVectorCommandDto
                {
                    ChangeVector = _changeVector,
                    LastDocumentEtag = _lastDocumentEtag,
                    SourceDatabaseId = _sourceDatabaseId,
                };
            }
        }

        internal class MergedUpdateDatabaseChangeVectorCommandDto : TransactionOperationsMerger.IReplayableCommandDto<MergedUpdateDatabaseChangeVectorCommand>
        {
            public string ChangeVector;
            public long LastDocumentEtag;
            public string SourceDatabaseId;

            public MergedUpdateDatabaseChangeVectorCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                var command = new MergedUpdateDatabaseChangeVectorCommand(ChangeVector, LastDocumentEtag, SourceDatabaseId,
                    new AsyncManualResetEvent());
                return command;
            }
        }

        protected override void HandleHeartbeatMessage(DocumentsOperationContext documentsContext, BlittableJsonReaderObject message)
        {
            if (message.TryGet(nameof(ReplicationMessageHeader.DatabaseChangeVector), out string changeVector))
            {
                // saving the change vector and the last received document etag
                long lastEtag;
                string lastChangeVector;
                using (documentsContext.OpenReadTransaction())
                {
                    lastEtag = DocumentsStorage.GetLastReplicatedEtagFrom(documentsContext, ConnectionInfo.SourceDatabaseId);
                    lastChangeVector = DocumentsStorage.GetDatabaseChangeVector(documentsContext);
                }

                var status = ChangeVectorUtils.GetConflictStatus(changeVector, lastChangeVector);
                if (status == ConflictStatus.Update || _lastDocumentEtag > lastEtag)
                {
                    if (Logger.IsInfoEnabled)
                    {
                        Logger.Info(
                            $"Try to update the current database change vector ({lastChangeVector}) with {changeVector} in status {status}" +
                            $"with etag: {_lastDocumentEtag} (new) > {lastEtag} (old)");
                    }

                    var cmd = GetUpdateChangeVectorCommand(changeVector, _lastDocumentEtag, ConnectionInfo.SourceDatabaseId, _replicationFromAnotherSource);

                    if (_prevChangeVectorUpdate != null && _prevChangeVectorUpdate.IsCompleted == false)
                    {
                        if (Logger.IsInfoEnabled)
                        {
                            Logger.Info(
                                $"The previous task of updating the database change vector was not completed and has the status of {_prevChangeVectorUpdate.Status}, " +
                                "nevertheless we create an additional task.");
                        }
                    }
                    else
                    {
                        _prevChangeVectorUpdate = _database.TxMerger.Enqueue(cmd);
                    }
                }
            }
        }

        public class DataForReplicationCommand : IDisposable
        {
            internal string SourceDatabaseId { get; set; }

            internal ReplicationBatchItem[] ReplicatedItems { get; set; }

            internal Dictionary<Slice, AttachmentReplicationItem> ReplicatedAttachmentStreams { get; set; }

            public TcpConnectionHeaderMessage.SupportedFeatures SupportedFeatures { get; set; }

            public Logger Logger { get; set; }

            public void Dispose()
            {
                if (ReplicatedAttachmentStreams != null)
                {
                    foreach (var item in ReplicatedAttachmentStreams.Values)
                    {
                        item?.Dispose();
                    }

                    ReplicatedAttachmentStreams?.Clear();
                }

                if (ReplicatedItems != null)
                {
                    foreach (var item in ReplicatedItems)
                    {
                        item?.Dispose();
                    }
                }

                ReplicatedItems = null;
            }
        }

        protected override void HandleTaskCompleteIfNeeded()
        {
        }

        protected override Task HandleBatchAsync(DocumentsOperationContext context, DataForReplicationCommand batch, long lastEtag)
        {
            var replicationCommand = GetMergeDocumentsCommand(batch, lastEtag);
            return _database.TxMerger.Enqueue(replicationCommand);
        }

        protected override DynamicJsonValue GetHeartbeatStatusMessage(DocumentsOperationContext documentsContext, long lastDocumentEtag, string handledMessageType)
        {
            string databaseChangeVector;
            long currentLastEtagMatchingChangeVector;

            using (documentsContext.OpenReadTransaction())
            {
                // we need to get both of them in a transaction, the other side will check if its known change vector
                // is the same or higher then ours, and if so, we'll update the change vector on the sibling to reflect
                // our own latest etag. This allows us to have effective synchronization points, since each change will
                // be able to tell (roughly) where it is at on the entire cluster.
                databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(documentsContext);
                currentLastEtagMatchingChangeVector = DocumentsStorage.ReadLastEtag(documentsContext.Transaction.InnerTransaction);
            }
            if (Logger.IsInfoEnabled)
            {
                Logger.Info($"Sending heartbeat ok => {FromToString} with last document etag = {lastDocumentEtag}, " +
                            $"last document change vector: {databaseChangeVector}");
            }

            var heartbeat = base.GetHeartbeatStatusMessage(documentsContext, lastDocumentEtag, handledMessageType);
            heartbeat[nameof(ReplicationMessageReply.CurrentEtag)] = currentLastEtagMatchingChangeVector;
            heartbeat[nameof(ReplicationMessageReply.DatabaseChangeVector)] = databaseChangeVector;
            heartbeat[nameof(ReplicationMessageReply.DatabaseId)] = _database.DbId.ToString();

            LastHeartbeatTicks = _database.Time.GetUtcNow().Ticks;

            return heartbeat;
        }

        protected override void InvokeOnAttachmentStreamsReceived(int attachmentStreamCount) => AttachmentStreamsReceived?.Invoke(this, attachmentStreamCount);

        protected override void InvokeOnFailed(Exception exception) => Failed?.Invoke(this, exception);

        protected override void InvokeOnDocumentsReceived() => DocumentsReceived?.Invoke(this);

        internal class MergedDocumentReplicationCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly long _lastEtag;
            private readonly DataForReplicationCommand _replicationInfo;

            public MergedDocumentReplicationCommand(DataForReplicationCommand replicationInfo, long lastEtag)
            {
                _replicationInfo = replicationInfo;
                _lastEtag = lastEtag;
            }

            protected virtual ChangeVector PreProcessItem(DocumentsOperationContext context, ReplicationBatchItem item)
            {
                return context.GetChangeVector(item.ChangeVector).Order;
            }

            protected virtual Slice HandleRevisionTombstone(DocumentsOperationContext context, LazyStringValue id, List<IDisposable> toDispose)
            {
                toDispose.Add(Slice.From(context.Allocator, id, out Slice idSlice));
                return idSlice;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var toDispose = new List<IDisposable>();
                var database = context.DocumentDatabase;
                var conflictManager = new ConflictManager(database, database.ReplicationLoader.ConflictResolver);

                try
                {
                    IsIncomingReplication = true;

                    var operationsCount = 0;

                    var lastTransactionMarker = 0;
                    HashSet<LazyStringValue> docCountersToRecreate = null;
                    var handledAttachmentStreams = new HashSet<Slice>(SliceComparer.Instance);
                    context.LastDatabaseChangeVector ??= DocumentsStorage.GetDatabaseChangeVector(context);
                    foreach (var item in _replicationInfo.ReplicatedItems)
                    {
                        if (lastTransactionMarker != item.TransactionMarker)
                        {
                            context.TransactionMarkerOffset++;
                            lastTransactionMarker = item.TransactionMarker;
                        }

                        operationsCount++;

                        var changeVectorToMerge = PreProcessItem(context, item);

                        var incomingChangeVector = context.GetChangeVector(item.ChangeVector);
                        var changeVectorVersion = incomingChangeVector .Version;
                        var changeVectorOrder = incomingChangeVector .Order;

                        context.LastDatabaseChangeVector = ChangeVector.Merge(changeVectorToMerge, context.LastDatabaseChangeVector, context);

                        TimeSeriesStorage tss;
                        LazyStringValue docId;
                        LazyStringValue name;

                        switch (item)
                        {
                            case AttachmentReplicationItem attachment:

                                var localAttachment = database.DocumentsStorage.AttachmentsStorage.GetAttachmentByKey(context, attachment.Key);
                                if (_replicationInfo.ReplicatedAttachmentStreams?.TryGetValue(attachment.Base64Hash, out var attachmentStream) == true)
                                {
                                    if (database.DocumentsStorage.AttachmentsStorage.AttachmentExists(context, attachment.Base64Hash) == false)
                                    {
                                        Debug.Assert(localAttachment == null || AttachmentsStorage.GetAttachmentTypeByKey(attachment.Key) != AttachmentType.Revision,
                                            "the stream should have been written when the revision was added by the document");
                                        database.DocumentsStorage.AttachmentsStorage.PutAttachmentStream(context, attachment.Key, attachmentStream.Base64Hash, attachmentStream.Stream);
                                    }

                                    handledAttachmentStreams.Add(attachment.Base64Hash);
                                }

                                toDispose.Add(DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, attachment.Name, out _, out Slice attachmentName));
                                toDispose.Add(DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, attachment.ContentType, out _, out Slice contentType));

                                if (localAttachment == null || ChangeVectorUtils.GetConflictStatus(attachment.ChangeVector, localAttachment.ChangeVector) !=
                                    ConflictStatus.AlreadyMerged)
                                {
                                    database.DocumentsStorage.AttachmentsStorage.PutDirect(context, attachment.Key, attachmentName,
                                        contentType, attachment.Base64Hash, attachment.ChangeVector);
                                }

                                break;

                            case AttachmentTombstoneReplicationItem attachmentTombstone:

                                var tombstone = AttachmentsStorage.GetAttachmentTombstoneByKey(context, attachmentTombstone.Key);
                                if (tombstone != null && ChangeVectorUtils.GetConflictStatus(item.ChangeVector, tombstone.ChangeVector) == ConflictStatus.AlreadyMerged)
                                    continue;

                                database.DocumentsStorage.AttachmentsStorage.DeleteAttachmentDirect(context, attachmentTombstone.Key, false, "$fromReplication", null,
                                    changeVectorVersion,
                                    attachmentTombstone.LastModifiedTicks);
                                break;

                            case RevisionTombstoneReplicationItem revisionTombstone:

                                var id = HandleRevisionTombstone(context, revisionTombstone.Id, toDispose);

                                database.DocumentsStorage.RevisionsStorage.DeleteRevision(context, id, revisionTombstone.Collection,
                                    changeVectorVersion, revisionTombstone.LastModifiedTicks);
                                break;

                            case CounterReplicationItem counter:
                                var changed = database.DocumentsStorage.CountersStorage.PutCounters(context, counter.Id, counter.Collection, counter.ChangeVector,
                                    counter.Values);
                                if (changed && _replicationInfo.SupportedFeatures.Replication.CaseInsensitiveCounters == false)
                                {
                                    // 4.2 counters
                                    docCountersToRecreate ??= new HashSet<LazyStringValue>(LazyStringValueComparer.Instance);
                                    docCountersToRecreate.Add(counter.Id);
                                }

                                break;

                            case TimeSeriesDeletedRangeItem deletedRange:
                                tss = database.DocumentsStorage.TimeSeriesStorage;

                                TimeSeriesValuesSegment.ParseTimeSeriesKey(deletedRange.Key, context, out docId, out name);

                                var deletionRangeRequest = new TimeSeriesStorage.DeletionRangeRequest
                                {
                                    DocumentId = docId,
                                    Collection = deletedRange.Collection,
                                    Name = name,
                                    From = deletedRange.From,
                                    To = deletedRange.To
                                };
                                var removedChangeVector = tss.DeleteTimestampRange(context, deletionRangeRequest, changeVectorVersion);
                                if (removedChangeVector != null)
                                {
                                    var removed = context.GetChangeVector(removedChangeVector);
                                    context.LastDatabaseChangeVector = ChangeVector.Merge(removed, changeVectorOrder, context);
                                }

                                break;

                            case TimeSeriesReplicationItem segment:
                                tss = database.DocumentsStorage.TimeSeriesStorage;
                                TimeSeriesValuesSegment.ParseTimeSeriesKey(segment.Key, context, out docId, out _, out var baseline);
                                if (_replicationInfo.SupportedFeatures.Replication.IncrementalTimeSeries == false &&
                                    TimeSeriesHandlerProcessorForGetTimeSeries.CheckIfIncrementalTs(segment.Name))
                                {
                                    var msg = $"Detected an item of type Incremental-TimeSeries : '{segment.Name}' on document '{docId}', " +
                                              $"while replication protocol version '{_replicationInfo.SupportedFeatures.ProtocolVersion}' does not support Incremental-TimeSeries feature.";

                                    database.NotificationCenter.Add(AlertRaised.Create(
                                        database.Name,
                                        "Incoming Replication",
                                        msg,
                                        AlertType.Replication,
                                        NotificationSeverity.Error));

                                    throw new LegacyReplicationViolationException(msg);
                                }
                                UpdateTimeSeriesNameIfNeeded(context, docId, segment, tss);
                                
                                var segmentChangeVector = context.GetChangeVector(segment.ChangeVector);
                                if (tss.TryAppendEntireSegment(context, segment, docId, segment.Name, baseline))
                                {
                                    var databaseChangeVector = context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(context);
                                    context.LastDatabaseChangeVector = ChangeVector.Merge(databaseChangeVector, segmentChangeVector, context);
                                    continue;
                                }

                                var options = new TimeSeriesStorage.AppendOptions
                                {
                                    VerifyName = false,
                                    ChangeVectorFromReplication = segment.ChangeVector
                                };

                                var values = segment.Segment.YieldAllValues(context, context.Allocator, baseline);
                                var changeVector = tss.AppendTimestamp(context, docId, segment.Collection, segment.Name, values, options);
                                context.LastDatabaseChangeVector = segmentChangeVector.MergeWith(changeVector, context);

                                break;

                            case DocumentReplicationItem doc:
                                Debug.Assert(doc.Flags.Contain(DocumentFlags.Artificial) == false);

                                BlittableJsonReaderObject document = doc.Data;

                                if (doc.Data != null)
                                {
                                    // if something throws at this point, this means something is really wrong and we should stop receiving documents.
                                    // the other side will receive negative ack and will retry sending again.
                                    try
                                    {
                                        AssertAttachmentsFromReplication(context, doc.Id, document);
                                    }
                                    catch (MissingAttachmentException)
                                    {
                                        if (_replicationInfo.SupportedFeatures.Replication.MissingAttachments)
                                        {
                                            throw;
                                        }

                                        database.NotificationCenter.Add(AlertRaised.Create(
                                            database.Name,
                                            "Incoming Replication",
                                            $"Detected missing attachments for document '{doc.Id}'. Existing attachments in metadata:" +
                                            $" ({string.Join(',', GetAttachmentsNameAndHash(document).Select(x => $"name: {x.Name}, hash: {x.Hash}"))}).",
                                            AlertType.ReplicationMissingAttachments,
                                            NotificationSeverity.Warning));
                                    }
                                }

                                var nonPersistentFlags = NonPersistentDocumentFlags.FromReplication;
                                if (doc.Flags.Contain(DocumentFlags.Revision))
                                {
                                    database.DocumentsStorage.RevisionsStorage.Put(
                                        context,
                                        doc.Id,
                                        document,
                                        doc.Flags,
                                        nonPersistentFlags,
                                        changeVectorVersion,
                                        doc.LastModifiedTicks);
                                    continue;
                                }

                                if (doc.Flags.Contain(DocumentFlags.DeleteRevision))
                                {
                                    database.DocumentsStorage.RevisionsStorage.Delete(
                                        context,
                                        doc.Id,
                                        document,
                                        doc.Flags,
                                        nonPersistentFlags,
                                        changeVectorVersion,
                                        doc.LastModifiedTicks);
                                    continue;
                                }

                                var hasRemoteClusterTx = doc.Flags.Contain(DocumentFlags.FromClusterTransaction);
                                var conflictStatus = ConflictsStorage.GetConflictStatusForDocument(context, doc.Id, doc.ChangeVector, out var hasLocalClusterTx);
                                var flags = doc.Flags;
                                var resolvedDocument = document;

                                switch (conflictStatus)
                                {
                                    case ConflictStatus.Update:

                                        if (resolvedDocument != null)
                                        {
                                            if (flags.Contain(DocumentFlags.HasCounters) &&
                                                _replicationInfo.SupportedFeatures.Replication.CaseInsensitiveCounters == false)
                                            {
                                                var oldDoc = context.DocumentDatabase.DocumentsStorage.Get(context, doc.Id);
                                                if (oldDoc == null)
                                                {
                                                    // 4.2 documents might have counter names in metadata which don't exist in storage
                                                    // we need to replace metadata counters with the counter names from storage

                                                    nonPersistentFlags |= NonPersistentDocumentFlags.ResolveCountersConflict;
                                                }
                                            }

                                            try
                                            {
                                                database.DocumentsStorage.Put(context, doc.Id, null, resolvedDocument, doc.LastModifiedTicks,
                                                    incomingChangeVector, null, flags, nonPersistentFlags);
                                            }
                                            catch (DocumentCollectionMismatchException)
                                            {
                                                goto case ConflictStatus.Conflict;
                                            }
                                        }
                                        else
                                        {
                                            using (DocumentIdWorker.GetSliceFromId(context, doc.Id, out Slice keySlice))
                                            {
                                                database.DocumentsStorage.Delete(
                                                    context, keySlice, doc.Id, null,
                                                    doc.LastModifiedTicks,
                                                    incomingChangeVector,
                                                    new CollectionName(doc.Collection),
                                                    nonPersistentFlags,
                                                    flags);
                                            }
                                        }

                                        break;

                                    case ConflictStatus.Conflict:
                                        if (_replicationInfo.Logger.IsInfoEnabled)
                                            _replicationInfo.Logger.Info(
                                                $"Conflict check resolved to Conflict operation, resolving conflict for doc = {doc.Id}, with change vector = {doc.ChangeVector}");

                                        if (hasLocalClusterTx == hasRemoteClusterTx)
                                        {
                                            // when hasLocalClusterTx and hasRemoteClusterTx both 'true'
                                            // it is a case of a conflict between documents which were modified in a cluster transaction
                                            // in two _different clusters_, so we will treat it as a "normal" conflict

                                            IsIncomingReplication = false;

                                            conflictManager.HandleConflictForDocument(context, doc.Id, doc.Collection, doc.LastModifiedTicks,
                                                document, incomingChangeVector, doc.Flags);
                                            continue;
                                        }

                                        // cluster tx has precedence over regular tx

                                        if (hasLocalClusterTx)
                                            goto case ConflictStatus.AlreadyMerged;

                                        if (hasRemoteClusterTx)
                                            goto case ConflictStatus.Update;

                                        break;

                                    case ConflictStatus.AlreadyMerged:
                                        // we have to do nothing here
                                        break;

                                    default:
                                        throw new ArgumentOutOfRangeException(nameof(conflictStatus),
                                            "Invalid ConflictStatus: " + conflictStatus);
                                }

                                break;

                            default:
                                throw new ArgumentOutOfRangeException(item.GetType().ToString());
                        }
                    }

                    if (docCountersToRecreate != null)
                    {
                        foreach (var id in docCountersToRecreate)
                        {
                            context.DocumentDatabase.DocumentsStorage.DocumentPut.Recreate<DocumentPutAction.RecreateCounters>(context, id);
                        }
                    }

                    Debug.Assert(_replicationInfo.ReplicatedAttachmentStreams == null ||
                                 _replicationInfo.ReplicatedAttachmentStreams.Count == handledAttachmentStreams.Count,
                        "We should handle all attachment streams during WriteAttachment.");

                    //context.LastDatabaseChangeVector being an empty string is valid in the case of receiving docs into
                    //an empty database via filtered pull replication, since it does not modify the db's change vector
                    Debug.Assert(context.LastDatabaseChangeVector != null, $"database: {context.DocumentDatabase.Name};");

                    // instead of : SetLastReplicatedEtagFrom -> _incoming.ConnectionInfo.SourceDatabaseId, _lastEtag , we will store in context and write once right before commit (one time instead of repeating on all docs in the same Tx)
                    SaveSourceEtag(context);
                    return operationsCount;
                }
                finally
                {
                    foreach (var item in toDispose)
                    {
                        item.Dispose();
                    }

                    IsIncomingReplication = false;
                }
            }

            protected virtual void SaveSourceEtag(DocumentsOperationContext context)
            {
                context.LastReplicationEtagFrom ??= new Dictionary<string, long>();
                if (_replicationInfo.SourceDatabaseId != null)
                    context.LastReplicationEtagFrom[_replicationInfo.SourceDatabaseId] = _lastEtag;
            }

            private static void UpdateTimeSeriesNameIfNeeded(DocumentsOperationContext context, LazyStringValue docId, TimeSeriesReplicationItem segment, TimeSeriesStorage tss)
            {
                using (var slicer = new TimeSeriesSliceHolder(context, docId, segment.Name))
                {
                    var localName = tss.Stats.GetTimeSeriesNameOriginalCasing(context, slicer.StatsKey);
                    if (localName == null || localName.CompareTo(segment.Name) <= 0)
                        return;

                    var collectionName = new CollectionName(segment.Collection);
                    tss.Stats.UpdateTimeSeriesName(context, collectionName, slicer);
                    tss.ReplaceTimeSeriesNameInMetadata(context, docId, localName, segment.Name);
                }
            }

            public void AssertAttachmentsFromReplication(DocumentsOperationContext context, string id, BlittableJsonReaderObject document)
            {
                foreach (var attachment in AttachmentsStorage.GetAttachmentsFromDocumentMetadata(document))
                {
                    if (attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false)
                        continue;

                    if (context.DocumentDatabase.DocumentsStorage.AttachmentsStorage.AttachmentExists(context, hash))
                        continue;

                    using (Slice.From(context.Allocator, hash, out var hashSlice))
                    {
                        if (_replicationInfo.ReplicatedAttachmentStreams != null && _replicationInfo.ReplicatedAttachmentStreams.TryGetValue(hashSlice, out _))
                        {
                            // attachment exists but not in the correct order of items (RavenDB-13341)
                            continue;
                        }

                        attachment.TryGet(nameof(AttachmentName.Name), out LazyStringValue attachmentName);

                        var msg = $"Document '{id}' has attachment " +
                                  $"named: '{attachmentName?.ToString() ?? "unknown"}', hash: '{hash?.ToString() ?? "unknown"}' " +
                                  $"listed as one of its attachments but it doesn't exist in the attachment storage";

                        throw new MissingAttachmentException(msg);
                    }
                }
            }

            private IEnumerable<(string Name, string Hash)> GetAttachmentsNameAndHash(BlittableJsonReaderObject document)
            {
                foreach (var attachment in AttachmentsStorage.GetAttachmentsFromDocumentMetadata(document))
                {
                    attachment.TryGet(nameof(AttachmentName.Name), out LazyStringValue name);
                    attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash);

                    yield return (Name: name, Hash: hash);
                }
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                var replicatedAttachmentStreams = _replicationInfo.ReplicatedAttachmentStreams?
                    .Select(kv => KeyValuePair.Create(kv.Key.ToString(), kv.Value.Stream))
                    .ToArray();

                return new MergedDocumentReplicationCommandDto
                {
                    LastEtag = _lastEtag,
                    SupportedFeatures = _replicationInfo.SupportedFeatures,
                    ReplicatedItemDtos = _replicationInfo.ReplicatedItems.Select(i => i.Clone(context)).ToArray(),
                    SourceDatabaseId = _replicationInfo.SourceDatabaseId,
                    ReplicatedAttachmentStreams = replicatedAttachmentStreams
                };
            }
        }
    }

    internal class MergedDocumentReplicationCommandDto : TransactionOperationsMerger.IReplayableCommandDto<IncomingReplicationHandler.MergedDocumentReplicationCommand>
    {
        public ReplicationBatchItem[] ReplicatedItemDtos;
        public long LastEtag;
        public TcpConnectionHeaderMessage.SupportedFeatures SupportedFeatures;
        public string SourceDatabaseId;
        public KeyValuePair<string, Stream>[] ReplicatedAttachmentStreams;

        public IncomingReplicationHandler.MergedDocumentReplicationCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var replicatedItemsCount = ReplicatedItemDtos.Length;
            var replicationItems = new ReplicationBatchItem[replicatedItemsCount];
            for (var i = 0; i < replicatedItemsCount; i++)
            {
                replicationItems[i] = ReplicatedItemDtos[i].Clone(context);
            }

            Dictionary<Slice, AttachmentReplicationItem> replicatedAttachmentStreams = null;
            if (ReplicatedAttachmentStreams != null)
            {
                replicatedAttachmentStreams = new Dictionary<Slice, AttachmentReplicationItem>(SliceComparer.Instance);
                var attachmentStreamsCount = ReplicatedAttachmentStreams.Length;
                for (var i = 0; i < attachmentStreamsCount; i++)
                {
                    var replicationAttachmentStream = ReplicatedAttachmentStreams[i];
                    var item = CreateReplicationAttachmentStream(context, replicationAttachmentStream);
                    replicatedAttachmentStreams[item.Base64Hash] = item;
                }
            }

            var dataForReplicationCommand = new IncomingReplicationHandler.DataForReplicationCommand
            {
                SourceDatabaseId = SourceDatabaseId,
                ReplicatedItems = replicationItems,
                ReplicatedAttachmentStreams = replicatedAttachmentStreams,
                SupportedFeatures = SupportedFeatures,
                Logger = LoggingSource.Instance.GetLogger<IncomingReplicationHandler>(database.Name)
            };

            return new IncomingReplicationHandler.MergedDocumentReplicationCommand(dataForReplicationCommand, LastEtag);
        }

        private AttachmentReplicationItem CreateReplicationAttachmentStream(DocumentsOperationContext context, KeyValuePair<string, Stream> arg)
        {
            var attachmentStream = new AttachmentReplicationItem
            {
                Type = ReplicationBatchItem.ReplicationItemType.AttachmentStream,
                Stream = arg.Value
            };
            attachmentStream.ToDispose(Slice.From(context.Allocator, arg.Key, ByteStringType.Immutable, out attachmentStream.Base64Hash));
            return attachmentStream;
        }
    }
}
