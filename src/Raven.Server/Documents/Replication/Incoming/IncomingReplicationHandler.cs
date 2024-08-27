using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Documents.TransactionMerger.Commands;
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
        public readonly ReplicationLatestEtagRequest.ReplicationType ReplicationType;

        public event Action<IncomingReplicationHandler, Exception> Failed;
        public event Action<IncomingReplicationHandler> DocumentsReceived;
        public event Action<IncomingReplicationHandler, int> AttachmentStreamsReceived;

        public IncomingReplicationHandler(
            TcpConnectionOptions options,
            ReplicationLatestEtagRequest replicatedLastEtag,
            ReplicationLoader parent,
            JsonOperationContext.MemoryBuffer bufferToCopy,
            ReplicationLatestEtagRequest.ReplicationType replicationType) : base(parent, options, bufferToCopy, replicatedLastEtag)
        {
            _database = options.DocumentDatabase;
            _parent = parent;
            _attachmentStreamsTempFile = _database.DocumentsStorage.AttachmentsStorage.GetTempFile("replication");

            LastHeartbeatTicks = _database.Time.GetUtcNow().Ticks;
            ReplicationType = replicationType;
        }

        [ThreadStatic]
        public static bool IsIncomingInternalReplication;

        static IncomingReplicationHandler()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () => IsIncomingInternalReplication = false;
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

        protected override void EnsureNotDeleted()
        {
            if (_database is ShardedDocumentDatabase shardedDatabase)
            {
                _parent.EnsureNotDeleted(DatabaseRecord.GetKeyForDeletionInProgress(_parent.Server.NodeTag, shardedDatabase.ShardNumber));
                return;
            }

            _parent.EnsureNotDeleted(_parent._server.NodeTag);
        }

        private Task _prevChangeVectorUpdate;

        protected override int GetNextReplicationStatsId() => _parent.GetNextReplicationStatsId();

        protected virtual DocumentMergedTransactionCommand GetUpdateChangeVectorCommand(string changeVector, long lastDocumentEtag, IncomingConnectionInfo connectionInfo, AsyncManualResetEvent trigger)
        {
            return new MergedUpdateDatabaseChangeVectorCommand(changeVector, lastDocumentEtag, connectionInfo, trigger);
        }

        protected virtual DocumentMergedTransactionCommand GetMergeDocumentsCommand(DocumentsOperationContext context,
            DataForReplicationCommand data, long lastDocumentEtag)
        {
            return new MergedDocumentReplicationCommand(data, lastDocumentEtag, isInternal: ReplicationType == ReplicationLatestEtagRequest.ReplicationType.Internal);
        }

        internal class MergedUpdateDatabaseChangeVectorCommand : DocumentMergedTransactionCommand
        {
            private readonly string _changeVector;
            private readonly long _lastDocumentEtag;
            private readonly IncomingConnectionInfo _connectionInfo;
            private readonly AsyncManualResetEvent _trigger;

            public MergedUpdateDatabaseChangeVectorCommand(string changeVector, long lastDocumentEtag, IncomingConnectionInfo connectionInfo, AsyncManualResetEvent trigger)
            {
                _changeVector = changeVector;
                _lastDocumentEtag = lastDocumentEtag;
                _connectionInfo = connectionInfo;
                _trigger = trigger;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var operationsCount = 0;
                var lastReplicatedEtag = DocumentsStorage.GetLastReplicatedEtagFrom(context, _connectionInfo.SourceDatabaseId);
                if (_lastDocumentEtag > lastReplicatedEtag)
                {
                    DocumentsStorage.SetLastReplicatedEtagFrom(context, _connectionInfo.SourceDatabaseId, _lastDocumentEtag);
                    operationsCount++;
                }

                if (TryUpdateChangeVector(context))
                    operationsCount++;

                return operationsCount;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
            {
                return new MergedUpdateDatabaseChangeVectorCommandDto
                {
                    ChangeVector = _changeVector,
                    LastDocumentEtag = _lastDocumentEtag,
                    IncomingConnectionInfo = _connectionInfo,
                };
            }

            protected virtual bool TryUpdateChangeVector(DocumentsOperationContext context)
            {
                var current = context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(context);
                var conflictStatus = ChangeVectorUtils.GetConflictStatus(_changeVector, current);
                if (conflictStatus != ConflictStatus.Update)
                {
                    if (string.IsNullOrEmpty(_connectionInfo.SourceDatabaseBase64Id) == false)
                    {
                        var result = ChangeVectorUtils.TryUpdateChangeVector(_connectionInfo.SourceTag, _connectionInfo.SourceDatabaseBase64Id, _lastDocumentEtag, current);
                        if (result.IsValid)
                        {
                            context.LastDatabaseChangeVector = context.GetChangeVector(result.ChangeVector);
                        }
                    }

                    return false;
                }

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
        }

        internal sealed class MergedUpdateDatabaseChangeVectorCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedUpdateDatabaseChangeVectorCommand>
        {
            public string ChangeVector;
            public long LastDocumentEtag;
            public IncomingConnectionInfo IncomingConnectionInfo;

            public MergedUpdateDatabaseChangeVectorCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                var command = new MergedUpdateDatabaseChangeVectorCommand(ChangeVector, LastDocumentEtag, IncomingConnectionInfo,
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

                    var cmd = GetUpdateChangeVectorCommand(changeVector, _lastDocumentEtag, ConnectionInfo, _replicationFromAnotherSource);

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

        public override LiveReplicationPerformanceCollector.ReplicationPerformanceType GetReplicationPerformanceType()
        {
            return ReplicationType == ReplicationLatestEtagRequest.ReplicationType.Internal
                ? LiveReplicationPerformanceCollector.ReplicationPerformanceType.IncomingInternal
                : LiveReplicationPerformanceCollector.ReplicationPerformanceType.IncomingExternal;
        }

        protected override Task HandleBatchAsync(DocumentsOperationContext context, DataForReplicationCommand batch, long lastEtag)
        {
            var replicationCommand = GetMergeDocumentsCommand(context, batch, lastEtag);
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
                currentLastEtagMatchingChangeVector = _database.DocumentsStorage.ReadLastEtag(documentsContext.Transaction.InnerTransaction);
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

        protected override void InvokeOnFailed(Exception exception)
        {
            _parent.ForTestingPurposes?.OnIncomingReplicationHandlerFailure?.Invoke(exception);
            Failed?.Invoke(this, exception);
        }

        protected override void OnDocumentsReceived() => DocumentsReceived?.Invoke(this);

        internal class MergedDocumentReplicationCommand : DocumentMergedTransactionCommand
        {
            private readonly long _lastEtag;
            private readonly bool _isInternal;
            private readonly DataForReplicationCommand _replicationInfo;

            public MergedDocumentReplicationCommand(DataForReplicationCommand replicationInfo, long lastEtag, bool isInternal = false)
            {
                _replicationInfo = replicationInfo;
                _lastEtag = lastEtag;
                _isInternal = isInternal;
            }

            protected virtual ChangeVector PreProcessItem(DocumentsOperationContext context, ReplicationBatchItem item)
            {
                return context.GetChangeVector(item.ChangeVector).Order;
            }

            protected virtual NonPersistentDocumentFlags GetNonPersistentDocumentFlags() => NonPersistentDocumentFlags.FromReplication;

            protected virtual void HandleRevisionTombstone(DocumentsOperationContext context, string docId, string changeVector, out Slice changeVectorSlice, out Slice keySlice, List<IDisposable> toDispose)
            {
                if (docId != null)
                {
                    RevisionsStorage.CreateRevisionTombstoneKeySlice(context, docId, changeVector, out changeVectorSlice, out keySlice, toDispose);
                }
                else
                {
                    toDispose.Add(Slice.From(context.Allocator, changeVector, out keySlice));
                    changeVectorSlice = keySlice;
                }
            }

            protected virtual void SetIsIncomingReplication()
            {
                IsIncomingInternalReplication = _isInternal;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var toDispose = new List<IDisposable>();
                var database = context.DocumentDatabase;
                var conflictManager = new ConflictManager(database, database.ReplicationLoader.ConflictResolver);
                List<(string DocumentId, string ChangeVector, long ModifiedTicks)> pendingAttachmentsTombstoneUpdates = null;

                try
                {
                    SetIsIncomingReplication();

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
                        var changeVectorVersion = incomingChangeVector.Version;

                        context.LastDatabaseChangeVector = ChangeVector.Merge(changeVectorToMerge, context.LastDatabaseChangeVector, context);

                        TimeSeriesStorage tss;
                        LazyStringValue docId;
                        LazyStringValue name;

                        switch (item)
                        {
                            case AttachmentReplicationItem attachment:

                                var result = AttachmentOrTombstone.GetAttachmentOrTombstone(context, attachment.Key);
                                var isRevision = AttachmentsStorage.GetAttachmentTypeByKey(attachment.Key) == AttachmentType.Revision;
                                if (_replicationInfo.ReplicatedAttachmentStreams?.TryGetValue(attachment.Base64Hash, out var attachmentStream) == true)
                                {
                                    if (database.DocumentsStorage.AttachmentsStorage.AttachmentExists(context, attachment.Base64Hash) == false)
                                    {
                                        Debug.Assert(result.Attachment == null || isRevision == false,
                                            "the stream should have been written when the revision was added by the document");
                                        database.DocumentsStorage.AttachmentsStorage.PutAttachmentStream(context, attachment.Key, attachmentStream.Base64Hash, attachmentStream.Stream);
                                    }

                                    handledAttachmentStreams.Add(attachment.Base64Hash);
                                }

                                toDispose.Add(DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, attachment.Name, out _, out Slice attachmentName));
                                toDispose.Add(DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, attachment.ContentType, out _, out Slice contentType));

                                var local = context.GetChangeVector(result.ChangeVector);
                                var newChangeVector = ChangeVectorUtils.GetConflictStatus(incomingChangeVector, local) switch
                                {
                                    // we don't need to worry about the *contents* of the attachments, that is handled by the conflict detection during document replication
                                    ConflictStatus.Conflict => ChangeVector.Merge(incomingChangeVector, local, context),
                                    ConflictStatus.Update => attachment.ChangeVector,
                                    ConflictStatus.AlreadyMerged => null, // nothing to do
                                    _ => throw new ArgumentOutOfRangeException()
                                };

                                if (newChangeVector != null)
                                {
                                    database.DocumentsStorage.AttachmentsStorage.PutDirect(context, attachment.Key, attachmentName,
                                        contentType, attachment.Base64Hash, attachment.RetiredAtUtc, attachment.Flags, attachment.AttachmentSize, isRevision, newChangeVector);
                                }

                                break;

                            case AttachmentTombstoneReplicationItem attachmentTombstone:

                                var attachmentOrTombstone = AttachmentOrTombstone.GetAttachmentOrTombstone(context, attachmentTombstone.Key);
                                var local2 = context.GetChangeVector(attachmentOrTombstone.ChangeVector);

                                if (ChangeVectorUtils.GetConflictStatus(incomingChangeVector, local2) == ConflictStatus.AlreadyMerged)
                                    continue;

                                string documentId = CompoundKeyHelper.ExtractDocumentId(attachmentTombstone.Key); 
                                pendingAttachmentsTombstoneUpdates ??= new();
                                pendingAttachmentsTombstoneUpdates.Add((documentId, incomingChangeVector, attachmentTombstone.LastModifiedTicks));

                                newChangeVector = ChangeVectorUtils.GetConflictStatus(incomingChangeVector, local2) switch
                                {
                                    ConflictStatus.Conflict => ChangeVector.Merge(incomingChangeVector, local2, context),
                                    ConflictStatus.Update => attachmentTombstone.ChangeVector,
                                    _ => throw new ArgumentOutOfRangeException()
                                };
                                //TODO: egor add delete from retired tree as well! write test for that

                                try
                                {
                                    string collection;
                                    using (var doc1 = context.DocumentDatabase.DocumentsStorage.Get(context, documentId, DocumentFields.Data, throwOnConflict: false))
                                    {
                                        doc1.TryGetCollection(out collection);
                                    }

                                    database.DocumentsStorage.AttachmentsStorage.DeleteAttachmentDirect(context, attachmentTombstone.Key, false, "$fromReplication", null,
                                        newChangeVector,
                                        attachmentTombstone.LastModifiedTicks, collection);
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine(e);
                                    throw;
                                }
                                break;

                            case RevisionTombstoneReplicationItem revisionTombstone:

                                RevisionTombstoneReplicationItem.TryExtractDocumentIdAndChangeVectorFromKey(revisionTombstone.Id, out string id, out string revisionChangeVector);
                                HandleRevisionTombstone(context, id, revisionChangeVector, out var changeVectorSlice, out var idKeySlice, toDispose);
                                
                                database.DocumentsStorage.RevisionsStorage.DeleteRevision(context, idKeySlice, revisionTombstone.Collection,
                                    changeVectorVersion, revisionTombstone.LastModifiedTicks, changeVectorSlice, fromReplication: true);
                                break;

                            case CounterReplicationItem counter:
                                var changed = database.DocumentsStorage.CountersStorage.PutCounters(context, counter.Id, counter.Collection, incomingChangeVector,
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
                                tss.DeleteTimestampRange(context, deletionRangeRequest, incomingChangeVector, updateMetadata: false);
                               
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
                                
                                if (tss.TryAppendEntireSegment(context, segment, docId, segment.Name, incomingChangeVector, baseline))
                                {
                                    var databaseChangeVector = context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(context);
                                    context.LastDatabaseChangeVector = ChangeVector.Merge(databaseChangeVector, changeVectorToMerge, context);
                                    continue;
                                }

                                var options = new TimeSeriesStorage.AppendOptions
                                {
                                    VerifyName = false,
                                    ChangeVectorFromReplication = incomingChangeVector
                                };

                                var values = segment.Segment.YieldAllValues(context, context.Allocator, baseline);
                                var changeVector = tss.AppendTimestamp(context, docId, segment.Collection, segment.Name, values, options);
                                context.LastDatabaseChangeVector = changeVectorToMerge.MergeWith(changeVector, context);

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
                                        AssertAttachmentsFromReplication(context, doc);
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

                                var nonPersistentFlags = GetNonPersistentDocumentFlags();
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

                                            IsIncomingInternalReplication = false;

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

                    if (pendingAttachmentsTombstoneUpdates != null)
                    {
                        foreach (var (docId, cv, modifiedTicks) in pendingAttachmentsTombstoneUpdates)
                        {
                            var doc = context.DocumentDatabase.DocumentsStorage.Get(context, docId, DocumentFields.ChangeVector, throwOnConflict: false);

                            // RavenDB-19421: if the document doesn't exist and a conflict for the document doesn't exist, the tombstone doesn't matter
                            // and if the change vector is already merged, we should also check if we had a previous conflict on the existing document
                            // if not, then it is already taken into consideration
                            // we need to force an update when this is _not_ the case, because this replication batch gave us the tombstone only, without
                            // the related document update, so we need to simulate that locally

                            if (doc == null)
                            {
                                var conflicts = database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, docId);
                                foreach (var documentConflict in conflicts)
                                {
                                    if (documentConflict.Flags.Contain(DocumentFlags.HasAttachments) == false ||
                                        ChangeVector.GetConflictStatus(context, cv, documentConflict.ChangeVector) == ConflictStatus.AlreadyMerged)
                                        continue;

                                    // recreate attachments reference
                                    database.DocumentsStorage.AttachmentsStorage.PutAttachmentRevert(context, documentConflict.Id, documentConflict.Doc, out _);
                                }

                                continue;
                            }

                            if (ChangeVector.GetConflictStatus(context, cv, doc.ChangeVector) != ConflictStatus.AlreadyMerged || 
                                doc.Flags.Contain(DocumentFlags.HasAttachments | DocumentFlags.Resolved))
                            {
                                // have to load the full document
                                doc = context.DocumentDatabase.DocumentsStorage.Get(context, docId, fields: DocumentFields.All, throwOnConflict: false);
                                long lastModifiedTicks = Math.Max(modifiedTicks, doc.LastModified.Ticks); // old versions may send with 0 in the tombstone ticks

                                // recreate attachments reference
                                database.DocumentsStorage.AttachmentsStorage.PutAttachmentRevert(context, doc.Id, doc.Data, out _);

                                using var newVer = doc.Data.Clone(context);
                                // now we save it again, and a side effect of that is syncing all the attachments
                                context.DocumentDatabase.DocumentsStorage.Put(context, docId, null, newVer, lastModifiedTicks,
                                    flags: doc.Flags.Strip(DocumentFlags.FromClusterTransaction));
                            }
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
                    RecordDatabaseChangeVector(context);

                    return operationsCount;
                }
                finally
                {
                    foreach (var item in toDispose)
                    {
                        item.Dispose();
                    }

                    IsIncomingInternalReplication = false;
                }
            }

            protected virtual void SaveSourceEtag(DocumentsOperationContext context)
            {
                context.LastReplicationEtagFrom ??= new Dictionary<string, long>();
                context.LastReplicationEtagFrom[_replicationInfo.SourceDatabaseId] = _lastEtag;
            }

            private void RecordDatabaseChangeVector(DocumentsOperationContext context)
            {
                try
                {
                    var stats = _replicationInfo.IncomingHandler.GetLatestReplicationPerformance();
                    var scope = (IncomingReplicationStatsScope)stats.StatsScope;

                    // If the scope is null, it indicates that there are no replication statistics available, and we opt not to create a new one here
                    if (scope == null)
                        return;

                    using (var networkStats = scope.For(ReplicationOperation.Incoming.Network))
                    {
                        networkStats.RecordDatabaseChangeVector(context.LastDatabaseChangeVector);
                    }
                }
                catch (Exception e)
                {
                    if (_replicationInfo.Logger.IsInfoEnabled)
                        _replicationInfo.Logger.Info($"Failed to record the database change vector for database '{context.DocumentDatabase.Name}'.", e);
                }
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

            public void AssertAttachmentsFromReplication(DocumentsOperationContext context, DocumentReplicationItem doc)
            {
                foreach (var attachment in AttachmentsStorage.GetAttachmentsFromDocumentMetadata(doc.Data))
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

                        var type = doc.Flags.Contain(DocumentFlags.Revision) ? $"Revision '{doc.Id}' with change vector '{doc.ChangeVector}'" : $"Document '{doc.Id}'";
                        var msg = $"{type} has attachment " +
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

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
            {
                var replicatedAttachmentStreams = _replicationInfo.ReplicatedAttachmentStreams?
                    .Select(kv => KeyValuePair.Create(kv.Key.ToString(), kv.Value.Stream))
                    .ToArray();

                return new MergedDocumentReplicationCommandDto
                {
                    LastEtag = _lastEtag,
                    SupportedFeatures = _replicationInfo.SupportedFeatures,
                    ReplicatedItemDtos = _replicationInfo.ReplicatedItems.Select(i => i.Clone(context, context.Allocator)).ToArray(),
                    SourceDatabaseId = _replicationInfo.SourceDatabaseId,
                    ReplicatedAttachmentStreams = replicatedAttachmentStreams
                };
            }
        }
    }

    internal sealed class MergedDocumentReplicationCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, IncomingReplicationHandler.MergedDocumentReplicationCommand>
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
                replicationItems[i] = ReplicatedItemDtos[i].Clone(context, context.Allocator);
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
