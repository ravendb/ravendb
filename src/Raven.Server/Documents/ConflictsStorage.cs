using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron.Util;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Schemas.Conflicts;

namespace Raven.Server.Documents
{
    public unsafe class ConflictsStorage
    {
        public static readonly TableSchema ConflictsSchema = Current;
        public long ConflictsCount;

        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;
        private readonly Logger _logger;

        public ConflictsStorage(DocumentDatabase documentDatabase, Transaction tx)
        {
            _documentDatabase = documentDatabase;
            _documentsStorage = documentDatabase.DocumentsStorage;
            _logger = LoggingSource.Instance.GetLogger<ConflictsStorage>(documentDatabase.Name);

            ConflictsSchema.Create(tx, ConflictsSlice, 32);

            var conflictsTable = tx.OpenTable(ConflictsSchema, ConflictsSlice);
            ConflictsCount = conflictsTable.NumberOfEntries;
        }

        public void AssertFixedSizeTrees(Transaction tx)
        {
            var conflictsTable = tx.OpenTable(ConflictsSchema, ConflictsSlice);
            conflictsTable.AssertValidFixedSizeTrees();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ByteStringContext.InternalScope GetConflictsIdPrefix(DocumentsOperationContext context, Slice lowerId, out Slice prefixSlice)
        {
            return GetConflictsIdPrefix(context, lowerId.Content.Ptr, lowerId.Size, out prefixSlice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ByteStringContext.InternalScope GetConflictsIdPrefix(DocumentsOperationContext context, byte* lowerId, int lowerIdSize, out Slice prefixSlice)
        {
            var scope = context.Allocator.Allocate(lowerIdSize + 1, out ByteString keyMem);

            Memory.Copy(keyMem.Ptr, lowerId, lowerIdSize);
            keyMem.Ptr[lowerIdSize] = SpecialChars.RecordSeparator;

            prefixSlice = new Slice(SliceOptions.Key, keyMem);
            return scope;
        }

        public IEnumerable<List<DocumentConflict>> GetAllConflictsBySameId(DocumentsOperationContext context)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);

            var list = new List<DocumentConflict>();
            LazyStringValue lastId = null;

            foreach (var tvr in table.SeekForwardFrom(ConflictsSchema.Indexes[IdAndChangeVectorSlice], Slices.Empty, 0))
            {
                var conflict = TableValueToConflictDocument(context, ref tvr.Result.Reader);

                if (lastId != null && lastId.Equals(conflict.LowerId) == false)
                {
                    yield return list;
                    list = new List<DocumentConflict>();
                }

                list.Add(conflict);
                lastId = conflict.LowerId;
            }

            if (list.Count > 0)
                yield return list;
        }

        public IEnumerable<DocumentConflict> GetConflictsFrom(DocumentsOperationContext context, long etag, long skip = 0)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
            foreach (var tvr in table.SeekForwardFrom(ConflictsSchema.FixedSizeIndexes[AllConflictedDocsEtagsSlice], etag, skip))
            {
                yield return TableValueToConflictDocument(context, ref tvr.Reader);
            }
        }

        public GetConflictsPreviewResult GetConflictsPreviewResult(DocumentsOperationContext context, long skip = 0)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);

            var conflicts = new List<GetConflictsPreviewResult.ConflictPreview>();
            foreach (var tvr in table.SeekBackwardFromLast(ConflictsSchema.FixedSizeIndexes[AllConflictedDocsEtagsSlice], skip))
            {
                var documentConflict = TableValueToConflictDocument(context, ref tvr.Reader);

                var conflict = new GetConflictsPreviewResult.ConflictPreview
                {
                    Id = documentConflict.Id,
                    LastModified = documentConflict.LastModified
                };

                conflicts.Add(conflict);
            }

            conflicts.Sort(ConflictsPreviewComparer.Instance);

            return new GetConflictsPreviewResult
            {
                TotalResults = GetNumberOfConflicts(context),
                Results = conflicts.ToArray()
            };
        }


        public IEnumerable<DocumentConflict> GetConflictsByBucketFrom(DocumentsOperationContext context, int bucket, long etag)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);

            foreach (var result in GetItemsByBucket(context.Allocator, table, ConflictsSchema.DynamicKeyIndexes[ConflictsBucketAndEtagSlice], bucket, etag))
            {
                yield return TableValueToConflictDocument(context, ref result.Result.Reader);
            }
        }

        private static DocumentConflict TableValueToConflictDocument(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            var result = new DocumentConflict
            {
                StorageId = tvr.Id,
                LowerId = TableValueToString(context, (int)ConflictsTable.LowerId, ref tvr),
                Id = TableValueToId(context, (int)ConflictsTable.Id, ref tvr),
                ChangeVector = TableValueToChangeVector(context, (int)ConflictsTable.ChangeVector, ref tvr),
                Etag = TableValueToEtag((int)ConflictsTable.Etag, ref tvr),
                Collection = TableValueToId(context, (int)ConflictsTable.Collection, ref tvr),
                LastModified = TableValueToDateTime((int)ConflictsTable.LastModified, ref tvr),
                Flags = TableValueToFlags((int)ConflictsTable.Flags, ref tvr)
            };

            var read = tvr.Read((int)ConflictsTable.Data, out int size);
            if (size > 0)
            {
                //otherwise this is a tombstone conflict and should be treated as such
                result.Doc = new BlittableJsonReaderObject(read, size, context);
                Transaction.DebugDisposeReaderAfterTransaction(context.Transaction.InnerTransaction, result.Doc);
            }

            return result;
        }

        public static DocumentConflict ParseRawDataSectionConflictWithValidation(JsonOperationContext context, ref TableValueReader tvr, int expectedSize, out long etag)
        {
            var read = tvr.Read((int)ConflictsTable.Data, out var size);
            if (size > expectedSize || size <= 0)
                throw new ArgumentException("Document size is invalid, possible corruption when parsing BlittableJsonReaderObject", nameof(size));

            var result = new DocumentConflict
            {
                StorageId = tvr.Id,
                LowerId = TableValueToString(context, (int)ConflictsTable.LowerId, ref tvr),
                Id = TableValueToId(context, (int)ConflictsTable.Id, ref tvr),
                ChangeVector = TableValueToChangeVector(context, (int)ConflictsTable.ChangeVector, ref tvr),
                Etag = etag = TableValueToEtag((int)ConflictsTable.Etag, ref tvr),
                Doc = new BlittableJsonReaderObject(read, size, context),
                Collection = TableValueToId(context, (int)ConflictsTable.Collection, ref tvr),
                LastModified = TableValueToDateTime((int)ConflictsTable.LastModified, ref tvr),
                Flags = TableValueToFlags((int)ConflictsTable.Flags, ref tvr)
            };

            return result;
        }

        public void ThrowOnDocumentConflict(DocumentsOperationContext context, Slice lowerId)
        {
            using (GetConflictsIdPrefix(context, lowerId, out Slice prefixSlice))
            {
                var conflicts = GetConflictsFor(context, prefixSlice);
                long largestEtag = 0;
                if (conflicts.Count > 0)
                {
                    var conflictRecords = new List<GetConflictsResult.Conflict>();
                    foreach (var conflict in conflicts)
                    {
                        if (largestEtag < conflict.Etag)
                            largestEtag = conflict.Etag;
                        conflictRecords.Add(new GetConflictsResult.Conflict
                        {
                            ChangeVector = conflict.ChangeVector
                        });
                    }

                    ThrowDocumentConflictException(lowerId.ToString(), largestEtag);
                }
            }
        }

        private static void ThrowDocumentConflictException(string docId, long etag)
        {
            throw new DocumentConflictException($"Conflict detected on '{docId}', conflict must be resolved before the document will be accessible.", docId, etag);
        }

        public long GetConflictsMaxEtagFor(DocumentsOperationContext context, Slice prefixSlice)
        {
            if (ConflictsCount == 0)
                return 0;

            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
            long maxEtag = 0L;
            foreach (var tvr in conflictsTable.SeekForwardFrom(ConflictsSchema.Indexes[IdAndChangeVectorSlice], prefixSlice, 0, true))
            {
                var etag = TableValueToEtag((int)ConflictsTable.Etag, ref tvr.Result.Reader);
                if (maxEtag < etag)
                    maxEtag = etag;
            }
            return maxEtag;
        }

        public bool HasHigherChangeVector(DocumentsOperationContext context, Slice prefixSlice, string expectedChangeVector)
        {
            if (ConflictsCount == 0)
                return false;

            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
            foreach (var tvr in conflictsTable.SeekForwardFrom(ConflictsSchema.Indexes[IdAndChangeVectorSlice], prefixSlice, 0, true))
            {
                var changeVector = TableValueToChangeVector(context, (int)ConflictsTable.ChangeVector, ref tvr.Result.Reader);
                if (ChangeVectorUtils.GetConflictStatus(changeVector, expectedChangeVector) == ConflictStatus.AlreadyMerged)
                    return true;
            }
            return false;
        }

        public (IReadOnlyList<string> ChangeVectors, NonPersistentDocumentFlags NonPersistentFlags) DeleteConflictsFor(
            DocumentsOperationContext context, string id, BlittableJsonReaderObject document)
        {
            if (ConflictsCount == 0)
                return (null, NonPersistentDocumentFlags.None);

            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
                return DeleteConflictsFor(context, lowerId, document);
        }

        public (List<string> ChangeVectors, NonPersistentDocumentFlags NonPersistentFlags) DeleteConflictsFor(
            DocumentsOperationContext context, Slice lowerId, BlittableJsonReaderObject document)
        {
            if (ConflictsCount == 0)
                return (null, NonPersistentDocumentFlags.None);

            var changeVectors = new List<string>();
            var nonPersistentFlags = NonPersistentDocumentFlags.None;
            string deleteAttachmentChangeVector = null;
            using (GetConflictsIdPrefix(context, lowerId, out Slice prefixSlice))
            {
                var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
                conflictsTable.DeleteForwardFrom(ConflictsSchema.Indexes[IdAndChangeVectorSlice], prefixSlice, true, long.MaxValue, conflictDocument =>
                {
                    var conflicted = TableValueToConflictDocument(context, ref conflictDocument.Reader);
                    var collection = _documentsStorage.ExtractCollectionName(context, conflicted.Collection);

                    if (conflicted.Doc != null)
                    {
                        if (conflicted.Flags.Contain(DocumentFlags.HasCounters))
                            nonPersistentFlags |= NonPersistentDocumentFlags.ResolveCountersConflict;
                        if (conflicted.Flags.Contain(DocumentFlags.HasTimeSeries))
                            nonPersistentFlags |= NonPersistentDocumentFlags.ResolveTimeSeriesConflict;

                        _documentsStorage.RevisionsStorage.Put(
                            context, conflicted.Id, conflicted.Doc, conflicted.Flags | DocumentFlags.Conflicted | DocumentFlags.HasRevisions, nonPersistentFlags, conflicted.ChangeVector,
                            conflicted.LastModified.Ticks,
                            collectionName: collection,
                            configuration: _documentsStorage.RevisionsStorage.ConflictConfiguration.Default);
                    }
                    else if (conflicted.Flags.Contain(DocumentFlags.FromReplication) == false)
                    {
                        using (Slice.External(context.Allocator, conflicted.LowerId, out var key))
                        {
                            var lastModifiedTicks = _documentDatabase.Time.GetUtcNow().Ticks;
                            _documentsStorage.RevisionsStorage.DeleteRevision(context, key, conflicted.Collection, conflicted.ChangeVector, lastModifiedTicks);
                        }
                    }
                    _documentsStorage.EnsureLastEtagIsPersisted(context, conflicted.Etag);
                    changeVectors.Add(conflicted.ChangeVector);

                    if (conflicted.Flags.Contain(DocumentFlags.HasAttachments) == false)
                        return;

                    if (string.IsNullOrEmpty(deleteAttachmentChangeVector))
                    {
                        var newEtag = _documentsStorage.GenerateNextEtag();
                        deleteAttachmentChangeVector = _documentsStorage.GetNewChangeVector(context, newEtag);
                    }
                    nonPersistentFlags |= DeleteAttachmentConflicts(context, lowerId, document, conflictDocument, deleteAttachmentChangeVector);
                });
            }

            // once this value has been set, we can't set it to false
            // an older transaction may be running and seeing it is false it
            // will not detect a conflict. It is an optimization only that
            // we have to do, so we'll handle it.

            var listCount = changeVectors.Count;
            if (listCount == 0) // there were no conflicts for this document
                return (changeVectors, nonPersistentFlags);

            // Only register the event if we actually deleted any conflicts
            var tx = context.Transaction.InnerTransaction.LowLevelTransaction;
            tx.AfterCommitWhenNewTransactionsPrevented += _ =>
            {
                Interlocked.Add(ref ConflictsCount, -listCount);
            };
            return (changeVectors, nonPersistentFlags | NonPersistentDocumentFlags.Resolved);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private NonPersistentDocumentFlags DeleteAttachmentConflicts(DocumentsOperationContext context, Slice lowerId,
            BlittableJsonReaderObject document, Table.TableValueHolder before, string changeVector)
        {
            var dataPtr = before.Reader.Read((int)ConflictsTable.Data, out int size);
            Debug.Assert(size >= 0);
            if (size <= 0)
                return NonPersistentDocumentFlags.None;

            Debug.Assert(document != null, "This is not a delete conflict so we should also provide the document.");
            using (var conflictDocument = new BlittableJsonReaderObject(dataPtr, size, context))
            {
                _documentsStorage.AttachmentsStorage.DeleteAttachmentConflicts(context, lowerId, document, conflictDocument, changeVector);
            }
            return NonPersistentDocumentFlags.ResolveAttachmentsConflict;
        }

        public void DeleteConflictsFor(DocumentsOperationContext context, string changeVector)
        {
            if (ConflictsCount == 0)
                return;

            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);

            using (Slice.From(context.Allocator, changeVector, out Slice changeVectorSlice))
            {
                if (conflictsTable.DeleteByKey(changeVectorSlice) == false)
                    return;

                var tx = context.Transaction.InnerTransaction.LowLevelTransaction;
                tx.AfterCommitWhenNewTransactionsPrevented += _ =>
                {
                    Interlocked.Decrement(ref ConflictsCount);
                };
            }
        }

        public bool HasConflictsFor(DocumentsOperationContext context, LazyStringValue id)
        {
            if (ConflictsCount == 0)
                return false;

            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            using (GetConflictsIdPrefix(context, lowerId, out Slice prefixSlice))
            {
                var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
                foreach (var _ in conflictsTable.SeekForwardFrom(ConflictsSchema.Indexes[IdAndChangeVectorSlice], prefixSlice, 0, true))
                {
                    return true;
                }
                return false;
            }
        }

        public IReadOnlyList<DocumentConflict> GetConflictsFor(DocumentsOperationContext context, string id)
        {
            if (ConflictsCount == 0)
                return ImmutableAppendOnlyList<DocumentConflict>.Empty;

            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            using (GetConflictsIdPrefix(context, lowerId, out Slice prefixSlice))
            {
                return GetConflictsFor(context, prefixSlice);
            }
        }

        public IReadOnlyList<DocumentConflict> GetConflictsFor(DocumentsOperationContext context, Slice prefixSlice)
        {
            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
            var items = new List<DocumentConflict>();
            foreach (var tvr in conflictsTable.SeekForwardFrom(ConflictsSchema.Indexes[IdAndChangeVectorSlice], prefixSlice, 0, true))
            {
                var conflict = TableValueToConflictDocument(context, ref tvr.Result.Reader);
                items.Add(conflict);
            }
            return items;
        }

        public string GetMergedConflictChangeVectorsAndDeleteConflicts(DocumentsOperationContext context, Slice lowerId, long newEtag, string existingChangeVector = null)
        {
            if (ConflictsCount == 0)
                return MergeVectorsWithoutConflicts(newEtag, existingChangeVector);

            var conflictChangeVectors = DeleteConflictsFor(context, lowerId, null).ChangeVectors;
            if (conflictChangeVectors == null ||
                conflictChangeVectors.Count == 0)
                return MergeVectorsWithoutConflicts(newEtag, existingChangeVector);

            var newChangeVector = ChangeVectorUtils.NewChangeVector(_documentDatabase.ServerStore.NodeTag, newEtag, _documentsStorage.Environment.Base64Id);
            conflictChangeVectors.Add(newChangeVector);
            return ChangeVectorUtils.MergeVectors(conflictChangeVectors);
        }

        private string MergeVectorsWithoutConflicts(long newEtag, string existing)
        {
            if (existing != null)
            {
                var result = ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase.ServerStore.NodeTag, _documentsStorage.Environment.Base64Id, newEtag, existing);
                return result.ChangeVector;
            }
            return ChangeVectorUtils.NewChangeVector(_documentDatabase.ServerStore.NodeTag, newEtag, _documentsStorage.Environment.Base64Id);
        }

        public bool ShouldThrowConcurrencyExceptionOnConflict(DocumentsOperationContext context, Slice lowerId, long? expectedEtag, out long? currentMaxConflictEtag)
        {
            if (expectedEtag.HasValue == false)
            {
                currentMaxConflictEtag = null;
                return false;
            }

            using (GetConflictsIdPrefix(context, lowerId.Content.Ptr, lowerId.Size, out Slice prefixSlice))
            {
                currentMaxConflictEtag = GetConflictsMaxEtagFor(context, prefixSlice);

                return currentMaxConflictEtag != expectedEtag.Value;
            }
        }

        public (string ChangeVector, NonPersistentDocumentFlags NonPersistentFlags) MergeConflictChangeVectorIfNeededAndDeleteConflicts(string documentChangeVector,
            DocumentsOperationContext context, string id, long newEtag, BlittableJsonReaderObject document)
        {
            var result = DeleteConflictsFor(context, id, document);
            if (result.ChangeVectors == null ||
                result.ChangeVectors.Count == 0)
            {
                return (documentChangeVector, result.NonPersistentFlags);
            }

            var changeVectorList = new List<string>
            {
                documentChangeVector,
                context.LastDatabaseChangeVector ?? GetDatabaseChangeVector(context),
                ChangeVectorUtils.NewChangeVector(_documentDatabase, newEtag)
            };
            changeVectorList.AddRange(result.ChangeVectors);
            var merged = ChangeVectorUtils.MergeVectors(changeVectorList);

            return (merged, result.NonPersistentFlags);
        }

        public void AddConflict(
            DocumentsOperationContext context,
            string id,
            long lastModifiedTicks,
            BlittableJsonReaderObject incomingDoc,
            string incomingChangeVector,
            string incomingTombstoneCollection,
            DocumentFlags flags,
            NonPersistentDocumentFlags nonPersistentFlags = NonPersistentDocumentFlags.None)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Adding conflict to {id} (Incoming change vector {incomingChangeVector})");

            var tx = context.Transaction.InnerTransaction;
            var conflictsTable = tx.OpenTable(ConflictsSchema, ConflictsSlice);

            var fromSmuggler = nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromSmuggler);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out Slice lowerId, out Slice idPtr))
            {
                CollectionName collectionName;

                // ReSharper disable once ArgumentsStyleLiteral
                var existing = _documentsStorage.GetDocumentOrTombstone(context, id, throwOnConflict: false);
                if (existing.Document != null)
                {
                    var existingDoc = existing.Document;
                    collectionName = _documentsStorage.ExtractCollectionName(context, existingDoc.Data);

                    if (fromSmuggler == false)
                    {
                        AddToConflictsTable(existingDoc.ChangeVector,
                            collectionName.Name,
                            existingDoc.Data.BasePointer,
                            existingDoc.Data.Size,
                            existingDoc.LastModified.Ticks,
                            (int)existingDoc.Flags);
                    }

                    // we delete the data directly, without generating a tombstone, because we have a 
                    // conflict instead
                    _documentsStorage.EnsureLastEtagIsPersisted(context, existingDoc.Etag);

                    //make sure that the relevant collection tree exists
                    var table = tx.OpenTable(DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));
                    table.Delete(existingDoc.StorageId);
                }
                else if (existing.Tombstone != null)
                {
                    var existingTombstone = existing.Tombstone;

                    if (fromSmuggler == false)
                    {
                        AddToConflictsTable(existingTombstone.ChangeVector,
                            existingTombstone.Collection,
                            data: null,
                            dataSize: 0,
                            existingTombstone.LastModified.Ticks,
                            (int)existingTombstone.Flags);
                    }

                    // we delete the data directly, without generating a tombstone, because we have a 
                    // conflict instead
                    _documentsStorage.EnsureLastEtagIsPersisted(context, existingTombstone.Etag);

                    collectionName = _documentsStorage.GetCollection(existingTombstone.Collection, throwIfDoesNotExist: true);

                    var table = tx.OpenTable(TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));
                    table.Delete(existingTombstone.StorageId);

                }
                else // has existing conflicts
                {
                    collectionName = _documentsStorage.ExtractCollectionName(context, incomingDoc);

                    using (GetConflictsIdPrefix(context, lowerId, out Slice prefixSlice))
                    {
                        var conflicts = GetConflictsFor(context, prefixSlice);

                        ThrowIfNoConflictsWereFound(fromSmuggler, conflicts.Count, id);

                        foreach (var conflict in conflicts)
                        {
                            var conflictStatus = ChangeVectorUtils.GetConflictStatus(incomingChangeVector, conflict.ChangeVector);
                            switch (conflictStatus)
                            {
                                case ConflictStatus.Update:
                                    DeleteConflictsFor(context, conflict.ChangeVector); // delete this, it has been subsumed
                                    break;
                                case ConflictStatus.Conflict:
                                    if (fromSmuggler &&
                                        DocumentCompare.IsEqualTo(conflict.Doc, incomingDoc, DocumentCompare.DocumentCompareOptions.Default) == DocumentCompareResult.Equal)
                                    {
                                        return; // we already have a conflict with equal content, no need to create another one
                                    }
                                    continue; // we'll add this conflict if no one else also includes it
                                case ConflictStatus.AlreadyMerged:
                                    return; // we already have a conflict that includes this version
                                default:
                                    throw new ArgumentOutOfRangeException("Invalid conflict status " + conflictStatus);
                            }
                        }
                    }
                }

                byte* doc = null;
                var docSize = 0;
                string collection;
                if (incomingDoc != null) // can be null if it is a tombstone
                {
                    doc = incomingDoc.BasePointer;
                    docSize = incomingDoc.Size;
                    collection = _documentsStorage.ExtractCollectionName(context, incomingDoc).Name;
                }
                else
                {
                    collection = incomingTombstoneCollection;
                }

                AddToConflictsTable(incomingChangeVector,
                    collection,
                    doc,
                    docSize,
                    lastModifiedTicks,
                    (int)flags);

                context.Transaction.AddAfterCommitNotification(new DocumentChange
                {
                    ChangeVector = incomingChangeVector,
                    CollectionName = collectionName.Name,
                    Id = id,
                    Type = DocumentChangeTypes.Conflict
                });

                void AddToConflictsTable(string changeVector, string col, byte* data, int dataSize, long lastModified, int documentFlags)
                {
                    using (Slice.From(context.Allocator, changeVector, out Slice cv))
                    {
                        using (DocumentIdWorker.GetStringPreserveCase(context, col, out Slice collectionSlice))
                        using (conflictsTable.Allocate(out TableValueBuilder tvb))
                        {
                            tvb.Add(lowerId);
                            tvb.Add(SpecialChars.RecordSeparator);
                            tvb.Add(cv);
                            tvb.Add(idPtr);
                            tvb.Add(data, dataSize);
                            tvb.Add(Bits.SwapBytes(_documentsStorage.GenerateNextEtag()));
                            tvb.Add(collectionSlice);
                            tvb.Add(lastModified);
                            tvb.Add(documentFlags);
                            if (conflictsTable.Set(tvb))
                                Interlocked.Increment(ref ConflictsCount);
                        }
                    }
                }
            }
        }

        [Conditional("DEBUG")]
        private static void ThrowIfNoConflictsWereFound(bool fromSmuggler, int conflictsCount, string id)
        {
            if (fromSmuggler == false && conflictsCount == 0)
                throw new InvalidOperationException($"No existing conflict or document was found for '{id}'.");
        }

        public DeleteOperationResult? DeleteConflicts(DocumentsOperationContext context, Slice lowerId,
            string expectedChangeVector, string changeVector)
        {
            using (GetConflictsIdPrefix(context, lowerId, out Slice prefixSlice))
            {
                var conflicts = GetConflictsFor(context, prefixSlice);
                if (conflicts.Count > 0)
                {
                    // We do have a conflict for our deletion candidate
                    // Since this document resolve the conflict we don't need to alter the change vector.
                    // This way we avoid another replication back to the source

                    ThrowConcurrencyExceptionOnConflictIfNeeded(context, lowerId, expectedChangeVector);

                    var collectionName = ResolveConflictAndAddTombstone(context, changeVector, conflicts, out long etag);
                    context.Transaction.AddAfterCommitNotification(new DocumentChange
                    {
                        Type = DocumentChangeTypes.Delete,
                        Id = lowerId.ToString(),
                        ChangeVector = changeVector,
                        CollectionName = collectionName.Name,
                    });
                    return new DeleteOperationResult
                    {
                        Collection = collectionName,
                        Etag = etag,
                        ChangeVector = changeVector
                    };
                }
            }
            return null;
        }

        public void ThrowConcurrencyExceptionOnConflictIfNeeded(DocumentsOperationContext context, Slice lowerId, string expectedChangeVector)
        {
            if (expectedChangeVector == null)
                return;

            if (HasHigherChangeVector(context, lowerId, expectedChangeVector))
            {
                throw new ConcurrencyException($"Failed to resolve document conflict with change vector = {expectedChangeVector}, because we have a newer change vector.")
                {
                    Id = lowerId.ToString(),
                    ExpectedChangeVector = expectedChangeVector
                };
            }
        }

        private CollectionName ResolveConflictAndAddTombstone(DocumentsOperationContext context, string changeVector,
            IReadOnlyList<DocumentConflict> conflicts, out long etag)
        {
            var indexOfLargestEtag = FindIndexOfLargestEtagAndMergeChangeVectors(conflicts, out string mergedChangeVector);
            var latestConflict = conflicts[indexOfLargestEtag];
            var collectionName = new CollectionName(latestConflict.Collection);

            using (DocumentIdWorker.GetSliceFromId(context, latestConflict.Id, out Slice lowerId))
            {
                //note that CreateTombstone is also deleting conflicts
                etag = _documentsStorage.CreateTombstone(context,
                    lowerId,
                    latestConflict.Etag,
                    collectionName,
                    context.GetLazyString(mergedChangeVector),
                    latestConflict.LastModified.Ticks,
                    changeVector,
                    latestConflict.Flags,
                    NonPersistentDocumentFlags.None).Etag;
            }

            return collectionName;
        }

        private static int FindIndexOfLargestEtagAndMergeChangeVectors(IReadOnlyList<DocumentConflict> conflicts, out string mergedChangeVectorEntries)
        {
            mergedChangeVectorEntries = null;
            bool firstTime = true;

            int indexOfLargestEtag = 0;
            long largestEtag = 0;
            for (var i = 0; i < conflicts.Count; i++)
            {
                var conflict = conflicts[i];
                if (conflict.Etag > largestEtag)
                {
                    largestEtag = conflict.Etag;
                    indexOfLargestEtag = i;
                }

                if (firstTime)
                {
                    mergedChangeVectorEntries = conflict.ChangeVector;
                    firstTime = false;
                    continue;
                }
                mergedChangeVectorEntries = ChangeVectorUtils.MergeVectors(mergedChangeVectorEntries, conflict.ChangeVector);
            }

            return indexOfLargestEtag;
        }

        public static ConflictStatus GetConflictStatusForDocument(DocumentsOperationContext context, string id, string changeVector, out bool hasLocalClusterTx)
        {
            hasLocalClusterTx = false;

            //tombstones also can be a conflict entry
            var conflicts = context.DocumentDatabase.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, id);
            ConflictStatus status;
            if (conflicts.Count > 0)
            {
                foreach (var existingConflict in conflicts)
                {
                    status = ChangeVectorUtils.GetConflictStatus(changeVector, existingConflict.ChangeVector);
                    if (status == ConflictStatus.Conflict)
                    {
                        ConflictManager.AssertChangeVectorNotNull(existingConflict.ChangeVector);
                        return ConflictStatus.Conflict;
                    }
                }
                // this document will resolve the conflicts when putted
                return ConflictStatus.Update;
            }

            var result = context.DocumentDatabase.DocumentsStorage.GetDocumentOrTombstone(context, id);
            string local;

            if (result.Document != null)
            {
                local = result.Document.ChangeVector;
                hasLocalClusterTx = result.Document.Flags.Contain(DocumentFlags.FromClusterTransaction);
            }
            else if (result.Tombstone != null)
            {
                local = result.Tombstone.ChangeVector;
                hasLocalClusterTx = result.Tombstone.Flags.Contain(DocumentFlags.FromClusterTransaction);
            }
            else
                return ConflictStatus.Update; //document with 'id' doesn't exist locally, so just do PUT

            status = context.DocumentDatabase.DocumentsStorage.GetConflictStatus(changeVector, local, out var skipValidation);
            context.SkipChangeVectorValidation |= skipValidation;

            if (status == ConflictStatus.Conflict)
            {
                ConflictManager.AssertChangeVectorNotNull(local);
            }
            return status;
        }

        public long GetNumberOfDocumentsConflicts(DocumentsOperationContext context)
        {
            if (ConflictsCount == 0)
                return 0;

            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
            return conflictsTable.GetTree(ConflictsSchema.Indexes[ConflictsIdSlice]).State.NumberOfEntries;
        }

        public long GetNumberOfConflicts(DocumentsOperationContext context)
        {
            var table = new Table(ConflictsSchema, context.Transaction.InnerTransaction);
            return table.GetNumberOfEntriesFor(ConflictsSchema.FixedSizeIndexes[AllConflictedDocsEtagsSlice]);
        }

        public string GetCollection(DocumentsOperationContext context, string id)
        {
            LazyStringValue collection = null;
            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            using (GetConflictsIdPrefix(context, lowerId, out Slice prefixSlice))
            {
                foreach (var conflict in GetConflictsFor(context, prefixSlice))
                {
                    if (collection == null)
                        collection = conflict.Collection;

                    if (conflict.Collection.Equals(collection) == false)
                        throw new NotSupportedException($"Two different collections were found: '{collection}' and '{conflict.Collection}'");
                }

                if (collection == null)
                    throw new NotSupportedException($"Collection not found.");
            }

            return collection;
        }

        [StorageIndexEntryKeyGenerator]
        internal static ByteStringContext.Scope GenerateBucketAndEtagIndexKeyForConflicts(ByteStringContext context, ref TableValueReader tvr, out Slice slice)
        {
            return GenerateBucketAndEtagIndexKey(context, idIndex: (int)ConflictsTable.LowerId, etagIndex: (int)ConflictsTable.Etag, ref tvr, out slice);
        }
    }
}
