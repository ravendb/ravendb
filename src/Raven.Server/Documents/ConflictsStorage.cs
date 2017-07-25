using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Exceptions;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Extensions;
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
using Sparrow.Utils;
using Voron.Util;
using static Raven.Server.Documents.DocumentsStorage;
using ConcurrencyException = Voron.Exceptions.ConcurrencyException;

namespace Raven.Server.Documents
{
    public unsafe class ConflictsStorage
    {
        private static readonly Slice ChangeVectorSlice;
        private static readonly Slice IdAndChangeVectorSlice;
        public static readonly Slice AllConflictedDocsEtagsSlice;
        private static readonly Slice ConflictedCollectionSlice;
        public static readonly Slice ConflictsSlice;
        private static readonly Slice IdSlice;

        public static readonly TableSchema ConflictsSchema = new TableSchema();

        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;
        private readonly Logger _logger;

        public long ConflictsCount;

        private enum ConflictsTable
        {
            LowerId = 0,
            RecordSeparator = 1,
            ChangeVector = 2,
            Id = 3,
            Data = 4,
            Etag = 5,
            Collection = 6,
            LastModified = 7,
            Flags = 8,
        }

        static ConflictsStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "ChangeVector", ByteStringType.Immutable, out ChangeVectorSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Id", ByteStringType.Immutable, out IdSlice);
            Slice.From(StorageEnvironment.LabelsContext, "IdAndChangeVector", ByteStringType.Immutable, out IdAndChangeVectorSlice);
            Slice.From(StorageEnvironment.LabelsContext, "AllConflictedDocsEtags", ByteStringType.Immutable, out AllConflictedDocsEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "ConflictedCollection", ByteStringType.Immutable, out ConflictedCollectionSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Conflicts", ByteStringType.Immutable, out ConflictsSlice);

            /*
             The structure of conflicts table starts with the following fields:
             [ Conflicted Doc Id | Separator | Change Vector | ... the rest of fields ... ]
             PK of the conflicts table will be 'Change Vector' field, because when dealing with conflicts,
              the change vectors will always be different, hence the uniqueness of the ID. (inserts/updates will not overwrite)

            Additional indice is set to have composite ID of 'Conflicted Doc Id' and 'Change Vector' so we will be able to iterate
            on conflicts by conflicted doc id (using 'starts with')

            We need a separator in order to delete all conflicts all "users/1" without deleting "users/11" conflics.
             */

            ConflictsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)ConflictsTable.ChangeVector,
                Count = 1,
                IsGlobal = false,
                Name = ChangeVectorSlice
            });
            // required to get conflicts by ID
            ConflictsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)ConflictsTable.LowerId,
                Count = 3,
                IsGlobal = false,
                Name = IdAndChangeVectorSlice
            });
            ConflictsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)ConflictsTable.LowerId,
                Count = 1,
                IsGlobal = true,
                Name = IdSlice
            });
            ConflictsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)ConflictsTable.Etag,
                IsGlobal = true,
                Name = AllConflictedDocsEtagsSlice
            });
            ConflictsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)ConflictsTable.Collection,
                Count = 1,
                IsGlobal = true,
                Name = ConflictedCollectionSlice
            });
        }

        public ConflictsStorage(DocumentDatabase documentDatabase, Transaction tx)
        {
            _documentDatabase = documentDatabase;
            _documentsStorage = documentDatabase.DocumentsStorage;
            _logger = LoggingSource.Instance.GetLogger<ConflictsStorage>(documentDatabase.Name);

            ConflictsSchema.Create(tx, ConflictsSlice, 32);

            ConflictsCount = tx.OpenTable(ConflictsSchema, ConflictsSlice).NumberOfEntries;
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

            foreach (var tvr in table.SeekForwardFrom(ConflictsSchema.Indexes[IdAndChangeVectorSlice], Slices.Empty, 0, false))
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

        public IEnumerable<ReplicationBatchItem> GetConflictsFrom(DocumentsOperationContext context, long etag)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
            foreach (var tvr in table.SeekForwardFrom(ConflictsSchema.FixedSizeIndexes[AllConflictedDocsEtagsSlice], etag, 0))
            {
                yield return ReplicationBatchItem.From(TableValueToConflictDocument(context, ref tvr.Reader));
            }
        }

        public IEnumerable<DocumentConflict> GetConflictsAfter(DocumentsOperationContext context, long etag)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
            foreach (var tvr in table.SeekForwardFrom(ConflictsSchema.FixedSizeIndexes[AllConflictedDocsEtagsSlice], etag, 0))
            {
                yield return TableValueToConflictDocument(context, ref tvr.Reader);
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
                Collection = TableValueToString(context, (int)ConflictsTable.Collection, ref tvr),
                LastModified = TableValueToDateTime((int)ConflictsTable.LastModified, ref tvr),
                Flags = TableValueToFlags((int)ConflictsTable.Flags, ref tvr),
            };

            var read = tvr.Read((int)ConflictsTable.Data, out int size);
            if (size > 0)
            {
                //otherwise this is a tombstone conflict and should be treated as such
                result.Doc = new BlittableJsonReaderObject(read, size, context);
                DebugDisposeReaderAfterTransaction(context.Transaction, result.Doc);
            }

            return result;
        }

        public void ThrowOnDocumentConflict(DocumentsOperationContext context, Slice lowerId)
        {
            //TODO: don't forget to refactor this method
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
                var deleteCount = conflictsTable.DeleteForwardFrom(ConflictsSchema.Indexes[IdAndChangeVectorSlice], prefixSlice, true, long.MaxValue, conflictDocument =>
                {
                    var etag = TableValueToEtag((int)ConflictsTable.Etag, ref conflictDocument.Reader);
                    _documentsStorage.EnsureLastEtagIsPersisted(context, etag);

                    var conflictChangeVector = TableValueToChangeVector(context, (int)ConflictsTable.ChangeVector, ref conflictDocument.Reader);
                    changeVectors.Add(conflictChangeVector);

                    var flags = TableValueToFlags((int)ConflictsTable.Flags, ref conflictDocument.Reader);
                    if ((flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments)
                        return;

                    if (string.IsNullOrEmpty(deleteAttachmentChangeVector))
                    {
                        var newEtag = _documentsStorage.GenerateNextEtag();
                        deleteAttachmentChangeVector = _documentsStorage.GetNewChangeVector(context, newEtag);
                        context.LastDatabaseChangeVector = conflictChangeVector;
                    }
                    nonPersistentFlags |= DeleteAttachmentConflicts(context, lowerId, document, conflictDocument, deleteAttachmentChangeVector);
                });
            }

            // once this value has been set, we can't set it to false
            // an older transaction may be running and seeing it is false it
            // will not detect a conflict. It is an optimization only that
            // we have to do, so we'll handle it.

            // Only register the event if we actually deleted any conflicts
            var listCount = changeVectors.Count;
            if (listCount > 0)
            {
                var tx = context.Transaction.InnerTransaction.LowLevelTransaction;
                tx.AfterCommitWhenNewReadTransactionsPrevented += () =>
                {
                    Interlocked.Add(ref ConflictsCount, -listCount);
                };
            }
            return (changeVectors, nonPersistentFlags);
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
                tx.AfterCommitWhenNewReadTransactionsPrevented += () =>
                {
                    Interlocked.Decrement(ref ConflictsCount);
                };
            }
        }

        public DocumentConflict GetConflictForChangeVector(
            DocumentsOperationContext context,
            string id,
            LazyStringValue changeVector)
        {
            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);

            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            using (GetConflictsIdPrefix(context, lowerId, out Slice prefixSlice))
            {
                foreach (var tvr in conflictsTable.SeekForwardFrom(ConflictsSchema.Indexes[IdAndChangeVectorSlice], prefixSlice, 0, true))
                {
                    var currentChangeVector = TableValueToChangeVector(context, (int)ConflictsTable.ChangeVector, ref tvr.Result.Reader);
                    if (changeVector.CompareTo(currentChangeVector) == 0)
                    {
                        var dataPtr = tvr.Result.Reader.Read((int)ConflictsTable.Data, out int size);
                        var doc = size == 0 ? null : new BlittableJsonReaderObject(dataPtr, size, context);
                        DebugDisposeReaderAfterTransaction(context.Transaction, doc);
                        return new DocumentConflict
                        {
                            ChangeVector = currentChangeVector,
                            Id = context.AllocateStringValue(id, tvr.Result.Reader.Read((int)ConflictsTable.Id, out size), size),
                            StorageId = tvr.Result.Reader.Id,
                            //size == 0 --> this is a tombstone conflict
                            Doc = doc
                        };
                    }
                }
            }
            return null;
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
                return MergeVectorsWithoutConflicts(context, newEtag, existingChangeVector);

            var conflictChangeVectors = DeleteConflictsFor(context, lowerId, null).ChangeVectors;
            if (conflictChangeVectors == null ||
                conflictChangeVectors.Count == 0)
                return MergeVectorsWithoutConflicts(context, newEtag, existingChangeVector);

            var newChangeVector = ChangeVectorUtils.NewChangeVector(_documentDatabase.ServerStore.NodeTag, newEtag, _documentsStorage.Environment.DbId);
            conflictChangeVectors.Add(newChangeVector);
            return ChangeVectorUtils.MergeVectors(conflictChangeVectors);
        }

        private string MergeVectorsWithoutConflicts(DocumentsOperationContext context, long newEtag, string existing)
        {
            if (existing != null)
            {
                ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase.ServerStore.NodeTag, _documentsStorage.Environment.DbId, newEtag, ref existing);
                return existing;
            }
            return ChangeVectorUtils.NewChangeVector(_documentDatabase.ServerStore.NodeTag, newEtag, _documentsStorage.Environment.DbId);
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

        public void ThrowConcurrencyExceptionOnConflict(long? expectedEtag, long? currentMaxConflictEtag)
        {
            throw new ConcurrencyException(
                $"Tried to resolve document conflict with etag = {expectedEtag}, but the current max conflict etag is {currentMaxConflictEtag}. " +
                "This means that the conflict information with which you are trying to resolve the conflict is outdated. " +
                "Get conflict information and try resolving again.");
        }

        public (string ChangeVector, NonPersistentDocumentFlags NonPersistentFlags) MergeConflictChangeVectorIfNeededAndDeleteConflicts(
            string documentChangeVector, DocumentsOperationContext context, string id, long newEtag, BlittableJsonReaderObject document)
        {
            var result = DeleteConflictsFor(context, id, document);
            if (result.ChangeVectors == null ||
                result.ChangeVectors.Count == 0)
            {
                return (documentChangeVector, result.NonPersistentFlags);
            }

            string mergedChangeVectorEntries = null;
            var firstTime = true;
            foreach (var changeVector in result.ChangeVectors)
            {
                if (firstTime)
                {
                    mergedChangeVectorEntries = changeVector;
                    firstTime = false;
                    continue;
                }
                mergedChangeVectorEntries = ChangeVectorUtils.MergeVectors(mergedChangeVectorEntries, changeVector);
            }
            if (string.IsNullOrEmpty(documentChangeVector) == false)
                mergedChangeVectorEntries = ChangeVectorUtils.MergeVectors(mergedChangeVectorEntries, documentChangeVector);
            var newChangeVector =
                ChangeVectorUtils.NewChangeVector(_documentDatabase.ServerStore.NodeTag, newEtag, _documentDatabase.DbId);
            mergedChangeVectorEntries = ChangeVectorUtils.MergeVectors(mergedChangeVectorEntries, newChangeVector);

            return (mergedChangeVectorEntries, result.NonPersistentFlags);
        }

        public void AddConflict(
            DocumentsOperationContext context,
            string id,
            long lastModifiedTicks,
            BlittableJsonReaderObject incomingDoc,
            string incomingChangeVector,
            string incomingTombstoneCollection,
            DocumentFlags flags)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Adding conflict to {id} (Incoming change vector {incomingChangeVector})");

            var tx = context.Transaction.InnerTransaction;
            var conflictsTable = tx.OpenTable(ConflictsSchema, ConflictsSlice);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out Slice lowerId, out Slice idPtr))
            {
                CollectionName collectionName;

                // ReSharper disable once ArgumentsStyleLiteral
                var existing = _documentsStorage.GetDocumentOrTombstone(context, id, throwOnConflict: false);
                LazyStringValue lazyCollectionName;
                if (existing.Document != null)
                {
                    var existingDoc = existing.Document;

                    lazyCollectionName = CollectionName.GetLazyCollectionNameFrom(context, existingDoc.Data);

                    using (Slice.From(context.Allocator, existingDoc.ChangeVector, out var cv))
                    using (conflictsTable.Allocate(out TableValueBuilder tvb))
                    {
                        tvb.Add(lowerId);
                        tvb.Add(SpecialChars.RecordSeparator);
                        tvb.Add(cv.Content.Ptr, cv.Size);
                        tvb.Add(idPtr);
                        tvb.Add(existingDoc.Data.BasePointer, existingDoc.Data.Size);
                        tvb.Add(Bits.SwapBytes(_documentsStorage.GenerateNextEtag()));
                        tvb.Add(lazyCollectionName.Buffer, lazyCollectionName.Size);
                        tvb.Add(existingDoc.LastModified.Ticks);
                        tvb.Add((int)existingDoc.Flags);
                        if (conflictsTable.Set(tvb))
                            Interlocked.Increment(ref ConflictsCount);
                    }

                    // we delete the data directly, without generating a tombstone, because we have a 
                    // conflict instead
                    _documentsStorage.EnsureLastEtagIsPersisted(context, existingDoc.Etag);
                    collectionName = _documentsStorage.ExtractCollectionName(context, existingDoc.Id, existingDoc.Data);

                    //make sure that the relevant collection tree exists
                    var table = tx.OpenTable(DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));
                    table.Delete(existingDoc.StorageId);
                }
                else if (existing.Tombstone != null)
                {
                    var existingTombstone = existing.Tombstone;
                    using (Slice.From(context.Allocator, existingTombstone.ChangeVector, out var cv))
                    using (conflictsTable.Allocate(out TableValueBuilder tvb))
                    {
                        tvb.Add(lowerId);
                        tvb.Add(SpecialChars.RecordSeparator);
                        tvb.Add(cv.Content.Ptr, cv.Size);
                        tvb.Add(idPtr);
                        tvb.Add(null, 0);
                        tvb.Add(Bits.SwapBytes(_documentsStorage.GenerateNextEtag()));
                        tvb.Add(existingTombstone.Collection.Buffer, existingTombstone.Collection.Size);
                        tvb.Add(existingTombstone.LastModified.Ticks);
                        tvb.Add((int)existingTombstone.Flags);
                        if (conflictsTable.Set(tvb))
                            Interlocked.Increment(ref ConflictsCount);
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
                    collectionName = _documentsStorage.ExtractCollectionName(context, id, incomingDoc);

                    using (GetConflictsIdPrefix(context, lowerId, out Slice prefixSlice))
                    {
                        var conflicts = GetConflictsFor(context, prefixSlice);
                        foreach (var conflict in conflicts)
                        {
                            var conflictStatus = ChangeVectorUtils.GetConflictStatus(incomingChangeVector, conflict.ChangeVector);
                            switch (conflictStatus)
                            {
                                case ConflictStatus.Update:
                                    DeleteConflictsFor(context, conflict.ChangeVector); // delete this, it has been subsumed
                                    break;
                                case ConflictStatus.Conflict:
                                    break; // we'll add this conflict if no one else also includes it
                                case ConflictStatus.AlreadyMerged:
                                    return; // we already have a conflict that includes this version
                                default:
                                    throw new ArgumentOutOfRangeException("Invalid conflict status " + conflictStatus);
                            }
                        }
                    }
                }

                var etag = _documentsStorage.GenerateNextEtag();
                ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase.ServerStore.NodeTag, _documentDatabase.DbId, etag, ref context.LastDatabaseChangeVector);

                byte* doc = null;
                var docSize = 0;
                if (incomingDoc != null) // can be null if it is a tombstone
                {
                    doc = incomingDoc.BasePointer;
                    docSize = incomingDoc.Size;
                    lazyCollectionName = CollectionName.GetLazyCollectionNameFrom(context, incomingDoc);
                }
                else
                {
                    lazyCollectionName = context.GetLazyString(incomingTombstoneCollection);
                }

                using (lazyCollectionName)
                using (Slice.From(context.Allocator, incomingChangeVector, out var cv))
                using (conflictsTable.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(lowerId);
                    tvb.Add(SpecialChars.RecordSeparator);
                    tvb.Add(cv.Content.Ptr, cv.Size);
                    tvb.Add(idPtr);
                    tvb.Add(doc, docSize);
                    tvb.Add(Bits.SwapBytes(etag));
                    tvb.Add(lazyCollectionName.Buffer, lazyCollectionName.Size);
                    tvb.Add(lastModifiedTicks);
                    tvb.Add((int)flags);
                    if (conflictsTable.Set(tvb))
                        Interlocked.Increment(ref ConflictsCount);
                }

                context.Transaction.AddAfterCommitNotification(new DocumentChange
                {
                    ChangeVector = str,
                    CollectionName = collectionName.Name,
                    Id = id,
                    Type = DocumentChangeTypes.Conflict,
                    IsSystemDocument = false,
                });
            }
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

                    long etag;
                    var collectionName = ResolveConflictAndAddTombstone(context, changeVector, conflicts, out etag);
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
                throw new ConcurrencyException($"Failed to resolve document conflict with change vector = {expectedChangeVector}, beacuse we have a newer change vector.");
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
                    DocumentFlags.None).Etag;
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

        public static ConflictStatus GetConflictStatusForDocument(DocumentsOperationContext context, string id, LazyStringValue remote, out string conflictingVector)
        {
            //tombstones also can be a conflict entry
            conflictingVector = null;
            var conflicts = context.DocumentDatabase.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, id);
            if (conflicts.Count > 0)
            {
                foreach (var existingConflict in conflicts)
                {
                    if (ChangeVectorUtils.GetConflictStatus(remote, existingConflict.ChangeVector) == ConflictStatus.Conflict)
                    {
                        conflictingVector = existingConflict.ChangeVector;
                        return ConflictStatus.Conflict;
                    }
                }
                // this document will resolve the conflicts when putted
                return ConflictStatus.Update;
            }

            var result = context.DocumentDatabase.DocumentsStorage.GetDocumentOrTombstone(context, id);
            string local;

            if (result.Document != null)
                local = result.Document.ChangeVector;
            else if (result.Tombstone != null)
                local = result.Tombstone.ChangeVector;
            else
                return ConflictStatus.Update; //document with 'id' doesn't exist locally, so just do PUT


            var status = ChangeVectorUtils.GetConflictStatus(remote, local);
            if (status == ConflictStatus.Conflict)
            {
                conflictingVector = local;
            }

            return status;
        }

        public long GetCountOfDocumentsConflicts(DocumentsOperationContext context)
        {
            if (ConflictsCount == 0)
                return 0;

            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
            return conflictsTable.GetTree(ConflictsSchema.Indexes[IdSlice]).State.NumberOfEntries;
        }
    }
}