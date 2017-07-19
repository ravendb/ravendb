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
                LowerId = DocumentsStorage.TableValueToString(context, (int)ConflictsTable.LowerId, ref tvr),
                Id = DocumentsStorage.TableValueToId(context, (int)ConflictsTable.Id, ref tvr),
                ChangeVector = DocumentsStorage.TableValueToChangeVector(ref tvr, (int)ConflictsTable.ChangeVector),
                Etag = DocumentsStorage.TableValueToEtag((int)ConflictsTable.Etag, ref tvr),
                Collection = DocumentsStorage.TableValueToString(context, (int)ConflictsTable.Collection, ref tvr),
                LastModified = DocumentsStorage.TableValueToDateTime((int)ConflictsTable.LastModified, ref tvr),
                Flags = DocumentsStorage.TableValueToFlags((int)ConflictsTable.Flags, ref tvr),
            };

            var read = tvr.Read((int)ConflictsTable.Data, out int size);
            if (size > 0)
            {
                //otherwise this is a tombstone conflict and should be treated as such
                result.Doc = new BlittableJsonReaderObject(read, size, context);
                DocumentsStorage.DebugDisposeReaderAfterTransaction(context.Transaction, result.Doc);
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
                var etag = DocumentsStorage.TableValueToEtag((int)ConflictsTable.Etag, ref tvr.Result.Reader);
                if (maxEtag < etag)
                    maxEtag = etag;
            }
            return maxEtag;
        }

        public (IReadOnlyList<ChangeVectorEntry[]> ChangeVectors, NonPersistentDocumentFlags NonPersistentFlags) DeleteConflictsFor(
            DocumentsOperationContext context, string id, BlittableJsonReaderObject document)
        {
            if (ConflictsCount == 0)
                return (null, NonPersistentDocumentFlags.None);

            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
                return DeleteConflictsFor(context, lowerId, document);
        }

        public (IReadOnlyList<ChangeVectorEntry[]> ChangeVectors, NonPersistentDocumentFlags NonPersistentFlags) DeleteConflictsFor(
            DocumentsOperationContext context, Slice lowerId, BlittableJsonReaderObject document)
        {
            if (ConflictsCount == 0)
                return (null, NonPersistentDocumentFlags.None);

            var changeVectors = new List<ChangeVectorEntry[]>();
            var nonPersistentFlags = NonPersistentDocumentFlags.None;
            ChangeVectorEntry[] deleteAttachmentChangeVector = null;
            using (GetConflictsIdPrefix(context, lowerId, out Slice prefixSlice))
            {
                var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
                var deleteCount = conflictsTable.DeleteForwardFrom(ConflictsSchema.Indexes[IdAndChangeVectorSlice], prefixSlice, true, long.MaxValue, conflictDocument =>
                {
                    var etag = DocumentsStorage.TableValueToEtag((int)ConflictsTable.Etag, ref conflictDocument.Reader);
                    _documentsStorage.EnsureLastEtagIsPersisted(context, etag);

                    var conflictChangeVector = DocumentsStorage.TableValueToChangeVector(ref conflictDocument.Reader, (int)ConflictsTable.ChangeVector);
                    changeVectors.Add(conflictChangeVector);

                    var flags = DocumentsStorage.TableValueToFlags((int)ConflictsTable.Flags, ref conflictDocument.Reader);
                    if ((flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments)
                        return;

                    if (deleteAttachmentChangeVector == null)
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
            BlittableJsonReaderObject document, Table.TableValueHolder before, ChangeVectorEntry[] changeVector)
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

        public void DeleteConflictsFor(DocumentsOperationContext context, ChangeVectorEntry[] changeVector)
        {
            if (ConflictsCount == 0)
                return;

            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);

            fixed (ChangeVectorEntry* pChangeVector = changeVector)
            {
                using (Slice.External(context.Allocator, (byte*)pChangeVector, sizeof(ChangeVectorEntry) * changeVector.Length, out Slice changeVectorSlice))
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
        }

        public DocumentConflict GetConflictForChangeVector(
            DocumentsOperationContext context,
            string id,
            ChangeVectorEntry[] changeVector)
        {
            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);

            using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
            using (GetConflictsIdPrefix(context, lowerId, out Slice prefixSlice))
            {
                foreach (var tvr in conflictsTable.SeekForwardFrom(ConflictsSchema.Indexes[IdAndChangeVectorSlice], prefixSlice, 0, true))
                {
                    var currentChangeVector = DocumentsStorage.TableValueToChangeVector(ref tvr.Result.Reader, (int)ConflictsTable.ChangeVector);
                    if (currentChangeVector.SequenceEqual(changeVector))
                    {
                        var dataPtr = tvr.Result.Reader.Read((int)ConflictsTable.Data, out int size);
                        var doc = size == 0 ? null : new BlittableJsonReaderObject(dataPtr, size, context);
                        DocumentsStorage.DebugDisposeReaderAfterTransaction(context.Transaction, doc);
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

        public ChangeVectorEntry[] GetMergedConflictChangeVectorsAndDeleteConflicts(
           DocumentsOperationContext context,
           Slice lowerId,
           long newEtag,
           ChangeVectorEntry[] existing = null)
        {
            if (ConflictsCount == 0)
                return MergeVectorsWithoutConflicts(newEtag, existing);

            var conflictChangeVectors = DeleteConflictsFor(context, lowerId, null).ChangeVectors;
            if (conflictChangeVectors == null ||
                conflictChangeVectors.Count == 0)
                return MergeVectorsWithoutConflicts(newEtag, existing);

            // need to merge the conflict change vectors
            var maxEtags = new Dictionary<Guid, long>
            {
                [_documentsStorage.Environment.DbId] = newEtag
            };

            foreach (var conflictChangeVector in conflictChangeVectors)
            foreach (var entry in conflictChangeVector)
            {
                if (maxEtags.TryGetValue(entry.DbId, out long etag) == false ||
                    etag < entry.Etag)
                {
                    maxEtags[entry.DbId] = entry.Etag;
                }
            }

            var changeVector = new ChangeVectorEntry[maxEtags.Count];

            var index = 0;
            foreach (var maxEtag in maxEtags)
            {
                changeVector[index].DbId = maxEtag.Key;
                changeVector[index].Etag = maxEtag.Value;
                index++;
            }
            return changeVector;
        }

        private ChangeVectorEntry[] MergeVectorsWithoutConflicts(long newEtag, ChangeVectorEntry[] existing)
        {
            if (existing != null)
                return ChangeVectorUtils.UpdateChangeVectorWithNewEtag(_documentsStorage.Environment.DbId, newEtag, existing);

            // TODO: Should we use here? return _documentsStorage.GetNewChangeVector(context, newEtag);
            return new[]
            {
                new ChangeVectorEntry
                {
                    Etag = newEtag,
                    DbId = _documentsStorage.Environment.DbId
                }
            };
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

        public (ChangeVectorEntry[] ChangeVector, NonPersistentDocumentFlags NonPersistentFlags) MergeConflictChangeVectorIfNeededAndDeleteConflicts(
            ChangeVectorEntry[] documentChangeVector, DocumentsOperationContext context, string id, long newEtag, BlittableJsonReaderObject document)
        {
            var result = DeleteConflictsFor(context, id, document);
            if (result.ChangeVectors == null ||
                result.ChangeVectors.Count == 0)
            {
                return (documentChangeVector, result.NonPersistentFlags);
            }

            ChangeVectorEntry[] mergedChangeVectorEntries = null;
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
            if (documentChangeVector != null)
                mergedChangeVectorEntries = ChangeVectorUtils.MergeVectors(mergedChangeVectorEntries, documentChangeVector);

            mergedChangeVectorEntries = ChangeVectorUtils.MergeVectors(mergedChangeVectorEntries, new[]
            {
                new ChangeVectorEntry
                {
                    DbId = _documentDatabase.DbId,
                    Etag = newEtag
                }
            });

            return (mergedChangeVectorEntries, result.NonPersistentFlags);
        }

        public void AddConflict(
            DocumentsOperationContext context, 
            string id, 
            long lastModifiedTicks, 
            BlittableJsonReaderObject incomingDoc, 
            ChangeVectorEntry[] incomingChangeVector, 
            string incomingTombstoneCollection, 
            DocumentFlags flags)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Adding conflict to {id} (Incoming change vector {incomingChangeVector.Format()})");

            var tx = context.Transaction.InnerTransaction;
            var conflictsTable = tx.OpenTable(ConflictsSchema, ConflictsSlice);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out Slice lowerId, out Slice idPtr))
            {
                CollectionName collectionName;

                // ReSharper disable once ArgumentsStyleLiteral
                var existing = _documentsStorage.GetDocumentOrTombstone(context, id, throwOnConflict: false);
                if (existing.Document != null)
                {
                    var existingDoc = existing.Document;

                    fixed (ChangeVectorEntry* pChangeVector = existingDoc.ChangeVector)
                    {
                        var lazyCollectionName = CollectionName.GetLazyCollectionNameFrom(context, existingDoc.Data);

                        using (conflictsTable.Allocate(out TableValueBuilder tvb))
                        {
                            tvb.Add(lowerId);
                            tvb.Add(SpecialChars.RecordSeparator);
                            tvb.Add((byte*)pChangeVector, existingDoc.ChangeVector.Length * sizeof(ChangeVectorEntry));
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
                        var table = tx.OpenTable(DocumentsStorage.DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));
                        table.Delete(existingDoc.StorageId);
                    }
                }
                else if (existing.Tombstone != null)
                {
                    var existingTombstone = existing.Tombstone;

                    fixed (ChangeVectorEntry* pChangeVector = existingTombstone.ChangeVector)
                    {
                        using (conflictsTable.Allocate(out TableValueBuilder tvb))
                        {
                            tvb.Add(lowerId);
                            tvb.Add(SpecialChars.RecordSeparator);
                            tvb.Add((byte*)pChangeVector, existingTombstone.ChangeVector.Length * sizeof(ChangeVectorEntry));
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

                        var table = tx.OpenTable(DocumentsStorage.TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));
                        table.Delete(existingTombstone.StorageId);
                    }
                }
                else // has existing conflicts
                {
                    collectionName = _documentsStorage.ExtractCollectionName(context, id, incomingDoc);

                    using (GetConflictsIdPrefix(context, lowerId, out Slice prefixSlice))
                    {
                        var conflicts = GetConflictsFor(context, prefixSlice);
                        foreach (var conflict in conflicts)
                        {
                            var conflictStatus = GetConflictStatus(incomingChangeVector, conflict.ChangeVector);
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
                context.UpdateLastDatabaseChangeVector(_documentDatabase.DbId, etag);
                fixed (ChangeVectorEntry* pChangeVector = incomingChangeVector)
                {
                    byte* doc = null;
                    var docSize = 0;
                    LazyStringValue lazyCollectionName;
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
                    using (conflictsTable.Allocate(out TableValueBuilder tvb))
                    {
                        tvb.Add(lowerId);
                        tvb.Add(SpecialChars.RecordSeparator);
                        tvb.Add((byte*)pChangeVector, sizeof(ChangeVectorEntry) * incomingChangeVector.Length);
                        tvb.Add(idPtr);
                        tvb.Add(doc, docSize);
                        tvb.Add(Bits.SwapBytes(etag));
                        tvb.Add(lazyCollectionName.Buffer, lazyCollectionName.Size);
                        tvb.Add(lastModifiedTicks);
                        tvb.Add((int)flags);
                        if (conflictsTable.Set(tvb))
                            Interlocked.Increment(ref ConflictsCount);
                    }
                }

                context.Transaction.AddAfterCommitNotification(new DocumentChange
                {
                    Etag = etag,
                    CollectionName = collectionName.Name,
                    Id = id,
                    Type = DocumentChangeTypes.Conflict,
                    IsSystemDocument = false,
                });
            }
        }

        public DocumentsStorage.DeleteOperationResult? DeleteConflicts(DocumentsOperationContext context,
            Slice lowerId,
            long? expectedEtag,
            ChangeVectorEntry[] changeVector)
        {
            using (GetConflictsIdPrefix(context, lowerId, out Slice prefixSlice))
            {
                var conflicts = GetConflictsFor(context, prefixSlice);
                if (conflicts.Count > 0)
                {
                    // We do have a conflict for our deletion candidate
                    // Since this document resolve the conflict we don't need to alter the change vector.
                    // This way we avoid another replication back to the source
                    if (ShouldThrowConcurrencyExceptionOnConflict(context, lowerId, expectedEtag, out var currentMaxConflictEtag))
                    {
                        ThrowConcurrencyExceptionOnConflict(expectedEtag, currentMaxConflictEtag);
                    }

                    long etag;
                    var collectionName = ResolveConflictAndAddTombstone(context, changeVector, conflicts, out etag);
                    return new DocumentsStorage.DeleteOperationResult
                    {
                        Collection = collectionName,
                        Etag = etag
                    };
                }
            }
            return null;
        }

        private CollectionName ResolveConflictAndAddTombstone(DocumentsOperationContext context,
            ChangeVectorEntry[] changeVector, IReadOnlyList<DocumentConflict> conflicts, out long etag)
        {
            var indexOfLargestEtag = FindIndexOfLargestEtagAndMergeChangeVectors(conflicts, out ChangeVectorEntry[] mergedChangeVector);
            var latestConflict = conflicts[indexOfLargestEtag];
            var collectionName = new CollectionName(latestConflict.Collection);

            using (DocumentIdWorker.GetSliceFromId(context, latestConflict.Id, out Slice lowerId))
            {
                //note that CreateTombstone is also deleting conflicts
                etag = _documentsStorage.CreateTombstone(context,
                    lowerId,
                    latestConflict.Etag,
                    collectionName,
                    mergedChangeVector,
                    latestConflict.LastModified.Ticks,
                    changeVector,
                    DocumentFlags.None).Etag;
            }

            return collectionName;
        }

        private static int FindIndexOfLargestEtagAndMergeChangeVectors(IReadOnlyList<DocumentConflict> conflicts, out ChangeVectorEntry[] mergedChangeVectorEntries)
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

        public static ConflictStatus GetConflictStatusForDocument(DocumentsOperationContext context, string id, ChangeVectorEntry[] remote, out ChangeVectorEntry[] conflictingVector)
        {
            //tombstones also can be a conflict entry
            conflictingVector = null;
            var conflicts = context.DocumentDatabase.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, id);
            if (conflicts.Count > 0)
            {
                foreach (var existingConflict in conflicts)
                {
                    if (GetConflictStatus(remote, existingConflict.ChangeVector) == ConflictStatus.Conflict)
                    {
                        conflictingVector = existingConflict.ChangeVector;
                        return ConflictStatus.Conflict;
                    }
                }
                // this document will resolve the conflicts when putted
                return ConflictStatus.Update;
            }

            var result = context.DocumentDatabase.DocumentsStorage.GetDocumentOrTombstone(context, id);
            ChangeVectorEntry[] local;

            if (result.Document != null)
                local = result.Document.ChangeVector;
            else if (result.Tombstone != null)
                local = result.Tombstone.ChangeVector;
            else
                return ConflictStatus.Update; //document with 'id' doesn't exist locally, so just do PUT


            var status = GetConflictStatus(remote, local);
            if (status == ConflictStatus.Conflict)
            {
                conflictingVector = local;
            }

            return status;
        }

        public static ConflictStatus GetConflictStatus(ChangeVectorEntry[] remote, ChangeVectorEntry[] local)
        {
            if (local == null)
                return ConflictStatus.Update;

            //any missing entries from a change vector are assumed to have zero value
            var remoteHasLargerEntries = local.Length < remote.Length;
            var localHasLargerEntries = remote.Length < local.Length;

            Array.Sort(remote); // todo: check if we need this
            Array.Sort(local); // todo: check if we need this

            var localIndex = 0;
            var remoteIndex = 0;

            while (localIndex < local.Length && remoteIndex < remote.Length)
            {
                var compareResult = remote[remoteIndex].DbId.CompareTo(local[localIndex].DbId);
                if (compareResult == 0)
                {
                    remoteHasLargerEntries |= remote[remoteIndex].Etag > local[localIndex].Etag;
                    localHasLargerEntries |= local[localIndex].Etag > remote[remoteIndex].Etag;
                    remoteIndex++;
                    localIndex++;
                }
                else if (compareResult > 0)
                {
                    localIndex++;
                    localHasLargerEntries = true;
                }
                else
                {
                    remoteIndex++;
                    remoteHasLargerEntries = true;
                }

                if (localHasLargerEntries && remoteHasLargerEntries)
                    break;
            }

            if (remoteIndex < remote.Length)
            {
                remoteHasLargerEntries = true;
            }

            if (localIndex < local.Length)
            {
                localHasLargerEntries = true;
            }

            if (remoteHasLargerEntries && localHasLargerEntries)
                return ConflictStatus.Conflict;

            if (remoteHasLargerEntries == false && localHasLargerEntries == false)
                return ConflictStatus.AlreadyMerged; // change vectors identical

            return remoteHasLargerEntries ? ConflictStatus.Update : ConflictStatus.AlreadyMerged;
        }

        public enum ConflictStatus
        {
            Update,
            Conflict,
            AlreadyMerged
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