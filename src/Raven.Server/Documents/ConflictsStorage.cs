using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Exceptions;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.Replication;
using Raven.Server.Extensions;
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
        private static readonly Slice KeySlice;
        private static readonly Slice KeyAndChangeVectorSlice;
        public static readonly Slice AllConflictedDocsEtagsSlice;
        private static readonly Slice ConflictedCollectionSlice;
        public static readonly Slice ConflictsSlice;

        public static readonly TableSchema ConflictsSchema = new TableSchema();

        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;
        private readonly Logger _logger;

        public long ConflictsCount;

        private enum ConflictsTable
        {
            LoweredKey = 0,
            Separator = 1,
            ChangeVector = 2,
            OriginalKey = 3,
            Data = 4,
            Etag = 5,
            Collection = 6,
            LastModified = 7,
        }

        static ConflictsStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "Key", ByteStringType.Immutable, out KeySlice);
            Slice.From(StorageEnvironment.LabelsContext, "KeyAndChangeVector", ByteStringType.Immutable, out KeyAndChangeVectorSlice);
            Slice.From(StorageEnvironment.LabelsContext, "AllConflictedDocsEtags", ByteStringType.Immutable, out AllConflictedDocsEtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "ConflictedCollection", ByteStringType.Immutable, out ConflictedCollectionSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Conflicts", ByteStringType.Immutable, out ConflictsSlice);

            /*
             The structure of conflicts table starts with the following fields:
             [ Conflicted Doc Id | Separator | Change Vector | ... the rest of fields ... ]
             PK of the conflicts table will be 'Change Vector' field, because when dealing with conflicts,
              the change vectors will always be different, hence the uniqueness of the key. (inserts/updates will not overwrite)

            Additional indice is set to have composite key of 'Conflicted Doc Id' and 'Change Vector' so we will be able to iterate
            on conflicts by conflicted doc id (using 'starts with')

            We need a separator in order to delete all conflicts all "users/1" without deleting "users/11" conflics.
             */

            ConflictsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)ConflictsTable.ChangeVector,
                Count = 1,
                IsGlobal = false,
                Name = KeySlice
            });
            // required to get conflicts by key
            ConflictsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)ConflictsTable.LoweredKey,
                Count = 3,
                IsGlobal = false,
                Name = KeyAndChangeVectorSlice
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
        public static ByteStringContext.InternalScope GetConflictsKeyPrefix(DocumentsOperationContext context, Slice lowerKey, out Slice prefixSlice)
        {
            return GetConflictsKeyPrefix(context, lowerKey.Content.Ptr, lowerKey.Size, out prefixSlice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext.InternalScope GetConflictsKeyPrefix(DocumentsOperationContext context, byte* lowerKey, int lowerKeySize, out Slice prefixSlice)
        {
            ByteString keyMem;
            var scope = context.Allocator.Allocate(lowerKeySize + 1, out keyMem);

            Memory.Copy(keyMem.Ptr, lowerKey, lowerKeySize);
            keyMem.Ptr[lowerKeySize] = SpecialChars.RecordSeperator;

            prefixSlice = new Slice(SliceOptions.Key, keyMem);
            return scope;
        }

        public IEnumerable<List<DocumentConflict>> GetAllConflictsBySameKey(DocumentsOperationContext context)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);

            var list = new List<DocumentConflict>();
            LazyStringValue lastKey = null;
            
            foreach (var tvr in table.SeekForwardFrom(ConflictsSchema.Indexes[KeyAndChangeVectorSlice], Slices.Empty, 0, false))
            {
                var conflict = TableValueToConflictDocument(context, ref tvr.Result.Reader);

                if (lastKey != null && lastKey.Equals(conflict.LoweredKey) == false)
                {
                    yield return list;
                    list = new List<DocumentConflict>();
                }

                list.Add(conflict);
                lastKey = conflict.LoweredKey;
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

        private static DocumentConflict TableValueToConflictDocument(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            var result = new DocumentConflict
            {
                StorageId = tvr.Id,
                LoweredKey = DocumentsStorage.TableValueToString(context, (int)ConflictsTable.LoweredKey, ref tvr),
                Key = DocumentsStorage.TableValueToKey(context, (int)ConflictsTable.OriginalKey, ref tvr),
                ChangeVector = DocumentsStorage.GetChangeVectorEntriesFromTableValueReader(ref tvr, (int)ConflictsTable.ChangeVector),
                Etag = DocumentsStorage.TableValueToEtag((int)ConflictsTable.Etag, ref tvr),
                Collection = DocumentsStorage.TableValueToString(context, (int)ConflictsTable.Collection, ref tvr)
            };


            int size;
            var read = tvr.Read((int)ConflictsTable.Data, out size);
            if (size > 0)
            {
                //otherwise this is a tombstone conflict and should be treated as such
                result.Doc = new BlittableJsonReaderObject(read, size, context);
                DocumentsStorage.DebugDisposeReaderAfterTransction(context.Transaction, result.Doc);
            }

            result.LastModified = new DateTime(*(long*)tvr.Read((int)ConflictsTable.LastModified, out size));

            return result;
        }

        public void ThrowOnDocumentConflict(DocumentsOperationContext context, Slice loweredKey)
        {
            //TODO: don't forget to refactor this method
            Slice prefixSlice;
            using (GetConflictsKeyPrefix(context, loweredKey, out prefixSlice))
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

                    ThrowDocumentConflictException(loweredKey.ToString(), largestEtag);
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
            foreach (var tvr in conflictsTable.SeekForwardFrom(ConflictsSchema.Indexes[KeyAndChangeVectorSlice], prefixSlice, 0, true))
            {
                var etag = DocumentsStorage.TableValueToEtag((int)ConflictsTable.Etag, ref tvr.Result.Reader);
                if (maxEtag < etag)
                    maxEtag = etag;
            }
            return maxEtag;
        }

        public void DeleteConflictsFor(DocumentsOperationContext context, string key)
        {
            if (ConflictsCount == 0)
                return;

            DocumentKeyWorker.GetSliceFromKey(context, key, out Slice lowerKey);
            DeleteConflictsFor(context, lowerKey);
        }

        public IReadOnlyList<ChangeVectorEntry[]> DeleteConflictsFor(DocumentsOperationContext context, Slice lowerKey)
        {
            var list = new List<ChangeVectorEntry[]>();
            if (ConflictsCount == 0)
                return list;

            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
            Slice prefixSlice;
            using (GetConflictsKeyPrefix(context, lowerKey, out prefixSlice))
            {
                conflictsTable.DeleteForwardFrom(ConflictsSchema.Indexes[KeyAndChangeVectorSlice], prefixSlice, true, long.MaxValue, before =>
                {
                    var etag = DocumentsStorage.TableValueToEtag((int)ConflictsTable.Etag, ref before.Reader);
                    _documentsStorage.EnsureLastEtagIsPersisted(context, etag);

                    var changeVector = DocumentsStorage.GetChangeVectorEntriesFromTableValueReader(ref before.Reader, (int)ConflictsTable.ChangeVector);
                    list.Add(changeVector);
                });
            }

            // once this value has been set, we can't set it to false
            // an older transaction may be running and seeing it is false it
            // will not detect a conflict. It is an optimization only that
            // we have to do, so we'll handle it.

            //Only register the event if we actually deleted any conflicts
            var listCount = list.Count;
            if (listCount > 0)
            {
                var tx = context.Transaction.InnerTransaction.LowLevelTransaction;
                tx.AfterCommitWhenNewReadTransactionsPrevented += () =>
                {
                    Interlocked.Add(ref ConflictsCount, -listCount);
                };
            }
            return list;
        }

        public void DeleteConflictsFor(DocumentsOperationContext context, ChangeVectorEntry[] changeVector)
        {
            if (ConflictsCount == 0)
                return;

            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);

            fixed (ChangeVectorEntry* pChangeVector = changeVector)
            {
                Slice changeVectorSlice;
                using (Slice.External(context.Allocator, (byte*)pChangeVector, sizeof(ChangeVectorEntry) * changeVector.Length, out changeVectorSlice))
                {
                    if (conflictsTable.DeleteByKey(changeVectorSlice))
                    {
                        var tx = context.Transaction.InnerTransaction.LowLevelTransaction;
                        tx.AfterCommitWhenNewReadTransactionsPrevented += () =>
                        {
                            Interlocked.Decrement(ref ConflictsCount);
                        };
                    }
                }
            }
        }

        public DocumentConflict GetConflictForChangeVector(
            DocumentsOperationContext context,
            string key,
            ChangeVectorEntry[] changeVector)
        {
            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);

            Slice keyPtr;
            Slice lowerKey;
            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out keyPtr);

            Slice prefixSlice;
            using (GetConflictsKeyPrefix(context, lowerKey, out prefixSlice))
            {
                foreach (var tvr in conflictsTable.SeekForwardFrom(ConflictsSchema.Indexes[KeyAndChangeVectorSlice], prefixSlice, 0, true))
                {
                    var currentChangeVector = DocumentsStorage.GetChangeVectorEntriesFromTableValueReader(ref tvr.Result.Reader, (int)ConflictsTable.ChangeVector);
                    if (currentChangeVector.SequenceEqual(changeVector))
                    {
                        int size;
                        var dataPtr = tvr.Result.Reader.Read((int)ConflictsTable.Data, out size);
                        var doc = (size == 0) ? null : new BlittableJsonReaderObject(dataPtr, size, context);
                        DocumentsStorage.DebugDisposeReaderAfterTransction(context.Transaction, doc);
                        return new DocumentConflict
                        {
                            ChangeVector = currentChangeVector,
                            Key = context.AllocateStringValue(key, tvr.Result.Reader.Read((int)ConflictsTable.OriginalKey, out size), size),
                            StorageId = tvr.Result.Reader.Id,
                            //size == 0 --> this is a tombstone conflict
                            Doc = doc
                        };
                    }
                }
            }
            return null;
        }

        public IReadOnlyList<DocumentConflict> GetConflictsFor(DocumentsOperationContext context, string key)
        {
            if (ConflictsCount == 0)
                return ImmutableAppendOnlyList<DocumentConflict>.Empty;

            Slice lowerKey, prefixSlice;
            using (DocumentKeyWorker.GetSliceFromKey(context, key, out lowerKey))
            using (GetConflictsKeyPrefix(context, lowerKey, out prefixSlice))
            {
                return GetConflictsFor(context, prefixSlice);
            }
        }

        public IReadOnlyList<DocumentConflict> GetConflictsFor(DocumentsOperationContext context, Slice prefixSlice)
        {
            var conflictsTable = context.Transaction.InnerTransaction.OpenTable(ConflictsSchema, ConflictsSlice);
            var items = new List<DocumentConflict>();
            foreach (var tvr in conflictsTable.SeekForwardFrom(ConflictsSchema.Indexes[KeyAndChangeVectorSlice], prefixSlice, 0, true))
            {
                var conflict = TableValueToConflictDocument(context, ref tvr.Result.Reader);
                items.Add(conflict);
            }
            return items;
        }

        public ChangeVectorEntry[] GetMergedConflictChangeVectorsAndDeleteConflicts(
           DocumentsOperationContext context,
           Slice lowerKey,
           long newEtag,
           ChangeVectorEntry[] existing = null)
        {
            if (ConflictsCount == 0)
                return MergeVectorsWithoutConflicts(newEtag, existing);

            var conflictChangeVectors = DeleteConflictsFor(context, lowerKey);
            if (conflictChangeVectors.Count == 0)
                return MergeVectorsWithoutConflicts(newEtag, existing);

            // need to merge the conflict change vectors
            var maxEtags = new Dictionary<Guid, long>
            {
                [_documentsStorage.Environment.DbId] = newEtag
            };

            foreach (var conflictChangeVector in conflictChangeVectors)
                foreach (var entry in conflictChangeVector)
                {
                    long etag;
                    if (maxEtags.TryGetValue(entry.DbId, out etag) == false ||
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

            return new[]
            {
                new ChangeVectorEntry
                {
                    Etag = newEtag,
                    DbId = _documentsStorage.Environment.DbId
                }
            };
        }

        public bool ShouldThrowConcurrencyExceptionOnConflict(DocumentsOperationContext context, Slice loweredKey, long? expectedEtag, out long? currentMaxConflictEtag)
        {
            if (expectedEtag.HasValue == false)
            {
                currentMaxConflictEtag = null;
                return false;
            }

            using (GetConflictsKeyPrefix(context, loweredKey.Content.Ptr, loweredKey.Size, out Slice prefixSlice))
            {
                currentMaxConflictEtag = GetConflictsMaxEtagFor(context, prefixSlice);

                return currentMaxConflictEtag != 0 && currentMaxConflictEtag != expectedEtag.Value;
            }
        }

        public void ThrowConcurrencyExceptionOnConflict(long? expectedEtag, long? currentMaxConflictEtag)
        {
            throw new ConcurrencyException(
                $"Tried to resolve document conflict with etag = {expectedEtag}, but the current max conflict etag is {currentMaxConflictEtag}. " +
                "This means that the conflict information with which you are trying to resolve the conflict is outdated. " +
                "Get conflict information and try resolving again.");
        }

        public ChangeVectorEntry[] MergeConflictChangeVectorIfNeededAndDeleteConflicts(ChangeVectorEntry[] documentChangeVector, DocumentsOperationContext context, string key, long newEtag)
        {
            ChangeVectorEntry[] mergedChangeVectorEntries = null;
            bool firstTime = true;
            foreach (var conflict in GetConflictsFor(context, key))
            {
                if (firstTime)
                {
                    mergedChangeVectorEntries = conflict.ChangeVector;
                    firstTime = false;
                    continue;
                }
                mergedChangeVectorEntries = ChangeVectorUtils.MergeVectors(mergedChangeVectorEntries, conflict.ChangeVector);
            }

            //We had conflicts need to delete them
            if (mergedChangeVectorEntries != null)
            {
                DeleteConflictsFor(context, key);
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

                return mergedChangeVectorEntries;
            }
            return documentChangeVector; // this covers the null && null case too
        }

        public void AddConflict(
            DocumentsOperationContext context,
            string key,
            long lastModifiedTicks,
            BlittableJsonReaderObject incomingDoc,
            ChangeVectorEntry[] incomingChangeVector,
            string incomingTombstoneCollection)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Adding conflict to {key} (Incoming change vector {incomingChangeVector.Format()})");

            var tx = context.Transaction.InnerTransaction;
            var conflictsTable = tx.OpenTable(ConflictsSchema, ConflictsSlice);

            CollectionName collectionName;

            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, key, out Slice lowerKey, out Slice keyPtr);
            // ReSharper disable once ArgumentsStyleLiteral
            var existing = _documentsStorage.GetDocumentOrTombstone(context, key, throwOnConflict: false);
            if (existing.Document != null)
            {
                var existingDoc = existing.Document;

                fixed (ChangeVectorEntry* pChangeVector = existingDoc.ChangeVector)
                {
                    var lazyCollectionName = CollectionName.GetLazyCollectionNameFrom(context, existingDoc.Data);

                    TableValueBuilder tbv;
                    using (conflictsTable.Allocate(out tbv))
                    {
                        tbv.Add(lowerKey);
                        tbv.Add(SpecialChars.RecordSeperator);
                        tbv.Add((byte*)pChangeVector, existingDoc.ChangeVector.Length * sizeof(ChangeVectorEntry));
                        tbv.Add(keyPtr);
                        tbv.Add(existingDoc.Data.BasePointer, existingDoc.Data.Size);
                        tbv.Add(Bits.SwapBytes(_documentsStorage.GenerateNextEtag()));
                        tbv.Add(lazyCollectionName.Buffer, lazyCollectionName.Size);
                        tbv.Add(existingDoc.LastModified.Ticks);

                        conflictsTable.Set(tbv);
                    }

                    Interlocked.Increment(ref ConflictsCount);
                    // we delete the data directly, without generating a tombstone, because we have a 
                    // conflict instead
                    _documentsStorage.EnsureLastEtagIsPersisted(context, existingDoc.Etag);
                    collectionName = _documentsStorage.ExtractCollectionName(context, existingDoc.Key, existingDoc.Data);

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
                    TableValueBuilder tableValueBuilder;
                    using (conflictsTable.Allocate(out tableValueBuilder))
                    {
                        tableValueBuilder.Add(lowerKey);
                        tableValueBuilder.Add(SpecialChars.RecordSeperator);
                        tableValueBuilder.Add((byte*)pChangeVector, existingTombstone.ChangeVector.Length * sizeof(ChangeVectorEntry));
                        tableValueBuilder.Add(keyPtr);
                        tableValueBuilder.Add(null, 0);
                        tableValueBuilder.Add(Bits.SwapBytes(_documentsStorage.GenerateNextEtag()));
                        tableValueBuilder.Add(existingTombstone.Collection.Buffer, existingTombstone.Collection.Size);
                        tableValueBuilder.Add(existingTombstone.LastModified.Ticks);
                        conflictsTable.Set(tableValueBuilder);
                    }
                    Interlocked.Increment(ref ConflictsCount);
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
                collectionName = _documentsStorage.ExtractCollectionName(context, key, incomingDoc);

                Slice prefixSlice;
                using (GetConflictsKeyPrefix(context, lowerKey, out prefixSlice))
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

            fixed (ChangeVectorEntry* pChangeVector = incomingChangeVector)
            {
                byte* doc = null;
                int docSize = 0;
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
                    tvb.Add(lowerKey);
                    tvb.Add(SpecialChars.RecordSeperator);
                    tvb.Add((byte*)pChangeVector, sizeof(ChangeVectorEntry) * incomingChangeVector.Length);
                    tvb.Add(keyPtr);
                    tvb.Add(doc, docSize);
                    tvb.Add(Bits.SwapBytes(etag));
                    tvb.Add(lazyCollectionName.Buffer, lazyCollectionName.Size);
                    tvb.Add(lastModifiedTicks);

                    Interlocked.Increment(ref ConflictsCount);
                    conflictsTable.Set(tvb);
                }
            }

            context.Transaction.AddAfterCommitNotification(new DocumentChange
            {
                Etag = etag,
                CollectionName = collectionName.Name,
                Key = key,
                Type = DocumentChangeTypes.Conflict,
                IsSystemDocument = false,
            });
        }

        public DocumentsStorage.DeleteOperationResult? DeleteConflicts(DocumentsOperationContext context,
            Slice loweredKey,
            long? expectedEtag,
            ChangeVectorEntry[] changeVector)
        {
            Slice prefixSlice;
            using (GetConflictsKeyPrefix(context, loweredKey, out prefixSlice))
            {
                var conflicts = GetConflictsFor(context, prefixSlice);
                if (conflicts.Count > 0)
                {
                    // We do have a conflict for our deletion candidate
                    // Since this document resolve the conflict we dont need to alter the change vector.
                    // This way we avoid another replication back to the source
                    if (ShouldThrowConcurrencyExceptionOnConflict(context, loweredKey, expectedEtag, out var currentMaxConflictEtag))
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

            using (DocumentKeyWorker.GetSliceFromKey(context, latestConflict.Key, out Slice lowerKey))
            {
                //note that CreateTombstone is also deleting conflicts
                etag = _documentsStorage.CreateTombstone(context,
                    lowerKey,
                    latestConflict.Etag,
                    collectionName,
                    mergedChangeVector,
                    latestConflict.LastModified.Ticks,
                    changeVector,
                    DocumentFlags.None);
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

        public static ConflictStatus GetConflictStatusForDocument(DocumentsOperationContext context, string key, ChangeVectorEntry[] remote, out ChangeVectorEntry[] conflictingVector)
        {
            //tombstones also can be a conflict entry
            conflictingVector = null;
            var conflicts = context.DocumentDatabase.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, key);
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

            var result = context.DocumentDatabase.DocumentsStorage.GetDocumentOrTombstone(context, key);
            ChangeVectorEntry[] local;

            if (result.Document != null)
                local = result.Document.ChangeVector;
            else if (result.Tombstone != null)
                local = result.Tombstone.ChangeVector;
            else
                return ConflictStatus.Update; //document with 'key' doesnt exist locally, so just do PUT


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

            int remoteEntriesTakenIntoAccount = 0;
            for (int index = 0; index < local.Length; index++)
            {
                if (remote.Length < index && remote[index].DbId == local[index].DbId)
                {
                    remoteHasLargerEntries |= remote[index].Etag > local[index].Etag;
                    localHasLargerEntries |= local[index].Etag > remote[index].Etag;
                    remoteEntriesTakenIntoAccount++;
                }
                else
                {
                    var updated = false;
                    for (var remoteIndex = 0; remoteIndex < remote.Length; remoteIndex++)
                    {
                        if (remote[remoteIndex].DbId == local[index].DbId)
                        {
                            remoteHasLargerEntries |= remote[remoteIndex].Etag > local[index].Etag;
                            localHasLargerEntries |= local[index].Etag > remote[remoteIndex].Etag;
                            remoteEntriesTakenIntoAccount++;
                            updated = true;
                        }
                    }

                    if (!updated)
                        localHasLargerEntries = true;
                }
            }
            remoteHasLargerEntries |= remoteEntriesTakenIntoAccount < remote.Length;

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
    }
}