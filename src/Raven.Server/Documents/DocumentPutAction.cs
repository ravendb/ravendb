using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Changes;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Raven.Client.Util;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Schemas.Documents;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents
{
    public unsafe class DocumentPutAction
    {
        private readonly DocumentsStorage _documentsStorage;
        private readonly DocumentDatabase _documentDatabase;
        private readonly IRecreationType[] _recreationTypes;

        public DocumentPutAction(DocumentsStorage documentsStorage, DocumentDatabase documentDatabase)
        {
            _documentsStorage = documentsStorage;
            _documentDatabase = documentDatabase;
            _recreationTypes = new IRecreationType[]
            {
                new RecreateAttachments(documentsStorage),
                new RecreateCounters(documentsStorage),
                new RecreateTimeSeries(documentsStorage),
            };
        }

        public void Recreate<T>(DocumentsOperationContext context, string docId) where T : IRecreationType
        {
            var doc = _documentsStorage.Get(context, docId);
            if (doc?.Data == null)
                return;

            var type = _recreationTypes.Single(t => t.GetType() == typeof(T));

            using (doc.Data)
            {
                PutDocument(context, docId, expectedChangeVector: null, document: doc.Data, nonPersistentFlags: type.ResolveConflictFlag);
            }
        }

        private readonly struct CompareClusterTransactionId
        {
            private readonly ServerStore _serverStore;
            private readonly DocumentPutAction _parent;

            public CompareClusterTransactionId(DocumentPutAction parent)
            {
                _serverStore = parent._documentDatabase.ServerStore;
                _parent = parent;
            }

            public void ValidateAtomicGuard(string id, NonPersistentDocumentFlags nonPersistentDocumentFlags, string changeVector)
            {
                if (nonPersistentDocumentFlags != NonPersistentDocumentFlags.None) // replication or engine running an operation, we can skip checking it 
                    return;

                if (_parent._documentDatabase.ClusterTransactionId == null)
                    return;

                long indexFromChangeVector = ChangeVectorUtils.GetEtagById(changeVector, _parent._documentDatabase.ClusterTransactionId);
                if (indexFromChangeVector == 0)
                    return;

                using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext clusterContext))
                using (clusterContext.OpenReadTransaction())
                {
                    var (indexFromCluster, val) = _parent._documentDatabase.CompareExchangeStorage.GetCompareExchangeValue(clusterContext, ClusterWideTransactionHelper.GetAtomicGuardKey(id));
                    if (indexFromChangeVector != indexFromCluster)
                    {
                        throw new ConcurrencyException(
                            $"Cannot PUT document '{id}' because its change vector's cluster transaction index is set to {indexFromChangeVector} " +
                            $"but the compare exchange guard ('{ClusterWideTransactionHelper.GetAtomicGuardKey(id)}') is {(val == null ? "missing" : $"set to {indexFromCluster}")}")
                        {
                            Id = id
                        };
                    }
                }
            }
        }

        public PutOperationResults PutDocument(DocumentsOperationContext context, string id,
            string expectedChangeVector,
            BlittableJsonReaderObject document,
            long? lastModifiedTicks = null,
            ChangeVector changeVector = null,
            string oldChangeVectorForClusterTransactionIndexCheck = null,
            DocumentFlags newFlags = DocumentFlags.None,
            NonPersistentDocumentFlags nonPersistentFlags = NonPersistentDocumentFlags.None)
        {
            if (context.Transaction == null)
            {
                ThrowRequiresTransaction();
                return default; // never hit
            }

            var documentDebugHash = 0UL;
            ValidateDocument(id, document, ref documentDebugHash);

            var newEtag = _documentsStorage.GenerateNextEtag();
            var modifiedTicks = _documentsStorage.GetOrCreateLastModifiedTicks(lastModifiedTicks);

            var compareClusterTransaction = new CompareClusterTransactionId(this);
            if (oldChangeVectorForClusterTransactionIndexCheck != null)
            {
                compareClusterTransaction.ValidateAtomicGuard(id, nonPersistentFlags, oldChangeVectorForClusterTransactionIndexCheck);
            }

            id = BuildDocumentId(id, newEtag, out bool knownNewId);
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out Slice lowerId, out Slice idPtr))
            {
                if (newFlags.HasFlag(DocumentFlags.FromResharding) == false)
                    _documentsStorage.ValidateId(context, lowerId, type: DocumentChangeTypes.Put, newFlags);

                var collectionName = _documentsStorage.ExtractCollectionName(context, document);
                _documentsStorage._forTestingPurposes?.OnBeforeOpenTableWhenPutDocumentWithSpecificId?.Invoke(id);
                
                var table = context.Transaction.InnerTransaction.OpenTable(_documentDatabase.GetDocsSchemaForCollection(collectionName, newFlags), collectionName.GetTableName(CollectionTableType.Documents));

                var oldValue = default(TableValueReader);
                if (knownNewId == false)
                {
                    // delete a tombstone if it exists, if it known that it is a new ID, no need, so we can skip it
                    DeleteTombstoneIfNeeded(context, collectionName, lowerId.Content.Ptr, lowerId.Size);

                    table.ReadByKey(lowerId, out oldValue);
                }

                BlittableJsonReaderObject oldDoc = null;
                ChangeVector oldChangeVector = null;
                if (oldValue.Pointer == null)
                {
                    // expectedChangeVector being null means we don't care, 
                    // and empty means that it must be new
                    if (string.IsNullOrEmpty(expectedChangeVector) == false)
                        ThrowConcurrentExceptionOnMissingDoc(id, expectedChangeVector);
                }
                else
                {
                    // expectedChangeVector  has special meaning here
                    // null - means, don't care, don't check
                    // "" / empty - means, must be new
                    // anything else - must match exactly

                    oldChangeVector = TableValueToChangeVector(context, (int)DocumentsTable.ChangeVector, ref oldValue);

                    if (expectedChangeVector != null && ChangeVector.CompareVersion(oldChangeVector, expectedChangeVector, context) != 0)
                        ThrowConcurrentException(id, expectedChangeVector, oldChangeVector);

                    if (oldChangeVectorForClusterTransactionIndexCheck == null)
                    {
                        compareClusterTransaction.ValidateAtomicGuard(id, nonPersistentFlags, oldChangeVector);
                    }

                    oldDoc = new BlittableJsonReaderObject(oldValue.Read((int)DocumentsTable.Data, out int oldSize), oldSize, context);
                    var oldCollectionName = _documentsStorage.ExtractCollectionName(context, oldDoc);
                    if (oldCollectionName != collectionName)
                        ThrowInvalidCollectionNameChange(id, oldCollectionName, collectionName);

                    var oldFlags = TableValueToFlags((int)DocumentsTable.Flags, ref oldValue);

                    newFlags = _documentsStorage.GetFlagsFromOldDocument(newFlags, oldFlags, nonPersistentFlags);
                }

                var result = _documentsStorage.BuildChangeVectorAndResolveConflicts(context, lowerId, newEtag, document, changeVector, expectedChangeVector, newFlags, oldChangeVector);

                nonPersistentFlags |= result.NonPersistentFlags;

                if (UpdateLastDatabaseChangeVector(context, result.ChangeVector, newFlags, nonPersistentFlags))
                    changeVector = result.ChangeVector;

                if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.Resolved))
                {
                    newFlags |= DocumentFlags.Resolved;
                }

                var revisionsCount = _documentDatabase.DocumentsStorage.RevisionsStorage.GetRevisionsCount(context, id);
                if (revisionsCount > 0)
                {
                    newFlags |= DocumentFlags.HasRevisions;
                }

                if (collectionName.IsHiLo == false && newFlags.Contain(DocumentFlags.Artificial) == false)
                {
                    Recreate(context, id, oldDoc, ref document, ref newFlags, nonPersistentFlags, ref documentDebugHash);

                    var shouldVersion = _documentDatabase.DocumentsStorage.RevisionsStorage.ShouldVersionDocument(
                        collectionName, nonPersistentFlags, oldDoc, document, context, id, lastModifiedTicks, ref newFlags, out var configuration);

                    if (shouldVersion)
                    {
                        if (_documentDatabase.DocumentsStorage.RevisionsStorage.ShouldVersionOldDocument(context, newFlags, oldDoc, oldChangeVector, collectionName))
                        {
                            var oldFlags = TableValueToFlags((int)DocumentsTable.Flags, ref oldValue);
                            var oldTicks = TableValueToDateTime((int)DocumentsTable.LastModified, ref oldValue);

                            _documentDatabase.DocumentsStorage.RevisionsStorage.Put(context, id, oldDoc, oldFlags | DocumentFlags.HasRevisions | DocumentFlags.FromOldDocumentRevision, NonPersistentDocumentFlags.None,
                                oldChangeVector, oldTicks.Ticks, configuration, collectionName);
                        }

                        newFlags |= DocumentFlags.HasRevisions;
                        _documentDatabase.DocumentsStorage.RevisionsStorage.Put(context, id, document, newFlags, nonPersistentFlags, changeVector, modifiedTicks,
                            configuration, collectionName);

                        var revisionsCountAfterDelete = _documentDatabase.DocumentsStorage.RevisionsStorage.GetRevisionsCount(context, id);
                        if (revisionsCountAfterDelete == 0)
                            newFlags = newFlags.Strip(DocumentFlags.HasRevisions);

                    }
                }

                FlagsProperlySet(newFlags, changeVector);
                using (Slice.From(context.Allocator, changeVector.AsString(), out var cv))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(lowerId);
                    tvb.Add(Bits.SwapBytes(newEtag));
                    tvb.Add(idPtr);
                    tvb.Add(document.BasePointer, document.Size);
                    tvb.Add(cv.Content.Ptr, cv.Size);
                    tvb.Add(modifiedTicks);
                    tvb.Add((int)newFlags);
                    tvb.Add(context.GetTransactionMarker());

                    if (oldValue.Pointer == null)
                    {
                        table.Insert(tvb);
                    }
                    else
                    {
                        table.Update(oldValue.Id, tvb);
                    }
                }

                if (collectionName.IsHiLo == false && document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
                {
                    var hasExpirationDate = metadata.TryGet(Constants.Documents.Metadata.Expires, out string expirationDate);
                    var hasRefreshDate = metadata.TryGet(Constants.Documents.Metadata.Refresh, out string refreshDate);
                    var hasArchiveAtDate= metadata.TryGet(Constants.Documents.Metadata.ArchiveAt, out string archiveAtDate);

                    if (hasExpirationDate)
                        _documentsStorage.ExpirationStorage.Put(context, lowerId, expirationDate);

                    if (hasRefreshDate)
                        _documentsStorage.RefreshStorage.Put(context, lowerId, refreshDate);
                    
                    if (hasArchiveAtDate)
                        _documentsStorage.DataArchivalStorage.Put(context, lowerId, archiveAtDate);
                }

                _documentDatabase.Metrics.Docs.PutsPerSec.MarkSingleThreaded(1);
                _documentDatabase.Metrics.Docs.BytesPutsPerSec.MarkSingleThreaded(document.Size);

                context.Transaction.AddAfterCommitNotification(new DocumentChange
                {
                    ChangeVector = changeVector.AsString(),
                    CollectionName = collectionName.Name,
                    Id = id,
                    Type = DocumentChangeTypes.Put,
                });

                ValidateDocumentHash(id, document, documentDebugHash);
                ValidateDocument(id, document, ref documentDebugHash);

                return new PutOperationResults
                {
                    Etag = newEtag,
                    Id = id,
                    Collection = collectionName,
                    ChangeVector = changeVector.AsString(),
                    Flags = newFlags,
                    LastModified = new DateTime(modifiedTicks, DateTimeKind.Utc)
                };
            }
        }

        [Conditional("DEBUG")]
        private static void ValidateDocument(string id, BlittableJsonReaderObject document, ref ulong documentDebugHash)
        {
            document.BlittableValidation();
            BlittableJsonReaderObject.AssertNoModifications(document, id, assertChildren: true);
            AssertMetadataWasFiltered(document);
            documentDebugHash = document.DebugHash;
        }

        [Conditional("DEBUG")]
        private static void ValidateDocumentHash(string id, BlittableJsonReaderObject document, ulong documentDebugHash)
        {
            if (document.DebugHash != documentDebugHash)
            {
                throw new InvalidDataException("The incoming document " + id + " has changed _during_ the put process, " +
                                               "this is likely because you are trying to save a document that is already stored and was moved");
            }
        }

        protected virtual void CalculateSuffixForIdentityPartsSeparator(string id, ref char* idSuffixPtr, ref int idSuffixLength, ref int idLength)
        {
        }

        protected virtual void WriteSuffixForIdentityPartsSeparator(ref char* valueWritePosition, char* idSuffixPtr, int idSuffixLength)
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string BuildDocumentId(string id, long newEtag, out bool knownNewId)
        {
            if (string.IsNullOrWhiteSpace(id))
            {
                knownNewId = true;
                id = Guid.NewGuid().ToString();
            }
            else
            {
                // We use if instead of switch so the JIT will better inline this method
                var lastChar = id[^1];
                if (lastChar == '|')
                {
                    ThrowInvalidDocumentId(id);
                }

                fixed (char* idPtr = id)
                {
                    var idSuffixPtr = idPtr;
                    var idLength = id.Length;
                    var idSuffixLength = 0;

                    if (lastChar == _documentDatabase.IdentityPartsSeparator)
                    {
                        CalculateSuffixForIdentityPartsSeparator(id, ref idSuffixPtr, ref idSuffixLength, ref idLength);

                        string nodeTag = _documentDatabase.ServerStore.NodeTag;

                        // PERF: we are creating an string and mutating it for performance reasons.
                        //       while nasty this shouldn't have any side effects because value didn't
                        //       escape yet the function, so while not pretty it works (and it's safe).      
                        //       
                        int valueLength = idLength + 1 + 19 + nodeTag.Length + idSuffixLength;
                        string value = new('0', valueLength);
                        fixed (char* valuePtr = value)
                        {
                            char* valueWritePosition = valuePtr + value.Length;

                            WriteSuffixForIdentityPartsSeparator(ref valueWritePosition, idSuffixPtr, idSuffixLength);

                            valueWritePosition -= nodeTag.Length;
                            for (int j = 0; j < nodeTag.Length; j++)
                                valueWritePosition[j] = nodeTag[j];

                            int i;
                            for (i = 0; i < idLength; i++)
                                valuePtr[i] = id[i];

                            i += 19;
                            valuePtr[i] = '-';

                            Format.Backwards.WriteNumber(valuePtr + i - 1, (ulong)newEtag);
                        }

                        id = value;

                        knownNewId = true;
                    }
                    else
                    {
                        knownNewId = false;
                    }
                }
            }

            // Intentionally have just one return statement here for better inlining
            return id;
        }

        [DoesNotReturn]
        private static void ThrowInvalidDocumentId(string id)
        {
            throw new NotSupportedException("Document ids cannot end with '|', but was called with " + id +
                                            ". Identities are only generated for external requests, not calls to PutDocument and such.");
        }

        private void Recreate(DocumentsOperationContext context, string id, BlittableJsonReaderObject oldDoc,
            ref BlittableJsonReaderObject document, ref DocumentFlags flags, NonPersistentDocumentFlags nonPersistentFlags, ref ulong documentDebugHash)
        {
            for (int i = 0; i < _recreationTypes.Length; i++)
            {
                var type = _recreationTypes[i];
                if (RecreateIfNeeded(context, id, oldDoc, document, ref flags, nonPersistentFlags, type))
                {
                    ValidateDocumentHash(id, document, documentDebugHash);

                    document = context.ReadObject(document, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                    ValidateDocument(id, document, ref documentDebugHash);
#if DEBUG
                    type.Assert(id, document, flags);
#endif
                }
            }
        }


        private bool RecreateIfNeeded(DocumentsOperationContext context, string docId, BlittableJsonReaderObject oldDoc,
            BlittableJsonReaderObject document, ref DocumentFlags flags, NonPersistentDocumentFlags nonPersistentFlags, IRecreationType type)
        {
            BlittableJsonReaderObject metadata;
            BlittableJsonReaderArray current, old = null;

            if (nonPersistentFlags.Contain(type.ResolveConflictFlag))
            {
                TryGetMetadata(document, type, out metadata, out current);
                return RecreatePreserveCasing(current, ref flags);
            }

            if (flags.Contain(type.HasFlag) == false ||
                nonPersistentFlags.Contain(type.ByUpdateFlag) ||
                nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication))
                return false;

            if (oldDoc == null ||
                oldDoc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject oldMetadata) == false ||
                oldMetadata.TryGet(type.MetadataProperty, out old) == false)
                return false;

            // Make sure the user did not changed the value of @attachments in the @metadata
            // In most cases it won't be changed so we can use this value 
            // instead of recreating the document's blittable from scratch

            if (TryGetMetadata(document, type, out metadata, out current) == false ||
                current.Equals(old) == false)
            {
                return RecreatePreserveCasing(current, ref flags);
            }

            return false;

            bool RecreatePreserveCasing(BlittableJsonReaderArray currentMetadata, ref DocumentFlags documentFlags)
            {
                if ((type is RecreateTimeSeries || type is RecreateCounters) &&
                    currentMetadata == null && old != null)
                {
                    // use the '@counters'/'@timeseries' from old document's metadata

                    if (metadata == null)
                    {
                        document.Modifications = new DynamicJsonValue(document)
                        {
                            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                            {
                                [type.MetadataProperty] = old
                            }
                        };
                    }
                    else
                    {
                        metadata.Modifications = new DynamicJsonValue(metadata)
                        {
                            [type.MetadataProperty] = old
                        };
                        document.Modifications = new DynamicJsonValue(document)
                        {
                            [Constants.Documents.Metadata.Key] = metadata
                        };
                    }

                    return true;
                }

                var values = type.GetMetadata(context, docId);

                if (values.Count == 0)
                {
                    if (metadata != null && metadata.TryGetMember(type.MetadataProperty, out _))
                    {
                        metadata.Modifications = new DynamicJsonValue(metadata);
                        metadata.Modifications.Remove(type.MetadataProperty);
                        document.Modifications = new DynamicJsonValue(document)
                        {
                            [Constants.Documents.Metadata.Key] = metadata
                        };
                    }

                    documentFlags &= ~type.HasFlag;
                    return true;
                }

                documentFlags |= type.HasFlag;

                if (metadata == null)
                {
                    document.Modifications = new DynamicJsonValue(document)
                    {
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [type.MetadataProperty] = values
                        }
                    };
                }
                else
                {
                    metadata.Modifications = new DynamicJsonValue(metadata)
                    {
                        [type.MetadataProperty] = values
                    };
                    document.Modifications = new DynamicJsonValue(document)
                    {
                        [Constants.Documents.Metadata.Key] = metadata
                    };
                }

                return true;
            }
        }

        private static bool TryGetMetadata(BlittableJsonReaderObject document, IRecreationType type, out BlittableJsonReaderObject metadata, out BlittableJsonReaderArray current)
        {
            current = null;
            metadata = null;

            return document.TryGet(Constants.Documents.Metadata.Key, out metadata) &&
                   metadata.TryGet(type.MetadataProperty, out current);
        }

        public interface IRecreationType
        {
            string MetadataProperty { get; }

            DynamicJsonArray GetMetadata(DocumentsOperationContext context, string id);

            DocumentFlags HasFlag { get; }

            NonPersistentDocumentFlags ResolveConflictFlag { get; }
            NonPersistentDocumentFlags ByUpdateFlag { get; }

            Action<string, BlittableJsonReaderObject, DocumentFlags> Assert { get; }
        }

        private sealed class RecreateAttachments : IRecreationType
        {
            private readonly DocumentsStorage _storage;

            public RecreateAttachments(DocumentsStorage storage)
            {
                _storage = storage;
            }

            public string MetadataProperty => Constants.Documents.Metadata.Attachments;

            public DynamicJsonArray GetMetadata(DocumentsOperationContext context, string id)
            {
                return _storage.AttachmentsStorage.GetAttachmentsMetadataForDocument(context, id);
            }

            public DocumentFlags HasFlag => DocumentFlags.HasAttachments;

            public NonPersistentDocumentFlags ResolveConflictFlag => NonPersistentDocumentFlags.ResolveAttachmentsConflict;

            public NonPersistentDocumentFlags ByUpdateFlag => NonPersistentDocumentFlags.ByAttachmentUpdate;

            public Action<string, BlittableJsonReaderObject, DocumentFlags> Assert => (id, o, flags) =>
                _storage.AssertMetadataKey(id, o, flags, DocumentFlags.HasAttachments, Constants.Documents.Metadata.Attachments);
        }

        public sealed class RecreateCounters : IRecreationType
        {
            private readonly DocumentsStorage _storage;

            public RecreateCounters(DocumentsStorage storage)
            {
                _storage = storage;
            }

            public string MetadataProperty => Constants.Documents.Metadata.Counters;

            public DynamicJsonArray GetMetadata(DocumentsOperationContext context, string id)
            {
                return _storage.CountersStorage.GetCountersForDocumentList(context, id);
            }

            public DocumentFlags HasFlag => DocumentFlags.HasCounters;

            public NonPersistentDocumentFlags ResolveConflictFlag => NonPersistentDocumentFlags.ResolveCountersConflict;

            public NonPersistentDocumentFlags ByUpdateFlag => NonPersistentDocumentFlags.ByCountersUpdate;

            public Action<string, BlittableJsonReaderObject, DocumentFlags> Assert => (id, o, flags) =>
                _storage.AssertMetadataKey(id, o, flags, DocumentFlags.HasCounters, Constants.Documents.Metadata.Counters);
        }

        private sealed class RecreateTimeSeries : IRecreationType
        {
            private readonly DocumentsStorage _storage;

            public RecreateTimeSeries(DocumentsStorage storage)
            {
                _storage = storage;
            }

            public string MetadataProperty => Constants.Documents.Metadata.TimeSeries;

            public DynamicJsonArray GetMetadata(DocumentsOperationContext context, string id)
            {
                return _storage.TimeSeriesStorage.GetTimeSeriesNamesForDocument(context, id);
            }

            public DocumentFlags HasFlag => DocumentFlags.HasTimeSeries;

            public NonPersistentDocumentFlags ResolveConflictFlag => NonPersistentDocumentFlags.ResolveTimeSeriesConflict;

            public NonPersistentDocumentFlags ByUpdateFlag => NonPersistentDocumentFlags.ByTimeSeriesUpdate;

            public Action<string, BlittableJsonReaderObject, DocumentFlags> Assert => (id, o, flags) =>
                _storage.AssertMetadataKey(id, o, flags, DocumentFlags.HasTimeSeries, Constants.Documents.Metadata.TimeSeries);
        }

        [DoesNotReturn]
        public static void ThrowRequiresTransaction([CallerMemberName] string caller = null)
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentException("Context must be set with a valid transaction before calling " + caller, "context");
        }

        [DoesNotReturn]
        private static void ThrowConcurrentExceptionOnMissingDoc(string id, string expectedChangeVector)
        {
            throw new ConcurrencyException(
                $"Document {id} does not exist, but Put was called with change vector: {expectedChangeVector}. Optimistic concurrency violation, transaction will be aborted.")
            {
                Id = id,
                ExpectedChangeVector = expectedChangeVector
            };
        }

        [DoesNotReturn]
        private static void ThrowInvalidCollectionNameChange(string id, CollectionName oldCollectionName, CollectionName collectionName)
        {
            DocumentCollectionMismatchException.ThrowFor(id, oldCollectionName.Name, collectionName.Name);
        }

        [DoesNotReturn]
        private static void ThrowConcurrentException(string id, string expectedChangeVector, string oldChangeVector)
        {
            throw new ConcurrencyException(
                $"Document {id} has change vector {oldChangeVector}, but Put was called with {(expectedChangeVector.Length == 0 ? "expecting new document" : "change vector " + expectedChangeVector)}. Optimistic concurrency violation, transaction will be aborted.")
            {
                Id = id,
                ActualChangeVector = oldChangeVector,
                ExpectedChangeVector = expectedChangeVector
            };
        }

        private void DeleteTombstoneIfNeeded(DocumentsOperationContext context, CollectionName collectionName, byte* lowerId, int lowerSize)
        {
            var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(_documentsStorage.TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));
            if (tombstoneTable.NumberOfEntries == 0)
                return;

            using (Slice.External(context.Allocator, lowerId, lowerSize, out Slice id))
            {
                DeleteTombstone(tombstoneTable, id);
            }
        }

        public void DeleteTombstoneIfNeeded(DocumentsOperationContext context, CollectionName collectionName, Slice id)
        {
            var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(_documentsStorage.TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));
            if (tombstoneTable.NumberOfEntries == 0)
                return;

            DeleteTombstone(tombstoneTable, id);
        }

        private static void DeleteTombstone(Table tombstoneTable, Slice id)
        {
            foreach (var (tombstoneKey, tvh) in tombstoneTable.SeekByPrimaryKeyPrefix(id, Slices.Empty, 0))
            {
                if (IsTombstoneOfId(tombstoneKey, id) == false)
                    return;

                if (tombstoneTable.IsOwned(tvh.Reader.Id))
                {
                    tombstoneTable.Delete(tvh.Reader.Id);
                    return;
                }
            }
        }

        [Conditional("DEBUG")]
        public static void AssertMetadataWasFiltered(BlittableJsonReaderObject data)
        {
            if (data == null)
                return;

            var originalNoCacheValue = data.NoCache;

            data.NoCache = true;

            try
            {
                if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                    return;

                var names = metadata.GetPropertyNames();
                if (names.Contains(Constants.Documents.Metadata.Id, StringComparer.OrdinalIgnoreCase) ||
                    names.Contains(Constants.Documents.Metadata.LastModified, StringComparer.OrdinalIgnoreCase) ||
                    names.Contains(Constants.Documents.Metadata.IndexScore, StringComparer.OrdinalIgnoreCase) ||
                    names.Contains(Constants.Documents.Metadata.ChangeVector, StringComparer.OrdinalIgnoreCase) ||
                    names.Contains(Constants.Documents.Metadata.Flags, StringComparer.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException("Document's metadata should filter properties on before put to storage." + Environment.NewLine + data);
                }
            }
            finally
            {
                data.NoCache = originalNoCacheValue;
            }
        }
    }
}
