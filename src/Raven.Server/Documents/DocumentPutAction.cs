using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;
using System.Linq;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Exceptions;
using Sparrow;
using static Raven.Server.Documents.DocumentsStorage;

namespace Raven.Server.Documents
{
    public unsafe class DocumentPutAction
    {
        private readonly DocumentsStorage _documentsStorage;
        private readonly DocumentDatabase _documentDatabase;

        public DocumentPutAction(DocumentsStorage documentsStorage, DocumentDatabase documentDatabase)
        {
            _documentsStorage = documentsStorage;
            _documentDatabase = documentDatabase;
        }

        public PutOperationResults PutDocument(DocumentsOperationContext context, string id,
            string expectedChangeVector,
            BlittableJsonReaderObject document,
            long? lastModifiedTicks = null,
            string changeVector = null,
            DocumentFlags flags = DocumentFlags.None,
            NonPersistentDocumentFlags nonPersistentFlags = NonPersistentDocumentFlags.None)
        {
            if (context.Transaction == null)
            {
                ThrowRequiresTransaction();
                return default; // never hit
            }

#if DEBUG
            var documentDebugHash = document.DebugHash;
            document.BlittableValidation();
            BlittableJsonReaderObject.AssertNoModifications(document, id, assertChildren: true);
            AssertMetadataWasFiltered(document);
#endif

            var newEtag = _documentsStorage.GenerateNextEtag();
            var modifiedTicks = _documentsStorage.GetOrCreateLastModifiedTicks(lastModifiedTicks);

            id = BuildDocumentId(id, newEtag, out bool knownNewId);
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out Slice lowerId, out Slice idPtr))
            {
                var collectionName = _documentsStorage.ExtractCollectionName(context, document);
                var table = context.Transaction.InnerTransaction.OpenTable(DocsSchema, collectionName.GetTableName(CollectionTableType.Documents));
            
                var oldValue = default(TableValueReader);
                if (knownNewId == false)
                {
                    // delete a tombstone if it exists, if it known that it is a new ID, no need, so we can skip it
                    DeleteTombstoneIfNeeded(context, collectionName, lowerId.Content.Ptr, lowerId.Size);

                    table.ReadByKey(lowerId, out oldValue);
                }

                BlittableJsonReaderObject oldDoc = null;
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

                    if (expectedChangeVector != null) 
                    {
                        var oldChangeVector = TableValueToChangeVector(context, (int)DocumentsTable.ChangeVector, ref oldValue);
                        if (string.Compare(expectedChangeVector, oldChangeVector, StringComparison.Ordinal) != 0)
                            ThrowConcurrentException(id, expectedChangeVector, oldChangeVector);
                    }

                    oldDoc = new BlittableJsonReaderObject(oldValue.Read((int)DocumentsTable.Data, out int oldSize), oldSize, context);
                    var oldCollectionName = _documentsStorage.ExtractCollectionName(context, oldDoc);
                    if (oldCollectionName != collectionName)
                        ThrowInvalidCollectionNameChange(id, oldCollectionName, collectionName);

                    var oldFlags = TableValueToFlags((int)DocumentsTable.Flags, ref oldValue);

                    if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.FromReplication) == false)
                    {
                        if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByAttachmentUpdate) == false &&
                            oldFlags.Contain(DocumentFlags.HasAttachments))
                        {
                            flags |= DocumentFlags.HasAttachments;
                        }

                        if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ByCountersUpdate) == false &&
                            oldFlags.Contain(DocumentFlags.HasCounters))
                        {
                            flags |= DocumentFlags.HasCounters;
                        }

                        if (oldFlags.Contain(DocumentFlags.HasRevisions))
                        {
                            flags |= DocumentFlags.HasRevisions;
                        }
                    }
                }

                var result = BuildChangeVectorAndResolveConflicts(context, id, lowerId, newEtag, document, changeVector, expectedChangeVector, flags, oldValue);

                if (flags.Contain(DocumentFlags.FromClusterTransaction) == false)
                {
                    if (string.IsNullOrEmpty(result.ChangeVector))
                        ChangeVectorUtils.ThrowConflictingEtag(id, changeVector, newEtag, _documentsStorage.Environment.Base64Id, _documentDatabase.ServerStore.NodeTag);

                    changeVector = result.ChangeVector;
                }

                nonPersistentFlags |= result.NonPersistentFlags;
                if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.Resolved))
                {
                    flags |= DocumentFlags.Resolved;
                }

                if (collectionName.IsHiLo == false && flags.Contain(DocumentFlags.Artificial) == false)
                {
                    if (ShouldRecreateAttachments(context, lowerId, oldDoc, document, ref flags, nonPersistentFlags))
                    {
#if DEBUG
                        if (document.DebugHash != documentDebugHash)
                        {
                            throw new InvalidDataException("The incoming document " + id + " has changed _during_ the put process, " +
                                                           "this is likely because you are trying to save a document that is already stored and was moved");
                        }
#endif
                        document = context.ReadObject(document, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
#if DEBUG
                        documentDebugHash = document.DebugHash;
                        document.BlittableValidation();
                        BlittableJsonReaderObject.AssertNoModifications(document, id, assertChildren: true);
                        AssertMetadataWasFiltered(document);
                        AttachmentsStorage.AssertAttachments(document, flags);
#endif
                    }

                    if (ShouldRecreateCounters(context, id, oldDoc, document, ref flags, nonPersistentFlags))
                    {
#if DEBUG
                        if (document.DebugHash != documentDebugHash)
                        {
                            throw new InvalidDataException("The incoming document " + id + " has changed _during_ the put process, " +
                                                           "this is likely because you are trying to save a document that is already stored and was moved");
                        }
#endif
                        document = context.ReadObject(document, id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
#if DEBUG
                        documentDebugHash = document.DebugHash;
                        document.BlittableValidation();
                        BlittableJsonReaderObject.AssertNoModifications(document, id, assertChildren: true);
                        AssertMetadataWasFiltered(document);
                        CountersStorage.AssertCounters(document, flags);
#endif
                    }

                    var shouldVersion = _documentDatabase.DocumentsStorage.RevisionsStorage.ShouldVersionDocument(collectionName, nonPersistentFlags, oldDoc, document,
                        ref flags, out var configuration);
                    if (shouldVersion)
                    {
                        if (ShouldVersionOldDocument(flags, oldDoc))
                        {
                            var oldFlags = TableValueToFlags((int)DocumentsTable.Flags, ref oldValue);
                            var oldChangeVector = TableValueToChangeVector(context, (int)DocumentsTable.ChangeVector, ref oldValue);
                            var oldTicks = TableValueToDateTime((int)DocumentsTable.LastModified, ref oldValue);
                            
                            _documentDatabase.DocumentsStorage.RevisionsStorage.Put(context, id, oldDoc, oldFlags | DocumentFlags.HasRevisions, NonPersistentDocumentFlags.None,
                                oldChangeVector, oldTicks.Ticks, configuration, collectionName);
                        }
                        flags |= DocumentFlags.HasRevisions;

                        _documentDatabase.DocumentsStorage.RevisionsStorage.Put(context, id, document, flags, nonPersistentFlags, changeVector, modifiedTicks, configuration, collectionName);
                    }
                }

                using (Slice.From(context.Allocator, changeVector, out var cv))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(lowerId);
                    tvb.Add(Bits.SwapBytes(newEtag));
                    tvb.Add(idPtr);
                    tvb.Add(document.BasePointer, document.Size);
                    tvb.Add(cv.Content.Ptr, cv.Size);
                    tvb.Add(modifiedTicks);
                    tvb.Add((int)flags);
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

                if (collectionName.IsHiLo == false)
                {
                    _documentsStorage.ExpirationStorage.Put(context, lowerId, document);
                }

                context.LastDatabaseChangeVector = changeVector;
                _documentDatabase.Metrics.Docs.PutsPerSec.MarkSingleThreaded(1);
                _documentDatabase.Metrics.Docs.BytesPutsPerSec.MarkSingleThreaded(document.Size);

                context.Transaction.AddAfterCommitNotification(new DocumentChange
                {
                    ChangeVector = changeVector,
                    CollectionName = collectionName.Name,
                    Id = id,
                    Type = DocumentChangeTypes.Put,
                });

#if DEBUG
                if (document.DebugHash != documentDebugHash)
                {
                    throw new InvalidDataException("The incoming document " + id + " has changed _during_ the put process, " +
                                                   "this is likely because you are trying to save a document that is already stored and was moved");
                }
                document.BlittableValidation();
                BlittableJsonReaderObject.AssertNoModifications(document, id, assertChildren: true);
                AssertMetadataWasFiltered(document);
                AttachmentsStorage.AssertAttachments(document, flags);
#endif
                return new PutOperationResults
                {
                    Etag = newEtag,
                    Id = id,
                    Collection = collectionName,
                    ChangeVector = changeVector,
                    Flags = flags,
                    LastModified = new DateTime(modifiedTicks)
                };
            }
        }

        private static bool ShouldVersionOldDocument(DocumentFlags flags, BlittableJsonReaderObject oldDoc)
        {
            if (oldDoc == null)
                return false; // no document to version

            if (flags.Contain(DocumentFlags.HasRevisions))
                return false; // version already exists

            if (flags.Contain(DocumentFlags.Resolved))
                return false; // we already versioned it with the a conflicted flag

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (string ChangeVector, NonPersistentDocumentFlags NonPersistentFlags) BuildChangeVectorAndResolveConflicts(
            DocumentsOperationContext context, string id, Slice lowerId, long newEtag,
            BlittableJsonReaderObject document, string changeVector, string expectedChangeVector, DocumentFlags flags, TableValueReader oldValue)
        {
            var nonPersistentFlags = NonPersistentDocumentFlags.None;
            var fromReplication = (flags & DocumentFlags.FromReplication) == DocumentFlags.FromReplication;

            if (_documentsStorage.ConflictsStorage.ConflictsCount != 0)
            {
                // Since this document resolve the conflict we don't need to alter the change vector.
                // This way we avoid another replication back to the source

                _documentsStorage.ConflictsStorage.ThrowConcurrencyExceptionOnConflictIfNeeded(context, lowerId, expectedChangeVector);

                if (fromReplication)
                {
                    nonPersistentFlags = _documentsStorage.ConflictsStorage.DeleteConflictsFor(context, id, document).NonPersistentFlags;
                }
                else
                {
                    var result = _documentsStorage.ConflictsStorage.MergeConflictChangeVectorIfNeededAndDeleteConflicts(changeVector, context, id, newEtag, document);
                    changeVector = result.ChangeVector;
                    nonPersistentFlags = result.NonPersistentFlags;
                }
            }

            if (string.IsNullOrEmpty(changeVector) == false)
                return (changeVector, nonPersistentFlags);

            string oldChangeVector;
            if (fromReplication == false)
            {
                if (context.LastDatabaseChangeVector == null)
                    context.LastDatabaseChangeVector = GetDatabaseChangeVector(context);
                oldChangeVector = context.LastDatabaseChangeVector;
            }
            else
            {
                oldChangeVector = oldValue.Pointer != null ? TableValueToChangeVector(context, (int)DocumentsTable.ChangeVector, ref oldValue) : null;
            }
            changeVector = SetDocumentChangeVectorForLocalChange(context, lowerId, oldChangeVector, newEtag);
            return (changeVector, nonPersistentFlags);
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
                var lastChar = id[id.Length - 1];
                if (lastChar == '|')
                {
                    ThrowInvalidDocumentId(id);
                }

                if (lastChar == '/')
                {                    
                    string nodeTag = _documentDatabase.ServerStore.NodeTag;

                    // PERF: we are creating an string and mutating it for performance reasons.
                    //       while nasty this shouldn't have any side effects because value didn't
                    //       escape yet the function, so while not pretty it works (and it's safe).      
                    string value = new string('0', id.Length + 1 + 19 + nodeTag.Length);
                    fixed (char* valuePtr = value)
                    {
                        char* valueTagPtr = valuePtr + value.Length - nodeTag.Length;
                        for (int j = 0; j < nodeTag.Length; j++)
                            valueTagPtr[j] = nodeTag[j];

                        int i;
                        for (i = 0; i < id.Length; i++)
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

            // Intentionally have just one return statement here for better inlining
            return id;
        }

        private static void ThrowInvalidDocumentId(string id)
        {
            throw new NotSupportedException("Document ids cannot end with '|', but was called with " + id +
                                            ". Identities are only generated for external requests, not calls to PutDocument and such.");
        }

        private bool RecreateAttachments(DocumentsOperationContext context, Slice lowerId, BlittableJsonReaderObject document,
            BlittableJsonReaderObject metadata, ref DocumentFlags flags)
        {
            var actualAttachments = _documentsStorage.AttachmentsStorage.GetAttachmentsMetadataForDocument(context, lowerId);
            if (actualAttachments.Count == 0)
            {
                flags &= ~DocumentFlags.HasAttachments;
                var attachmentsKey = context.GetLazyStringForFieldWithCaching(Constants.Documents.Metadata.Attachments);
                if (metadata?.Contains(attachmentsKey) != true)
                    return false;

                metadata.Modifications = new DynamicJsonValue(metadata);
                metadata.Modifications.Remove(Constants.Documents.Metadata.Attachments);
                document.Modifications = new DynamicJsonValue(document)
                {
                    [Constants.Documents.Metadata.Key] = metadata
                };
                return true;

            }

            flags |= DocumentFlags.HasAttachments;
            if (metadata == null)
            {
                document.Modifications = new DynamicJsonValue(document)
                {
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Attachments] = actualAttachments
                    }
                };
            }
            else
            {
                metadata.Modifications = new DynamicJsonValue(metadata)
                {
                    [Constants.Documents.Metadata.Attachments] = actualAttachments
                };
                document.Modifications = new DynamicJsonValue(document)
                {
                    [Constants.Documents.Metadata.Key] = metadata
                };
            }

            return true;
        }

        private bool ShouldRecreateAttachments(DocumentsOperationContext context, Slice lowerId, BlittableJsonReaderObject oldDoc,
            BlittableJsonReaderObject document, ref DocumentFlags flags, NonPersistentDocumentFlags nonPersistentFlags)
        {
            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ResolveAttachmentsConflict) ||
                nonPersistentFlags.Contain(NonPersistentDocumentFlags.PreserveAttachments))
            {
                document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata);
                return RecreateAttachments(context, lowerId, document, metadata, ref flags);
            }

            if ((flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments &&
                (nonPersistentFlags & NonPersistentDocumentFlags.ByAttachmentUpdate) != NonPersistentDocumentFlags.ByAttachmentUpdate &&
                (nonPersistentFlags & NonPersistentDocumentFlags.FromReplication) != NonPersistentDocumentFlags.FromReplication)
            {
                if (oldDoc != null &&
                    oldDoc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject oldMetadata) &&
                    oldMetadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray oldAttachments))
                {
                    // Make sure the user did not changed the value of @attachments in the @metadata
                    // In most cases it won't be changed so we can use this value 
                    // instead of recreating the document's blittable from scratch
                    if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                        metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false ||
                        attachments.Equals(oldAttachments) == false)
                    {
                        return RecreateAttachments(context, lowerId, document, metadata, ref flags);
                    }
                }
            }

            return false;
        }

        private bool RecreateCounters(DocumentsOperationContext context, string id, BlittableJsonReaderObject document,
            BlittableJsonReaderObject metadata, ref DocumentFlags flags)
        {
            var countersStorage = context.DocumentDatabase.DocumentsStorage.CountersStorage;
            var onDiskCounters = countersStorage.GetCountersForDocument(context, id).ToList();

            if (onDiskCounters.Count == 0)
            {
                flags &= ~DocumentFlags.HasCounters;
                var counterKeyString = context.GetLazyStringForFieldWithCaching(Constants.Documents.Metadata.Counters);
                if (metadata?.Contains(counterKeyString) != true)
                    return false;

                metadata.Modifications = new DynamicJsonValue(metadata);
                metadata.Modifications.Remove(Constants.Documents.Metadata.Counters);
                document.Modifications = new DynamicJsonValue(document)
                {
                    [Constants.Documents.Metadata.Key] = metadata
                };

                return true;
            }

            flags |= DocumentFlags.HasCounters;

            if (metadata == null)
            {
                document.Modifications = new DynamicJsonValue(document)
                {
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Counters] = onDiskCounters
                    }
                };
            }
            else
            {
                metadata.Modifications = new DynamicJsonValue(metadata)
                {
                    [Constants.Documents.Metadata.Counters] = new DynamicJsonArray(onDiskCounters)
                };

                document.Modifications = new DynamicJsonValue(document)
                {
                    [Constants.Documents.Metadata.Key] = metadata
                };

            }

            return true;
        }

        private bool ShouldRecreateCounters(DocumentsOperationContext context, string id, BlittableJsonReaderObject oldDoc,
            BlittableJsonReaderObject document, ref DocumentFlags flags, NonPersistentDocumentFlags nonPersistentFlags)
        {
            if (nonPersistentFlags.Contain(NonPersistentDocumentFlags.ResolveCountersConflict) || 
                flags.Contain(DocumentFlags.FromClusterTransaction))
            {
                document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata);
                return RecreateCounters(context, id, document, metadata, ref flags);
            }

            if ((flags & DocumentFlags.HasCounters) == DocumentFlags.HasCounters &&
                (nonPersistentFlags & NonPersistentDocumentFlags.FromReplication) != NonPersistentDocumentFlags.FromReplication &&
                (nonPersistentFlags & NonPersistentDocumentFlags.ByCountersUpdate) != NonPersistentDocumentFlags.ByCountersUpdate)
            {
                if (oldDoc != null &&
                    oldDoc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject oldMetadata) &&
                    oldMetadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray oldCounters))
                {
                    // Make sure the user did not change the value of @counters in the @metadata
                    // In most cases it won't be changed so we can use this value 
                    // instead of recreating the document's blittable from scratch
                    if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                        metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray counters) == false ||
                        counters.Length != oldCounters.Length ||
                        !counters.All(oldCounters.Contains) )
                    {
                        return RecreateCounters(context, id, document, metadata, ref flags);
                    }
                }
            }

            return false;
        }

        public static void ThrowRequiresTransaction([CallerMemberName]string caller = null)
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentException("Context must be set with a valid transaction before calling " + caller, "context");
        }

        private static void ThrowConcurrentExceptionOnMissingDoc(string id, string expectedChangeVector)
        {
            throw new ConcurrencyException(
                $"Document {id} does not exist, but Put was called with change vector: {expectedChangeVector}. Optimistic concurrency violation, transaction will be aborted.")
            {
                ExpectedChangeVector = expectedChangeVector
            };
        }

        private static void ThrowInvalidCollectionNameChange(string id, CollectionName oldCollectionName, CollectionName collectionName)
        {
            throw new InvalidOperationException(
                $"Changing '{id}' from '{oldCollectionName.Name}' to '{collectionName.Name}' via update is not supported.{Environment.NewLine}" +
                $"Delete it and recreate the document {id}.");
        }

        private static void ThrowConcurrentException(string id, string expectedChangeVector, string oldChangeVector)
        {
            throw new ConcurrencyException(
                $"Document {id} has change vector {oldChangeVector}, but Put was called with {(expectedChangeVector.Length == 0 ? "expecting new document" : "change vector " + expectedChangeVector)}. Optimistic concurrency violation, transaction will be aborted.")
            {
                ActualChangeVector = oldChangeVector,
                ExpectedChangeVector = expectedChangeVector
            };
        }

        private static void DeleteTombstoneIfNeeded(DocumentsOperationContext context, CollectionName collectionName, byte* lowerId, int lowerSize)
        {
            var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, collectionName.GetTableName(CollectionTableType.Tombstones));
            using (Slice.External(context.Allocator, lowerId, lowerSize, out Slice id))
            {
                if (tombstoneTable.ReadByKey(id, out var reader) == false)
                    return;

                if (tombstoneTable.IsOwned(reader.Id))
                {
                    tombstoneTable.Delete(reader.Id);
                }
            }
        }

        private string SetDocumentChangeVectorForLocalChange(DocumentsOperationContext context, Slice lowerId, string oldChangeVector, long newEtag)
        {
            if (string.IsNullOrEmpty(oldChangeVector) == false)
            {
                var result = ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase.ServerStore.NodeTag, _documentsStorage.Environment.Base64Id, newEtag, oldChangeVector);
                return result.ChangeVector;
            }

            return _documentsStorage.ConflictsStorage.GetMergedConflictChangeVectorsAndDeleteConflicts(context, lowerId, newEtag);
        }

        [Conditional("DEBUG")]
        public static void AssertMetadataWasFiltered(BlittableJsonReaderObject data)
        {
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
