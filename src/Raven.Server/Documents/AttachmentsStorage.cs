﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Attachments;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Schemas.Attachments;
using static Raven.Server.Documents.Schemas.Documents;
using static Raven.Server.Documents.Schemas.Tombstones;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents
{
    public sealed class AttachmentDetailsServer : AttachmentDetails
    {
        public CollectionName CollectionName;
    }

    public struct MoveAttachmentDetailsServer
    {
        public AttachmentDetails Result;
        public CollectionName SourceCollectionName;
        public CollectionName DestinationCollectionName;
    }

    public struct AttachmentHashesCount
    {
        public long RetiredHashes;
        public long RegularHashes;
        public long TotalHashes;
    }

    public unsafe partial class AttachmentsStorage
    {
        public RetiredAttachmentsStorage RetiredAttachmentsStorage;

        internal readonly TableSchema AttachmentsSchema;
        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;
        private readonly Logger _logger;

        public AttachmentsStorage([NotNull] DocumentDatabase database, [NotNull] Transaction tx, [NotNull] TableSchema schema)
        {
            if (tx == null)
                throw new ArgumentNullException(nameof(tx));

            _documentDatabase = database ?? throw new ArgumentNullException(nameof(database));
            _documentsStorage = database.DocumentsStorage;

            AttachmentsSchema = schema ?? throw new ArgumentNullException(nameof(schema));

            tx.CreateTree(AttachmentsSlice);
            AttachmentsSchema.Create(tx, AttachmentsMetadataSlice, 44);
            _documentDatabase.DocumentsStorage.TombstonesSchema.Create(tx, AttachmentsTombstonesSlice, 16);
            _logger = LoggingSource.Instance.GetLogger<AttachmentsStorage>(database.Name);
            RetiredAttachmentsStorage = new RetiredAttachmentsStorage(tx, database);
        }

        public static long ReadLastEtag(Transaction tx)
        {
            var tableTree = tx.ReadTree(AttachmentsMetadataSlice, RootObjectType.Table);
            var fst = tableTree.FixedTreeFor(AttachmentsEtagSlice, valSize: sizeof(long));

            using var it = fst.Iterate();
            return it.SeekToLast() ? it.CurrentKey : 0;
        }

        public IEnumerable<ReplicationBatchItem> GetAttachmentsFrom(DocumentsOperationContext context, long etag)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            foreach (var result in table.SeekForwardFrom(AttachmentsSchema.FixedSizeIndexes[AttachmentsEtagSlice], etag, 0))
            {
                var attachment = TableValueToAttachment(context, ref result.Reader);

                var stream = GetAttachmentStream(context, attachment.Base64Hash);
                if (stream == null && attachment.Flags.Contain(AttachmentFlags.Retired) == false)
                {
                    ThrowMissingAttachment(GetDocIdAndAttachmentName(context, attachment.Key));
                }
                else
                {
                    attachment.Stream = stream;
                }

                yield return AttachmentReplicationItem.From(context, attachment);
            }
        }

        public IEnumerable<Attachment> GetAllAttachments(DocumentsOperationContext context, bool includeStream = false)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            foreach (var result in table.SeekForwardFrom(AttachmentsSchema.FixedSizeIndexes[AttachmentsEtagSlice], 0, 0))
            {
                var attachment = TableValueToAttachment(context, ref result.Reader);
                if (includeStream)
                {
                    var stream = GetAttachmentStream(context, attachment.Base64Hash);
                    attachment.Stream = stream;
                }

                yield return attachment;
            }
        }

        public IEnumerable<string> GetAllAttachmentsStreamHashes(DocumentsOperationContext context)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(AttachmentsSlice);
            if (tree == null)
                yield break;

            using (var it = tree.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    yield break;
                do
                {
                    yield return it.CurrentKey.ToString();
                } while (it.MoveNext());
            }
        }

        public AttachmentHashesCount GetCountOfAttachmentsForHash(DocumentsOperationContext context, Slice hash)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            using var scope = context.Allocator.Allocate(hash.Size + sizeof(int), out ByteString keyMem);
            Memory.Copy(keyMem.Ptr, hash.Content.Ptr, hash.Size);

            Slice slice;
            var retiredHashes = GetHashesCount(AttachmentFlags.Retired);
            var regularHashes = GetHashesCount(AttachmentFlags.None);

            return new AttachmentHashesCount
            {
                RegularHashes = regularHashes,
                RetiredHashes = retiredHashes,
                TotalHashes = regularHashes + retiredHashes
            };

            long GetHashesCount(AttachmentFlags flag)
            {
                *(int*)(keyMem.Ptr + hash.Size) = Bits.SwapBytes((int)flag);
                slice = new Slice(SliceOptions.Key, keyMem);
                return table.GetCountOfMatchesFor(AttachmentsSchema.DynamicKeyIndexes[AttachmentsHashAndFlagSlice], slice);
            }
        }

        internal IEnumerable<AttachmentNameWithCount> GetAttachmentsMetadataForDocumentWithCounts(DocumentsOperationContext context, string lowerDocumentId)
        {
            using (Slice.From(context.Allocator, lowerDocumentId, out Slice lowerDocumentIdSlice))
            using (GetAttachmentPrefix(context, lowerDocumentIdSlice, AttachmentType.Document, Slices.Empty, out Slice prefixSlice))
            {
                foreach (var attachment in GetAttachmentsForDocument(context, prefixSlice))
                {
                    var counts = GetCountOfAttachmentsForHash(context, attachment.Base64Hash);
                    yield return new AttachmentNameWithCount
                    {
                        Name = attachment.Name,
                        Hash = attachment.Base64Hash.ToString(),
                        ContentType = attachment.ContentType,
                        Size = attachment.Size,
                        Count = counts.RegularHashes,
                        RetiredCount = counts.RetiredHashes,
                        TotalCount = counts.TotalHashes,
                    };
                }
            }
        }

        public void RetireAttachment(DocumentsOperationContext context, AttachmentDetailsServer attachment, Slice keySlice)
        {
            using (DocumentIdWorker.GetSliceFromId(context, attachment.DocumentId, out Slice lowerDocumentId))
            {
                if (TryGetDocumentTableValueReaderForAttachment(context, attachment.DocumentId, attachment.Name, lowerDocumentId, out var documentTvr) == false)
                    throw new DocumentDoesNotExistException($"Cannot retire attachment '{attachment.Name}' on a non existent document '{attachment.DocumentId}'.");

                var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
                if (table.ReadByKey(keySlice, out TableValueReader attachmentTvr) == false)
                    throw new AttachmentDoesNotExistException($"Cannot retire attachment '{attachment.Name}' because it doesn't exists.");

                var attachmentEtag = _documentsStorage.GenerateNextEtag();
                var changeVector = _documentsStorage.GetNewChangeVector(context, attachmentEtag);
                var size = TableValueToLong((int)AttachmentsTable.Size, ref attachmentTvr);
                var retireAt = TableValueToLong((int)AttachmentsTable.RetireAt, ref attachmentTvr);

                using (TableValueToSlice(context, (int)AttachmentsTable.Name, ref attachmentTvr, out var nameSlice))
                using (TableValueToSlice(context, (int)AttachmentsTable.ContentType, ref attachmentTvr, out var contentTypeSlice))
                using (TableValueToSlice(context, (int)AttachmentsTable.Hash, ref attachmentTvr, out var hashSlice))
                using (Slice.From(context.Allocator, changeVector, out var changeVectorSlice))
                using (TableValueToSlice(context, (int)AttachmentsTable.Collection, ref attachmentTvr, out var collectionSlice))

                using (table.Allocate(out TableValueBuilder tvb))
                {
                    // add Retired flag
                    tvb.Add(keySlice.Content.Ptr, keySlice.Size);
                    tvb.Add(Bits.SwapBytes(attachmentEtag));
                    tvb.Add(nameSlice);
                    tvb.Add(contentTypeSlice);
                    tvb.Add(hashSlice);
                    tvb.Add(context.GetTransactionMarker());
                    tvb.Add(changeVectorSlice);
                    tvb.Add(size);
                    tvb.Add(Bits.SwapBytes((int)AttachmentFlags.Retired));
                    tvb.Add(retireAt);
                    tvb.Add(collectionSlice);
                    table.Update(attachmentTvr.Id, tvb);

                    // Delete the attachment stream if needed
                    context.Transaction.CheckIfShouldDeleteAttachmentStream(hashSlice);
                }

                UpdateDocumentAfterAttachmentChange(context, lowerDocumentId, attachment.DocumentId, documentTvr, changeVector, extractCollectionName: false, out _);
            }
        }

        public AttachmentDetailsServer PutAttachment(DocumentsOperationContext context, string documentId, string name, string contentType,
            string hash, AttachmentFlags flags, long size, DateTime? retireAtDt, string expectedChangeVector = null, Stream stream = null, 
            bool updateDocument = true, bool extractCollectionName = false, bool fromSmuggler = false, CollectionName collection2 = null, bool fromEtl = false)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false);// never hit
            }

            // Attachment etag should be generated before updating the document
            var attachmentEtag = _documentsStorage.GenerateNextEtag();

            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice lowerDocumentId))
            {
                TableValueReader tvr = default;
                if (fromSmuggler == false)
                {
                    // This will validate that we cannot put an attachment on a conflicted document
                    var hasDoc = TryGetDocumentTableValueReaderForAttachment(context, documentId, name, lowerDocumentId, out tvr);
                    if (hasDoc == false)
                        throw new DocumentDoesNotExistException($"Cannot put attachment {name} on a non existent document '{documentId}'.");
                    if (TableValueToFlags((int)DocumentsTable.Flags, ref tvr).HasFlag(DocumentFlags.Artificial))
                        throw new InvalidOperationException($"Cannot put attachment {name} on artificial document '{documentId}'.");
                }
                CollectionName collectionName = fromSmuggler == false ? GetDocumentCollectionName(context, tvr) : collection2;
                Debug.Assert(collectionName != null, "collectionName != null");
                using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, name, out Slice lowerName, out Slice namePtr))
                using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, contentType, out Slice lowerContentType, out Slice contentTypePtr))
                using (Slice.From(context.Allocator, hash, out Slice base64Hash)) // Hash is a base64 string, so this is a special case that we do not need to escape
                using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, collectionName.Name, out Slice lowerCollectionName, out Slice collectionNamePtr))
                using (GetAttachmentKey(context, lowerDocumentId.Content.Ptr, lowerDocumentId.Size, lowerName.Content.Ptr, lowerName.Size, base64Hash,
                           lowerContentType.Content.Ptr, lowerContentType.Size, AttachmentType.Document, Slices.Empty, out Slice keySlice))
                {
                    Debug.Assert(base64Hash.Size == 44, $"Hash size should be 44 but was: {keySlice.Size}");


                    DeleteTombstoneIfNeeded(context, keySlice);

                    var changeVector = _documentsStorage.GetNewChangeVector(context, attachmentEtag);
                    Debug.Assert(changeVector != null);
                    long retireAt;

                    var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
                    void SetTableValue(TableValueBuilder tvb, Slice cv)
                    {
                        tvb.Add(keySlice.Content.Ptr, keySlice.Size);
                        tvb.Add(Bits.SwapBytes(attachmentEtag));
                        tvb.Add(namePtr);
                        tvb.Add(contentTypePtr);
                        tvb.Add(base64Hash.Content.Ptr, base64Hash.Size);
                        tvb.Add(context.GetTransactionMarker());
                        tvb.Add(cv.Content.Ptr, cv.Size);
                        tvb.Add(size);
                        tvb.Add(Bits.SwapBytes((int)flags));
                        tvb.Add(retireAt);
                        tvb.Add(collectionNamePtr);

                    }

                    if (table.ReadByKey(keySlice, out TableValueReader oldValue))
                    {
                        // This is an update to the attachment with the same stream and content type
                        // Just updating the etag and casing of the name and the content type.

                        if (expectedChangeVector != null)
                        {
                            var oldChangeVector = TableValueToChangeVector(context, (int)AttachmentsTable.ChangeVector, ref oldValue);
                            if (ChangeVector.CompareVersion(oldChangeVector, expectedChangeVector, context) != 0)
                                ThrowConcurrentException(documentId, name, expectedChangeVector, oldChangeVector);
                        }

                        size = TableValueToLong((int)AttachmentsTable.Size, ref oldValue);

                        retireAt = TableValueToLong((int)AttachmentsTable.RetireAt, ref oldValue);
                        var existingFlags = TableValueToAttachmentFlags((int)AttachmentsTable.Flags, ref oldValue);
                        if (existingFlags.HasFlag(AttachmentFlags.Retired) == false && retireAt == -1L)
                        {
                            var dbRecord = _documentDatabase.ReadDatabaseRecord();
                            Debug.Assert(collectionName != null, "collectionName != null");
                            Debug.Assert(flags == AttachmentFlags.None, "flags == AttachmentFlags.None");

                            TryPutRetiredAttachment(context, dbRecord, collectionName, keySlice, out retireAt);
                        }

                        using (Slice.From(context.Allocator, changeVector, out var changeVectorSlice))
                        using (table.Allocate(out TableValueBuilder tvb))
                        {
                            SetTableValue(tvb, changeVectorSlice);
                            table.Update(oldValue.Id, tvb);
                        }
                    }
                    else
                    {
                        var putStream = true;
                        var attachmentExists = false;

                        // We already asserted that the document is not in conflict, so we might have just one partial key, not more.
                        using (GetAttachmentPartialKey(context, keySlice, base64Hash.Size, lowerContentType.Size, out Slice partialKeySlice))
                        {
                            if (table.SeekOnePrimaryKeyPrefix(partialKeySlice, out TableValueReader partialTvr))
                            {
                                attachmentExists = true;
                                if (expectedChangeVector != null)
                                {
                                    var oldChangeVector = TableValueToChangeVector(context, (int)AttachmentsTable.ChangeVector, ref partialTvr);
                                    if (ChangeVector.CompareVersion(oldChangeVector, expectedChangeVector, context) != 0)
                                        ThrowConcurrentException(documentId, name, expectedChangeVector, oldChangeVector);
                                }

                                if (fromSmuggler == false)
                                {
                                    var doc = _documentsStorage.TableValueToDocument(context, ref tvr);
                                    var collection = _documentsStorage.ExtractCollectionName(context, doc.Data);
                                    var docChangeVector = context.GetChangeVector(doc.ChangeVector);
                                    var configuration = _documentsStorage.RevisionsStorage.GetRevisionsConfiguration(collection.Name, doc.Flags);
                                    if (configuration != null)
                                    {
                                        var shouldVersionOldDoc = _documentsStorage.RevisionsStorage.ShouldVersionOldDocument(context, doc.Flags, doc.Data, docChangeVector, collection);
                                        if (shouldVersionOldDoc)
                                        {
                                            _documentsStorage.RevisionsStorage.Put(context, documentId, doc.Data, doc.Flags | DocumentFlags.HasRevisions | DocumentFlags.FromOldDocumentRevision, NonPersistentDocumentFlags.None, docChangeVector, doc.LastModified.Ticks, configuration, collection);
                                        }
                                    }
                                }

                                // Delete the attachment stream only if we have a different hash
                                using (TableValueToSlice(context, (int)AttachmentsTable.Hash, ref partialTvr, out Slice existingHash))
                                {
                                    putStream = existingHash.Content.Match(base64Hash.Content) == false;
                                    if (putStream)
                                    {
                                        using (TableValueToSlice(context, (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType,
                                                   ref partialTvr, out Slice existingKey))
                                        {
                                            var existingEtag = TableValueToEtag((int)AttachmentsTable.Etag, ref partialTvr);
                                            var lastModifiedTicks = _documentDatabase.Time.GetUtcNow().Ticks;
                                            //TODO: egor if we update retired attachment (can this even happen?) we might delete the old one from cloud storage... I think need to check if it is retired && if it has purgeOn delete ?
                                            var existingAttachmentFlags = TableValueToAttachmentFlags((int)AttachmentsTable.Flags, ref partialTvr);
                                            var existingRetireAtTicks = TableValueToLong((int)AttachmentsTable.RetireAt, ref partialTvr);

                                            if (existingAttachmentFlags.Contain(AttachmentFlags.Retired))
                                            {
                                                var dbRecord2 = _documentDatabase.ReadDatabaseRecord();
                                                if (dbRecord2.RetiredAttachments is { Disabled: false })
                                                {
                                                    if (dbRecord2.RetiredAttachments.PurgeOnDelete == false)
                                                    {
                                                        // we cannot delete from cloud since PurgeOnDelete is false
                                                        DeleteInternal(context, existingKey, existingEtag, existingHash, changeVector, lastModifiedTicks, flags: DocumentFlags.None, existingAttachmentFlags, existingRetireAtTicks, collectionName.Name, storageOnly: true);
                                                    }
                                                    else
                                                    {
                                                        DeleteInternal(context, existingKey, existingEtag, existingHash, changeVector, lastModifiedTicks, flags: DocumentFlags.None, existingAttachmentFlags, existingRetireAtTicks, collectionName.Name, storageOnly: false);
                                                    }
                                                }
                                                else
                                                {
                                                    DeleteInternal(context, existingKey, existingEtag, existingHash, changeVector, lastModifiedTicks, flags: DocumentFlags.None, existingAttachmentFlags, existingRetireAtTicks, collectionName.Name, storageOnly: true);
                                                }
                                            }
                                            else
                                            {
                                                // we cannot delete from retired since there is no configuration
                                                DeleteInternal(context, existingKey, existingEtag, existingHash, changeVector, lastModifiedTicks, flags: DocumentFlags.None, existingAttachmentFlags, existingRetireAtTicks, collectionName.Name, storageOnly: false);
                                            }

                                        }
                                    }
                                }

                                table.Delete(partialTvr.Id);
                            }
                        }

                        if (attachmentExists == false && string.IsNullOrEmpty(expectedChangeVector) == false)
                        {
                            ThrowConcurrentExceptionOnMissingAttachment(documentId, name, expectedChangeVector);
                        }

                        if (putStream && fromSmuggler == false)
                        {
                            if (fromEtl == false || flags.Contain(AttachmentFlags.Retired) == false)
                            {
                                PutAttachmentStream(context, keySlice, base64Hash, stream);
                            }
                        }

                        var dbRecord = _documentDatabase.ReadDatabaseRecord();

                        if (fromSmuggler == false)
                        {
                            if (fromEtl && flags.Contain(AttachmentFlags.Retired))
                            {
                                retireAt = retireAtDt.HasValue == false ? -1L : retireAtDt.Value.Ticks;
                            }
                            else
                            {
                                Debug.Assert(collectionName != null, "collectionName != null");
                                Debug.Assert(flags == AttachmentFlags.None, "flags == AttachmentFlags.None");

                                if (retireAtDt != null)
                                {
                                    //TODO: egor now this is only done from ETL, but in theory can be done using the client API
                                    retireAt = retireAtDt.Value.Ticks;
                                    RetiredAttachmentsStorage.Put(context, keySlice, retireAtDt.Value.GetDefaultRavenFormat());
                                }
                                else
                                {
                                    TryPutRetiredAttachment(context, dbRecord, collectionName, keySlice, out retireAt);
                                }
                            }
                        }
                        else
                        {
                            if (retireAtDt != null)
                            {
                                retireAt = retireAtDt.Value.Ticks;
                                RetiredAttachmentsStorage.Put(context, keySlice, retireAtDt.Value.GetDefaultRavenFormat());
                            } 
                            else 
                                retireAt = -1L;
                        }
                   
                        using (Slice.From(context.Allocator, changeVector, out var changeVectorSlice))
                        using (table.Allocate(out TableValueBuilder tvb))
                        {
                            SetTableValue(tvb, changeVectorSlice);
                            table.Insert(tvb);
                        }
                    }

                    _documentDatabase.Metrics.Attachments.PutsPerSec.MarkSingleThreaded(1);



                    if (updateDocument)
                        UpdateDocumentAfterAttachmentChange(context, lowerDocumentId, documentId, tvr, changeVector, extractCollectionName: false, out _);

                    
                    return new AttachmentDetailsServer
                    {
                        ChangeVector = changeVector,
                        ContentType = contentType,
                        Name = name,
                        DocumentId = documentId,
                        Hash = hash,
                        Size = stream?.Length ?? -1,
                        CollectionName = extractCollectionName ? collectionName : default
                    };
                }
            }
        }

        private void TryPutRetiredAttachment(DocumentsOperationContext context, DatabaseRecord dbRecord, CollectionName collectionName, Slice keySlice, out long retireAt)
        {
            if (dbRecord.RetiredAttachments is { Disabled: false })
            {
                if (dbRecord.RetiredAttachments.RetirePeriods.TryGetValue(collectionName.Name, out var timeSpan))
                {
                    var retire = DateTime.UtcNow + timeSpan;
                    retireAt = retire.Ticks;
                    RetiredAttachmentsStorage.Put(context, keySlice, retire.GetDefaultRavenFormat());
                }
                else
                {
                    retireAt = -1L;
                }
            }
            else
            {
                retireAt = -1L;
            }
        }

        private void TryDeleteRetiredAttachment(DocumentsOperationContext context, Slice keySlice, string collection)
        {
            var dbRecord = _documentDatabase.ReadDatabaseRecord();
            if (dbRecord.RetiredAttachments is { Disabled: false })
            {
                if (dbRecord.RetiredAttachments.HasUploader() == false)
                {
                    var msg = $"Cannot delete attachment '{keySlice}' because {nameof(RetiredAttachmentsConfiguration)} does not have any uploader configured.";
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(msg);
                    throw new InvalidOperationException(msg);
                }

                RetiredAttachmentsStorage.PutDelete(context, keySlice, DateTime.UtcNow.Ticks, collection);
            }
            else
            {
                var msg = $"Tried to delete retired attachment with key: '{keySlice}' but {nameof(RetiredAttachmentsConfiguration)} is disabled.";
                if (_logger.IsOperationsEnabled)
                    _logger.Operations(msg);
                throw new InvalidOperationException(msg);
            }
        }

        /// <summary>
        /// Should be used only from replication or smuggler.
        /// </summary>
        public void PutDirect(DocumentsOperationContext context, Slice key, Slice name, Slice contentType, Slice base64Hash, DateTime? retireAt, Slice collection, AttachmentFlags flags, long size, bool isRevision, string changeVector = null)
        {
            Debug.Assert(base64Hash.Size == 44, $"Hash size should be 44 but was: {key.Size}");

            var newEtag = _documentsStorage.GenerateNextEtag();

            if (string.IsNullOrEmpty(changeVector))
            {
                changeVector = _documentsStorage.GetNewChangeVector(context, newEtag);
            }
            Debug.Assert(changeVector != null);
            DeleteTombstoneIfNeeded(context, key);
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            using (Slice.From(context.Allocator, changeVector, out var changeVectorSlice))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(key.Content.Ptr, key.Size);
                tvb.Add(Bits.SwapBytes(newEtag));
                tvb.Add(name.Content.Ptr, name.Size);
                tvb.Add(contentType.Content.Ptr, contentType.Size);
                tvb.Add(base64Hash.Content.Ptr, base64Hash.Size);
                tvb.Add(context.GetTransactionMarker());
                tvb.Add(changeVectorSlice.Content.Ptr, changeVectorSlice.Size);
                tvb.Add(size);
                tvb.Add(Bits.SwapBytes((int)flags));
                if(retireAt.HasValue)
                    tvb.Add(retireAt.Value.Ticks);
                else
                    tvb.Add(-1L);
                tvb.Add(collection.Content.Ptr, collection.Size);
                table.Set(tvb);
            }

            if (isRevision == false)
            {
                // TODO: egor do I need to check for retired config here ?
                if (flags.Contain(AttachmentFlags.Retired) == false && retireAt.HasValue)
                    RetiredAttachmentsStorage.Put(context, key, retireAt.Value.GetDefaultRavenFormat());
            }

            _documentDatabase.Metrics.Attachments.PutsPerSec.MarkSingleThreaded(1);
        }

        private CollectionName GetDocumentCollectionName(DocumentsOperationContext context, TableValueReader tvr)
        {
            var data = new BlittableJsonReaderObject(tvr.Read((int)DocumentsTable.Data, out int size), size, context);

            return _documentsStorage.ExtractCollectionName(context, data);
        }

        /// <summary>
        /// Update the document with an etag which is bigger than the attachmentEtag
        /// We need to call this after we already put the attachment, so it can version also this attachment
        /// </summary>
        private string UpdateDocumentAfterAttachmentChange(DocumentsOperationContext context, Slice lowerDocumentId, string documentId,
            TableValueReader tvr, string changeVector, bool extractCollectionName, out CollectionName collectionName)
        {
            // We can optimize this by copy just the document's data instead of the all tvr
            var copyOfDoc = context.GetMemory(tvr.Size);
            try
            {
                // we have to copy it to the side because we might do a defrag during update, and that
                // can cause corruption if we read from the old value (which we just deleted)
                Memory.Copy(copyOfDoc.Address, tvr.Pointer, tvr.Size);
                var copyTvr = new TableValueReader(copyOfDoc.Address, tvr.Size);
                var data = new BlittableJsonReaderObject(copyTvr.Read((int)DocumentsTable.Data, out int size), size, context);
                var attachments = GetAttachmentsMetadataForDocument(context, lowerDocumentId);

                var flags = DocumentFlags.None;

                data.Modifications = new DynamicJsonValue(data);
                if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
                {
                    metadata.Modifications = new DynamicJsonValue(metadata);

                    if (attachments.Count > 0)
                    {
                        flags |= DocumentFlags.HasAttachments;
                        metadata.Modifications[Constants.Documents.Metadata.Attachments] = attachments;
                    }
                    else
                    {
                        metadata.Modifications.Remove(Constants.Documents.Metadata.Attachments);
                        flags &= ~DocumentFlags.HasAttachments;
                    }

                    data.Modifications[Constants.Documents.Metadata.Key] = metadata;
                }
                else
                {
                    if (attachments.Count > 0)
                    {
                        flags |= DocumentFlags.HasAttachments;
                        data.Modifications[Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Attachments] = attachments
                        };
                    }
                    else
                    {
                        Debug.Assert(false, "Cannot remove an attachment and not have @attachments in @metadata");
                    }
                }

                using (data = context.ReadObject(data, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    collectionName = extractCollectionName ? _documentsStorage.ExtractCollectionName(context, data) : default;
                    return _documentsStorage.Put(context, documentId, null, data, null, null, null, flags, NonPersistentDocumentFlags.ByAttachmentUpdate).ChangeVector;
                }
            }
            finally
            {
                context.ReturnMemory(copyOfDoc);
            }
        }

        public string UpdateDocumentAfterAttachmentChange(DocumentsOperationContext context, string documentId)
        {
            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice lowerDocumentId))
            {
                var exists = _documentsStorage.GetTableValueReaderForDocument(context, lowerDocumentId, throwOnConflict: true, tvr: out TableValueReader tvr);
                if (exists == false)
                    return null;
                return UpdateDocumentAfterAttachmentChange(context, lowerDocumentId, documentId, tvr, null, extractCollectionName: false, out var _);
            }
        }

        public void DeleteAttachmentBeforeRevert(DocumentsOperationContext context, LazyStringValue lowerDocId)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, lowerDocId, out Slice lowerId, out Slice idSlice))
            {
                GetAttachmentKeyInternal(context, lowerId.Content.Ptr, lowerId.Content.Length, default, default, default(Slice), null, 0,
                    KeyType.Prefix, AttachmentType.Document, default, out var key);
                table.DeleteByPrimaryKeyPrefix(key);
            }
        }

        public void RevisionAttachments(DocumentsOperationContext context, BlittableJsonReaderObject document, Slice lowerId, Slice changeVector)
        {
            var currentAttachments = GetAttachmentsFromDocumentMetadata(document);

            foreach (var bjro in currentAttachments)
            {
                var attachment = JsonDeserializationClient.AttachmentName(bjro);
                PutRevisionAttachment(context, lowerId.Content.Ptr, lowerId.Size, changeVector, attachment);
            }
        }

        public void PutAttachmentRevert(DocumentsOperationContext context, LazyStringValue id, BlittableJsonReaderObject document, out bool hasAttachments)
        {
            hasAttachments = false;

            if (document.TryGet(Client.Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                metadata.TryGet(Client.Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                return;

            foreach (BlittableJsonReaderObject attachment in attachments)
            {
                hasAttachments = true;

                if (attachment.TryGet(nameof(AttachmentName.Name), out LazyStringValue name) == false ||
                    attachment.TryGet(nameof(AttachmentName.ContentType), out LazyStringValue contentType) == false ||
                    attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false ||
                    attachment.TryGet(nameof(AttachmentName.Flags), out AttachmentFlags flags) == false ||
                    attachment.TryGet(nameof(AttachmentName.Size), out long size) == false ||
                    attachment.TryGet(nameof(AttachmentName.RetireAt), out DateTime? retireAt) == false ||
                    attachment.TryGet(nameof(AttachmentName.Collection), out LazyStringValue collection) == false)
                    throw new ArgumentException($"The attachment info in missing a mandatory value: {attachment}");

                var cv = Slices.Empty;
                var type = AttachmentType.Document;

                using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerDocumentId))
                using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, name, out Slice lowerName, out Slice nameSlice))
                using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, contentType, out Slice lowerContentType, out Slice contentTypeSlice))
                using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, collection, out _, out Slice collectionSlice))
                using (Slice.External(context.Allocator, hash, out Slice base64Hash))
                using (GetAttachmentKey(context, lowerDocumentId.Content.Ptr, lowerDocumentId.Size, lowerName.Content.Ptr, lowerName.Size,
                    base64Hash, lowerContentType.Content.Ptr, lowerContentType.Size, type, cv, out Slice keySlice))
                {
                    PutDirect(context, keySlice, nameSlice, contentTypeSlice, base64Hash, retireAt, collectionSlice, flags, size, isRevision: false);
                }
            }
        }

        private void PutRevisionAttachment(DocumentsOperationContext context, byte* lowerId, int lowerIdSize, Slice changeVector, AttachmentName attachment)
        {
            var attachmentEtag = _documentsStorage.GenerateNextEtag();

            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, attachment.Name, out Slice lowerName, out Slice namePtr))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, attachment.ContentType, out Slice lowerContentType, out Slice contentTypePtr))
            using (Slice.From(context.Allocator, attachment.Hash, out var hashSlice))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, attachment.Collection, out Slice attachmentCollectionSlice, out Slice attachmentCollectionPtr))

            using (GetAttachmentKey(context, lowerId, lowerIdSize, lowerName.Content.Ptr, lowerName.Size, hashSlice,
                lowerContentType.Content.Ptr, lowerContentType.Size, AttachmentType.Revision, changeVector, out Slice keySlice))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(keySlice.Content.Ptr, keySlice.Size);
                tvb.Add(Bits.SwapBytes(attachmentEtag));
                tvb.Add(namePtr);
                tvb.Add(contentTypePtr);
                tvb.Add(hashSlice);
                tvb.Add(context.GetTransactionMarker());
                tvb.Add(changeVector.Content.Ptr, changeVector.Size);
                tvb.Add(attachment.Size);
                tvb.Add(Bits.SwapBytes((int)attachment.Flags));

                if (attachment.RetireAt.HasValue)
                    tvb.Add(attachment.RetireAt.Value.Ticks);
                else
                    tvb.Add(-1L);
                tvb.Add(attachmentCollectionPtr);
                table.Set(tvb);
            }
        }

        public void PutAttachmentStream(DocumentsOperationContext context, Slice key, Slice base64Hash, Stream stream)
        {
            stream.Position = 0; // We might retry a merged command, so it is a safe place to reset the position here to zero.

            var tree = context.Transaction.InnerTransaction.CreateTree(AttachmentsSlice);
            var existingStream = tree.ReadStream(base64Hash);
            if (existingStream == null)
            {
                tree.AddStream(base64Hash, stream, tag: key);
            }

            _documentDatabase.Metrics.Attachments.BytesPutsPerSec.MarkSingleThreaded(stream.Length);
        }

        private void DeleteAttachmentStream(DocumentsOperationContext context, Slice hash)
        {
            var myHashesStruct = GetCountOfAttachmentsForHash(context, hash);


            if (myHashesStruct.RegularHashes > 0 )
            {
                return;
            }

            Debug.Assert(myHashesStruct.TotalHashes == myHashesStruct.RetiredHashes);

            // all attachments are retired or there is no attachments
            DeleteAttachmentStreamInternal(context, hash);
        }

        private static void DeleteAttachmentStreamInternal(DocumentsOperationContext context, Slice hash)
        {
            var tree = context.Transaction.InnerTransaction.CreateTree(AttachmentsSlice);
            using (tree.GetStreamTag(hash, out var keySlice))
            {
                if (keySlice.HasValue == false)
                    return;

                tree.DeleteStream(hash);
            }
        }

        private bool TryGetDocumentTableValueReaderForAttachment(DocumentsOperationContext context, string documentId,
            string name, Slice lowerDocumentId, out TableValueReader tvr)
        {
            bool hasDoc;
            try
            {
                hasDoc = _documentsStorage.GetTableValueReaderForDocument(context, lowerDocumentId, throwOnConflict: true, tvr: out tvr);
            }
            catch (DocumentConflictException e)
            {
                throw new InvalidOperationException($"Cannot put/delete an attachment {name} on a document '{documentId}' when it has an unresolved conflict.", e);
            }
            return hasDoc;
        }

        public IEnumerable<Attachment> GetAttachmentsForDocument(DocumentsOperationContext context, Slice prefixSlice, bool includeStreams = false)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            foreach (var sr in table.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
            {
                var attachment = TableValueToAttachment(context, ref sr.Value.Reader);
                if (attachment == null)
                    continue;

                if (attachment.Flags.HasFlag(AttachmentFlags.Retired))
                {
                    yield return attachment;
                }
                else
                {
                    if (includeStreams)
                    {
                        var stream = GetAttachmentStream(context, attachment.Base64Hash);
                        if (stream == null)
                            throw new FileNotFoundException($"Attachment's stream {attachment.Name} on {prefixSlice.ToString()} was not found. This should not happen and is likely a bug.");

                        attachment.Stream = stream;
                    }

                    yield return attachment;
                }
            }
        }

        public IEnumerable<Attachment> GetAttachmentsForDocument(DocumentsOperationContext context, AttachmentType type, LazyStringValue documentId, string changeVector)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            using (DocumentIdWorker.GetLower(context.Allocator, documentId, out var lowerDocumentIdSlice))
            using (Slice.From(context.Allocator, changeVector, out var changeVectorSlice))
            using (GetAttachmentPrefix(context, lowerDocumentIdSlice, type, type == AttachmentType.Document ? Slices.Empty : changeVectorSlice, out Slice prefixSlice))
            {
                foreach (var sr in table.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
                {
                    var attachment = TableValueToAttachment(context, ref sr.Value.Reader);
                    if (attachment == null)
                        continue;

                    yield return attachment;
                }
            }
        }

        public DynamicJsonArray GetAttachmentsMetadataForDocument(DocumentsOperationContext context, Slice lowerDocumentId)
        {
             
            var attachments = new DynamicJsonArray();
            using (GetAttachmentPrefix(context, lowerDocumentId, AttachmentType.Document, Slices.Empty, out Slice prefixSlice))
            {
                foreach (Attachment attachment in GetAttachmentsForDocument(context, prefixSlice))
                {
 
                    attachments.Add(new DynamicJsonValue
                    {
                        [nameof(AttachmentName.Name)] = attachment.Name,
                        [nameof(AttachmentName.Hash)] = attachment.Base64Hash.ToString(),
                        [nameof(AttachmentName.ContentType)] = attachment.ContentType,
                        [nameof(AttachmentName.Size)] = attachment.Size,
                        [nameof(AttachmentName.Flags)] = attachment.Flags.ToString(),
                        [nameof(AttachmentName.RetireAt)] = attachment.RetiredAt,
                        [nameof(AttachmentName.Collection)] = attachment.Collection,


                    });
                }
            }
            return attachments;
        }

        public List<AttachmentDetails> GetAttachmentDetailsForDocument(DocumentsOperationContext context, Slice lowerDocumentId)
        {
            var attachments = new List<AttachmentDetails>();
            using (GetAttachmentPrefix(context, lowerDocumentId, AttachmentType.Document, Slices.Empty, out Slice prefixSlice))
            {
                foreach (var attachment in GetAttachmentsForDocument(context, prefixSlice))
                {
                    attachments.Add(new AttachmentDetails
                    {
                        Name = attachment.Name,
                        Hash = attachment.Base64Hash.ToString(),
                        ContentType = attachment.ContentType,
                        Size = attachment.Size,
                        ChangeVector = attachment.ChangeVector,
                        DocumentId = lowerDocumentId.ToString(),
                        Flags = attachment.Flags,
                        RetireAt = attachment.RetiredAt,
                        Collection = attachment.Collection
                    });
                }
            }
            return attachments;
        }

        public DynamicJsonArray GetAttachmentsMetadataForDocument(DocumentsOperationContext context, string docId)
        {
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, docId, out var lowerDocumentId, out _))
            {
                return GetAttachmentsMetadataForDocument(context, lowerDocumentId);
            }
        }

        public (long AttachmentCount, long StreamsCount) GetNumberOfAttachments(DocumentsOperationContext context)
        {
            // We count in also revision attachments

            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            var count = table.NumberOfEntries;

            var tree = context.Transaction.InnerTransaction.CreateTree(AttachmentsSlice);
            var streamsCount = tree.State.Header.NumberOfEntries;

            return (count, streamsCount);
        }

        public Attachment GetAttachment(DocumentsOperationContext context, string documentId, string name, AttachmentType type, string changeVector,
            string hash = null, string contentType = null, bool usePartialKey = true)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Argument is null or whitespace", nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Argument is null or whitespace", nameof(name));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Get", nameof(context));
            if (type != AttachmentType.Document && string.IsNullOrWhiteSpace(changeVector))
                throw new ArgumentException($"Change Vector cannot be empty for attachment type {type}", nameof(changeVector));

            var attachment = GetAttachmentDirect(context, documentId, name, type, changeVector, hash, contentType, usePartialKey);
            if (attachment == null)
            {
                if (type == AttachmentType.Revision)
                {
                    // Return the attachment of the current document if it has the same change vector
                    var document = _documentsStorage.Get(context, documentId, throwOnConflict: false);
                    if (document != null &&
                        document.TryGetMetadata(out var metadata) &&
                        metadata.TryGet(Constants.Documents.Metadata.ChangeVector, out string exitingDocumentCv) &&
                        exitingDocumentCv == changeVector)
                    {
                        return _documentsStorage.AttachmentsStorage.GetAttachment(context, documentId, name, AttachmentType.Document, null);
                    }
                }

                return null;
            }

            if (attachment.Flags.HasFlag(AttachmentFlags.Retired))
            {
                return attachment;
            }
                var stream = GetAttachmentStream(context, attachment.Base64Hash);
            if (stream == null)
                throw new FileNotFoundException($"Attachment's stream {name} on {documentId} was not found. This should not happen and is likely a bug.");
            attachment.Stream = stream;

            return attachment;
        }

        public bool AttachmentExists(DocumentsOperationContext context, LazyStringValue hash)
        {
            using (Slice.From(context.Allocator, hash.Buffer, hash.Size, out var slice))
                return AttachmentExists(context, slice);
        }

        public bool AttachmentExists(DocumentsOperationContext context, Slice base64Hash)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(AttachmentsSlice);
            return tree.StreamExist(base64Hash);
        }

        private Attachment GetAttachmentDirect(DocumentsOperationContext context, string documentId, string name, AttachmentType type, string changeVector,
            string hash = null, string contentType = null, bool usePartialKey = true)
        {
            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice lowerId))
            using (DocumentIdWorker.GetSliceFromId(context, name, out Slice lowerName))
            {
                Slice keySlice;
                ByteStringContext<ByteStringMemoryCache>.InternalScope scope;
                if (usePartialKey)
                {
                    scope = GetAttachmentPartialKey(context, lowerId.Content.Ptr, lowerId.Size, lowerName.Content.Ptr, lowerName.Size, type, changeVector, out keySlice);
                }
                else
                {
                    using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, contentType, out Slice lowerContentType, out Slice contentTypePtr))
                    using (Slice.From(context.Allocator, hash, out Slice base64Hash))
                    {
                        scope = GetAttachmentKey(context, lowerId.Content.Ptr, lowerId.Size, lowerName.Content.Ptr, lowerName.Size, base64Hash, lowerContentType.Content.Ptr,
                            lowerContentType.Size, AttachmentType.Document, Slices.Empty, out keySlice);
                    }
                }

                using (scope)
                {
                    var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
                    if (table.SeekOnePrimaryKeyPrefix(keySlice, out TableValueReader tvr) == false)
                        return null;

                    return TableValueToAttachment(context, ref tvr);
                }
            }
        }

        public Attachment GetAttachmentByKey(DocumentsOperationContext context, Slice key)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            if (table.SeekOnePrimaryKeyPrefix(key, out TableValueReader tvr) == false)
                return null;

            return TableValueToAttachment(context, ref tvr);
        }

        public Stream GetAttachmentStream(DocumentsOperationContext context, Slice hashSlice)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(AttachmentsSlice);
            return tree.ReadStream(hashSlice);
        }

        public Stream GetAttachmentStreamByKey(DocumentsOperationContext context, Slice key)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            if (table.ReadByKey(key, out TableValueReader tvr) == false)
            {
                throw new FileNotFoundException($"Attachment's stream for key '{key}' was not found. This should not happen and is likely a bug.");
            }

            using (TableValueToSlice(context, (int)AttachmentsTable.Hash, ref tvr, out Slice existingHash))
            {
                return GetAttachmentStream(context, existingHash);
            }
        }

        public long GetAttachmentStreamLengthByKey(DocumentsOperationContext context, Slice key)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            if (table.ReadByKey(key, out TableValueReader tvr) == false)
            {
                throw new FileNotFoundException($"Attachment's stream for key '{key}' was not found. This should not happen and is likely a bug.");
            }

            using (TableValueToSlice(context, (int)AttachmentsTable.Hash, ref tvr, out Slice existingHash))
            {
                return GetAttachmentStreamLength(context, existingHash);
            }
        }

        public LazyStringValue GetAttachmentNameByKey(DocumentsOperationContext context, Slice key)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            if (table.SeekOnePrimaryKeyPrefix(key, out TableValueReader tvr) == false)
                return null;

            return TableValueToId(context, (int)AttachmentsTable.Name, ref tvr);
        }

        public Stream GetAttachmentStream(DocumentsOperationContext context, Slice hashSlice, out string tag)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(AttachmentsSlice);
            tag = tree.GetStreamTag(hashSlice);
            return tree.ReadStream(hashSlice);
        }

        public static long GetAttachmentStreamLength(DocumentsOperationContext context, Slice hashSlice)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(AttachmentsSlice);
            var info = tree.GetStreamInfo(hashSlice, false);
            if (info == null)
                return -1;
            return info->TotalSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ByteStringContext.InternalScope GetAttachmentKey(DocumentsOperationContext context, byte* lowerId, int lowerIdSize,
            byte* lowerName, int lowerNameSize, Slice base64Hash, byte* lowerContentTypePtr, int lowerContentTypeSize,
            AttachmentType type, Slice changeVector, out Slice keySlice)
        {
            return GetAttachmentKeyInternal(context, lowerId, lowerIdSize, lowerName, lowerNameSize, base64Hash, lowerContentTypePtr, lowerContentTypeSize, KeyType.Key, type, changeVector, out keySlice);
        }

        // NOTE: GetAttachmentPartialKey should be called only when the document's that hold the attachment does not have a conflict.
        // In this specific case it is ensured that we have a unique partial keys.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ByteStringContext.InternalScope GetAttachmentPartialKey(DocumentsOperationContext context, byte* lowerId, int lowerIdSize, byte
            * lowerName, int lowerNameSize, AttachmentType type, string changeVector, out Slice partialKeySlice)
        {
            Slice cvSlice;
            ByteStringContext.InternalScope cvDispose;
            if (changeVector == null)
            {
                cvSlice = Slices.Empty;
                cvDispose = default(ByteStringContext.InternalScope);
            }
            else
            {
                cvDispose = Slice.From(context.Allocator, changeVector, out cvSlice);
            }

            using (cvDispose)
            {
                return GetAttachmentKeyInternal(context, lowerId, lowerIdSize, lowerName, lowerNameSize, default(Slice), null, 0,
                    KeyType.PartialKey, type, cvSlice, out partialKeySlice);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ByteStringContext.ExternalScope GetAttachmentPartialKey(DocumentsOperationContext context, Slice key,
            int base64HashSize, int lowerContentTypeSize, out Slice partialKeySlice)
        {
            return Slice.External(context.Allocator, key.Content, 0, key.Size - base64HashSize - 1 - lowerContentTypeSize, out partialKeySlice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ByteStringContext.InternalScope GetAttachmentPrefix(DocumentsOperationContext context, byte* lowerId, int lowerIdSize, AttachmentType type, Slice changeVector, out Slice prefixSlice)
        {
            return GetAttachmentKeyInternal(context, lowerId, lowerIdSize, null, 0, default(Slice), null, 0, KeyType.Prefix, type, changeVector, out prefixSlice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ByteStringContext.InternalScope GetAttachmentPrefix(DocumentsOperationContext context, Slice lowerId, AttachmentType type, Slice changeVector, out Slice prefixSlice)
        {
            return GetAttachmentKeyInternal(context, lowerId.Content.Ptr, lowerId.Size, null, 0, default(Slice), null, 0, KeyType.Prefix, type, changeVector, out prefixSlice);
        }

        /*
        // Document key: {lowerDocumentId|d|lowerName|hash|lowerContentType}
        // Revision key: {lowerDocumentId|r|changeVector|lowerName|hash|lowerContentType}
        //
        // Document partial key: {lowerDocumentId|d|lowerName|}
        // Revision partial key: {lowerDocumentId|r|changeVector|}
        //
        // Document prefix: {lowerDocumentId|d|}
        // Revision prefix: {lowerDocumentId|r|changeVector|}
        */

        private ByteStringContext.InternalScope GetAttachmentKeyInternal(
            DocumentsOperationContext context, byte* lowerId, int lowerIdSize, byte* lowerName, int lowerNameSize,
            Slice base64Hash, byte* lowerContentTypePtr, int lowerContentTypeSize, KeyType keyType, AttachmentType type,
            Slice changeVector, out Slice keySlice)
        {
            var size = lowerIdSize + 3;
            if (type != AttachmentType.Document)
            {
                size += changeVector.Size + 1;
            }
            if (keyType == KeyType.Key)
            {
                size += lowerNameSize + 1 + base64Hash.Size + 1 + lowerContentTypeSize;
            }
            else if (keyType == KeyType.PartialKey)
            {
                size += lowerNameSize + 1;
            }

            var scope = context.Allocator.Allocate(size, out ByteString keyMem);

            Memory.Copy(keyMem.Ptr, lowerId, lowerIdSize);
            var pos = lowerIdSize;
            keyMem.Ptr[pos++] = SpecialChars.RecordSeparator;

            switch (type)
            {
                case AttachmentType.Document:
                    keyMem.Ptr[pos++] = (byte)'d';
                    break;

                case AttachmentType.Revision:
                    keyMem.Ptr[pos++] = (byte)'r';
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
            keyMem.Ptr[pos++] = SpecialChars.RecordSeparator;

            if (type != AttachmentType.Document)
            {
                Memory.Copy(keyMem.Ptr + pos, changeVector.Content.Ptr, changeVector.Size);
                pos += changeVector.Size;
                keyMem.Ptr[pos++] = SpecialChars.RecordSeparator;
            }

            if (keyType != KeyType.Prefix)
            {
                Memory.Copy(keyMem.Ptr + pos, lowerName, lowerNameSize);
                pos += lowerNameSize;
                keyMem.Ptr[pos++] = SpecialChars.RecordSeparator;

                if (keyType == KeyType.Key)
                {
                    base64Hash.CopyTo(keyMem.Ptr + pos);
                    pos += base64Hash.Size;
                    keyMem.Ptr[pos++] = SpecialChars.RecordSeparator;

                    Memory.Copy(keyMem.Ptr + pos, lowerContentTypePtr, lowerContentTypeSize);
                }
            }

            keySlice = new Slice(SliceOptions.Key, keyMem);
            return scope;
        }

        public static AttachmentType GetAttachmentTypeByKey(Slice keySlice)
        {
            var index = 0;
            var found = false;
            for (int i = 0; i < keySlice.Size; i++)
            {
                if (Convert.ToChar(keySlice[i]) == SpecialChars.RecordSeparator)
                {
                    index = i;
                    found = true;
                    break;
                }
            }

            if (found == false)
                throw new InvalidOperationException($"Could not parse {nameof(keySlice)}");

            var b = keySlice[index + 1];
            var c = Convert.ToChar(b);
            if (c == 'r')
                return AttachmentType.Revision;

            Debug.Assert(c == 'd');
            return AttachmentType.Document;
        }

        private enum KeyType
        {
            Key,
            PartialKey,
            Prefix
        }

        public static Attachment TableValueToAttachment(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            var result = new Attachment
            {
                StorageId = tvr.Id,
                Key = TableValueToString(context, (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType, ref tvr),
                Etag = TableValueToEtag((int)AttachmentsTable.Etag, ref tvr),
                ChangeVector = TableValueToChangeVector(context, (int)AttachmentsTable.ChangeVector, ref tvr),
                Name = TableValueToId(context, (int)AttachmentsTable.Name, ref tvr),
                ContentType = TableValueToId(context, (int)AttachmentsTable.ContentType, ref tvr),
                Size = TableValueToLong((int)AttachmentsTable.Size, ref tvr),
                Flags = TableValueToAttachmentFlags((int)AttachmentsTable.Flags, ref tvr),
                RetiredAt = TableValueToNullableDateTime((int)AttachmentsTable.RetireAt, ref tvr),
                Collection = TableValueToId(context, (int)AttachmentsTable.Collection, ref tvr)
            };

            TableValueToSlice(context, (int)AttachmentsTable.Hash, ref tvr, out result.Base64Hash);

            result.TransactionMarker = *(short*)tvr.Read((int)AttachmentsTable.TransactionMarker, out int _);

            return result;
        }

        [DoesNotReturn]
        private static void ThrowMissingAttachment((LazyStringValue DocId, LazyStringValue AttachmentName) details)
        {
            throw new FileNotFoundException($"Attachment's stream for file '{details.AttachmentName}' in document '{details.DocId}' was not found. This should never happen.");
        }

        [DoesNotReturn]
        private static void ThrowConcurrentException(string documentId, string name, string expectedChangeVector, string oldChangeVector)
        {
            throw new ConcurrencyException(
                $"Attachment {name} of '{documentId}' has change vector {oldChangeVector}, but Put was called with {(expectedChangeVector.Length == 0 ? "expecting new document" : "change vector " + expectedChangeVector)}. Optimistic concurrency violation, transaction will be aborted.")
            {
                Id = documentId,
                ActualChangeVector = oldChangeVector,
                ExpectedChangeVector = expectedChangeVector
            };
        }

        [DoesNotReturn]
        private static void ThrowConcurrentExceptionOnMissingAttachment(string documentId, string name, string expectedChangeVector)
        {
            throw new ConcurrencyException(
                $"Attachment {name} of '{documentId}' does not exist, but Put was called with change vector '{expectedChangeVector}'. Optimistic concurrency violation, transaction will be aborted.")
            {
                Id = documentId,
                ExpectedChangeVector = expectedChangeVector
            };
        }

        public AttachmentDetailsServer CopyAttachment(DocumentsOperationContext context, string documentId, string name, string destinationId, string destinationName, LazyStringValue changeVector, AttachmentType attachmentType, bool extractCollectionName = false)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Argument cannot be null or whitespace.", nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Argument cannot be null or whitespace.", nameof(name));
            if (string.IsNullOrWhiteSpace(destinationId))
                throw new ArgumentException("Argument cannot be null or whitespace.", nameof(destinationId));
            if (string.IsNullOrWhiteSpace(destinationName))
                throw new ArgumentException("Argument cannot be null or whitespace.", nameof(destinationName));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Copy", nameof(context));

            var attachment = GetAttachment(context, documentId, name, attachmentType, changeVector);
            if (attachment == null)
                AttachmentDoesNotExistException.ThrowFor(documentId, name);

            var hash = attachment.Base64Hash.ToString();
            return PutAttachment(context, destinationId, destinationName, attachment.ContentType, hash, attachment.Flags, attachment.Size, attachment.RetiredAt, string.Empty, attachment.Stream, extractCollectionName: extractCollectionName);
        }

        public MoveAttachmentDetailsServer MoveAttachment(DocumentsOperationContext context, string sourceDocumentId, string sourceName, string destinationDocumentId, string destinationName, LazyStringValue changeVector, string hash = null, string contentType = null, bool usePartialKey = true, bool updateDocument = true, bool extractCollectionName = false)
        {
            if (string.IsNullOrWhiteSpace(sourceDocumentId))
                throw new ArgumentException("Argument cannot be null or whitespace.", nameof(sourceDocumentId));
            if (string.IsNullOrWhiteSpace(sourceName))
                throw new ArgumentException("Argument cannot be null or whitespace.", nameof(sourceName));
            if (string.IsNullOrWhiteSpace(destinationDocumentId))
                throw new ArgumentException("Argument cannot be null or whitespace.", nameof(destinationDocumentId));
            if (string.IsNullOrWhiteSpace(destinationName))
                throw new ArgumentException("Argument cannot be null or whitespace.", nameof(destinationName));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Rename", nameof(context));

            var attachment = GetAttachment(context, sourceDocumentId, sourceName, AttachmentType.Document, changeVector, hash, contentType, usePartialKey);
            if (attachment == null)
                AttachmentDoesNotExistException.ThrowFor(sourceDocumentId, sourceName);
            //TODO: egor can I do this for retired?
            var result = PutAttachment(context, destinationDocumentId, destinationName, attachment.ContentType, attachment.Base64Hash.ToString(), attachment.Flags,attachment.Size, retireAtDt: null, string.Empty, attachment.Stream, extractCollectionName: extractCollectionName);
            DeleteAttachment(context, sourceDocumentId, sourceName, changeVector, out var sourceCollectionName, updateDocument, hash, contentType, usePartialKey, extractCollectionName: extractCollectionName);

            return new MoveAttachmentDetailsServer()
            {
                Result = result,
                DestinationCollectionName = result.CollectionName,
                SourceCollectionName = sourceCollectionName
            };
        }

        public string ResolveAttachmentName(DocumentsOperationContext context, Slice lowerId, string name)
        {
            const string prefix = "RESOLVED";
            var count = 0;
            string newName = $"{prefix}_#{count}_{name}";

            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            while (true)
            {
                using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, newName, out Slice lowerName, out _))
                using (GetAttachmentPartialKey(context, lowerId.Content.Ptr, lowerId.Size, lowerName.Content.Ptr, lowerName.Size, AttachmentType.Document, changeVector: null, out Slice partialKeySlice))
                {
                    if (table.SeekOnePrimaryKeyPrefix(partialKeySlice, out _) == false)
                        break;

                    newName = $"{prefix}_#{++count}_{name}";
                }
            }

            return newName;
        }

        public void DeleteAttachment(DocumentsOperationContext context, string documentId, string name, LazyStringValue expectedChangeVector, out CollectionName collectionName, bool updateDocument = true,
            string hash = null, string contentType = null, bool usePartialKey = true, bool extractCollectionName = false, bool storageOnly = false)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Argument is null or whitespace", nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Argument is null or whitespace", nameof(name));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Get", nameof(context));

            collectionName = null;
            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice lowerDocumentId))
            {
                var hasDoc = TryGetDocumentTableValueReaderForAttachment(context, documentId, name, lowerDocumentId, out TableValueReader docTvr);
                if (hasDoc == false)
                {
                    if (expectedChangeVector != null)
                        throw new ConcurrencyException($"Document {documentId} does not exist, " +
                                                       $"but delete was called with change vector '{expectedChangeVector}' to remove attachment {name}. " +
                                                       "Optimistic concurrency violation, transaction will be aborted.")
                        {
                            Id = documentId,
                            ExpectedChangeVector = expectedChangeVector
                        };

                    // this basically mean that we tried to delete attachment whose document doesn't exist.
                    return;
                }

                var tombstoneEtag = _documentsStorage.GenerateNextEtag();
                var changeVector = _documentsStorage.GetNewChangeVector(context, tombstoneEtag);
                context.LastDatabaseChangeVector = changeVector;

                using (DocumentIdWorker.GetSliceFromId(context, name, out Slice lowerName))
                {
                    Slice keySlice;
                    ByteStringContext<ByteStringMemoryCache>.InternalScope scope;
                    if (usePartialKey)
                    {
                        scope = GetAttachmentPartialKey(context, lowerDocumentId.Content.Ptr, lowerDocumentId.Size, lowerName.Content.Ptr, lowerName.Size, AttachmentType.Document, null, out keySlice);
                    }
                    else
                    {
                        using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, contentType, out Slice lowerContentType, out Slice contentTypePtr))
                        using (Slice.From(context.Allocator, hash, out Slice base64Hash))
                        {
                            scope = GetAttachmentKey(context, lowerDocumentId.Content.Ptr, lowerDocumentId.Size, lowerName.Content.Ptr, lowerName.Size, base64Hash, lowerContentType.Content.Ptr,
                                lowerContentType.Size, AttachmentType.Document, Slices.Empty, out keySlice);
                        }
                    }

                    using (scope)
                    {
                        var lastModifiedTicks = _documentDatabase.Time.GetUtcNow().Ticks;
                        var collection = GetDocumentCollectionName(context, docTvr);
                        DeleteAttachmentDirect(context, keySlice, usePartialKey, name, expectedChangeVector, changeVector, lastModifiedTicks, storageOnly);
                    }
                }

                if (updateDocument)
                    UpdateDocumentAfterAttachmentChange(context, lowerDocumentId, documentId, docTvr, changeVector, extractCollectionName: extractCollectionName, out collectionName);
                else if (extractCollectionName)
                    collectionName = GetDocumentCollectionName(context, docTvr);
            }
        }

        public void DeleteAttachmentConflicts(DocumentsOperationContext context, Slice lowerId, BlittableJsonReaderObject document,
            BlittableJsonReaderObject conflictDocument, string changeVector)
        {
            if (conflictDocument.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject conflictMetadata) == false ||
                conflictMetadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray conflictAttachments) == false)
                return;

            if (document == null || document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
            {
                attachments = null;
            }
            var collection = _documentsStorage.ExtractCollectionName(context, document);
            foreach (BlittableJsonReaderObject conflictAttachment in conflictAttachments)
            {
                if (conflictAttachment.TryGet(nameof(AttachmentName.Name), out LazyStringValue conflictName) == false ||
                    conflictAttachment.TryGet(nameof(AttachmentName.ContentType), out LazyStringValue conflictContentType) == false ||
                    conflictAttachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue conflictHash) == false)
                {
                    Debug.Assert(false, "Should never happen.");
                    continue;
                }

                var attachmentFoundInResolveDocument = false;
                if (attachments != null)
                {
                    foreach (BlittableJsonReaderObject attachment in attachments)
                    {
                        if (attachment.TryGet(nameof(AttachmentName.Name), out LazyStringValue name) == false ||
                            attachment.TryGet(nameof(AttachmentName.ContentType), out LazyStringValue contentType) == false ||
                            attachment.TryGet(nameof(AttachmentName.Hash), out LazyStringValue hash) == false)
                        {
                            Debug.Assert(false, "Should never happen.");
                            continue;
                        }

                        if (conflictName.Equals(name) &&
                            conflictContentType.Equals(contentType) &&
                            conflictHash.Equals(hash))
                        {
                            attachmentFoundInResolveDocument = true;
                            break;
                        }
                    }
                }

                if (attachmentFoundInResolveDocument == false)
                    DeleteAttachmentDirect(context, lowerId, conflictName, conflictContentType, conflictHash, changeVector);
            }
        }

        private void DeleteAttachmentDirect(DocumentsOperationContext context, Slice lowerId, LazyStringValue conflictName,
            LazyStringValue conflictContentType, LazyStringValue conflictHash, string changeVector)
        {
            using (DocumentIdWorker.GetSliceFromId(context, conflictName, out Slice lowerName))
            using (DocumentIdWorker.GetSliceFromId(context, conflictContentType, out Slice lowerContentType))
            using (Slice.External(context.Allocator, conflictHash, out Slice base64Hash))
            using (_documentsStorage.AttachmentsStorage.GetAttachmentKey(context, lowerId.Content.Ptr, lowerId.Size,
                lowerName.Content.Ptr, lowerName.Size,
                base64Hash, lowerContentType.Content.Ptr, lowerContentType.Size, AttachmentType.Document, Slices.Empty, out Slice keySlice))
            {
                var lastModifiedTicks = _documentDatabase.Time.GetUtcNow().Ticks;
                DeleteAttachmentDirect(context, keySlice, false, null, null, changeVector, lastModifiedTicks, storageOnly: true);
            }
        }

        public Tombstone GetAttachmentTombstoneByKey(DocumentsOperationContext context, Slice key)
        {
            var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(_documentDatabase.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);
            if (tombstoneTable.ReadByKey(key, out var tvr))
            {
                var tombstone = TableValueToTombstone(context, ref tvr);
                if (tombstone.Type != Tombstone.TombstoneType.Attachment)
                {
                    Debug.Assert(false, "Tombstone must be of type attachment");
                    return null;
                }
                return tombstone;
            }

            return null;
        }

        public void DeleteAttachmentDirect(DocumentsOperationContext context, Slice key, bool isPartialKey, string name,
            string expectedChangeVector, string changeVector, long lastModifiedTicks, bool storageOnly = false)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            if (isPartialKey ?
                table.SeekOnePrimaryKeyPrefix(key, out TableValueReader tvr) == false :
                table.ReadByKey(key, out tvr) == false)
            {
                if (expectedChangeVector != null)
                    throw new ConcurrencyException($"Attachment {name} with key '{key}' does not exist, " +
                                                   $"but delete was called with change vector '{expectedChangeVector}'. " +
                                                   "Optimistic concurrency violation, transaction will be aborted.")
                    {
                        ExpectedChangeVector = expectedChangeVector
                    };

                // This basically means that we tried to delete attachment that doesn't exist.
                long attachmentEtag;
                var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(_documentDatabase.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);
                if (tombstoneTable.ReadByKey(key, out var existingTombstone))
                {
                    attachmentEtag = TableValueToEtag((int)TombstoneTable.Etag, ref existingTombstone);
                    tombstoneTable.Delete(existingTombstone.Id);
                }
                else
                {
                    // We'll create a tombstones just to make sure that it would replicate the delete.
                    attachmentEtag = _documentsStorage.GenerateNextEtagForReplicatedTombstoneMissingDocument(context);
                }

                CreateTombstone(context, key, attachmentEtag, changeVector, lastModifiedTicks, (int)DocumentFlags.None);
                return;
            }

            var currentChangeVector = TableValueToChangeVector(context, (int)AttachmentsTable.ChangeVector, ref tvr);
            var etag = TableValueToEtag((int)AttachmentsTable.Etag, ref tvr);
            using (isPartialKey ?
                TableValueToSlice(context, (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType, ref tvr, out key)
              : default(ByteStringContext.InternalScope))
            using (TableValueToSlice(context, (int)AttachmentsTable.Hash, ref tvr, out Slice hash))
            {
                if (expectedChangeVector != null && ChangeVector.CompareVersion(currentChangeVector, expectedChangeVector, context) != 0)
                {
                    throw new ConcurrencyException($"Attachment {name} with key '{key}' has change vector '{currentChangeVector}', but Delete was called with change vector '{expectedChangeVector}'. " +
                                                   "Optimistic concurrency violation, transaction will be aborted.")
                    {
                        ActualChangeVector = currentChangeVector,
                        ExpectedChangeVector = expectedChangeVector
                    };
                }

                var attachmentFlags = TableValueToAttachmentFlags((int)AttachmentsTable.Flags, ref tvr);
                var retireAtTicks = TableValueToLong((int)AttachmentsTable.RetireAt, ref tvr);
                var collection = TableValueToId(context, (int)AttachmentsTable.Collection, ref tvr);
                DeleteInternal(context, key, etag, hash, changeVector, lastModifiedTicks, flags: DocumentFlags.None, attachmentFlags, retireAtTicks, collection, storageOnly);
            }

            table.Delete(tvr.Id);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DeleteInternal(DocumentsOperationContext context, Slice key, long etag, Slice hash,
            string changeVector, long lastModifiedTicks, DocumentFlags flags, AttachmentFlags attachmentFlags, long retireAtTicks, string collection, bool storageOnly = false)
        {
            if (attachmentFlags.HasFlag(AttachmentFlags.Retired))
            {
                if (storageOnly == false)
                {
                    // populate retired tree
                    TryDeleteRetiredAttachment(context, key, collection);
                }
                else
                {
                   RetiredAttachmentsStorage.RemoveRetirePutValue(context, key, retireAtTicks);

                    // we create attachment tombstone with special flag to mark that we don't want to delete the attachment from cloud
                    CreateTombstone(context, key, etag, changeVector, lastModifiedTicks, (int)AttachmentTombstoneFlags.FromStorageOnly);
                }
            }
            else
            {
                CreateTombstone(context, key, etag, changeVector, lastModifiedTicks, (int)flags);

                if (retireAtTicks != -1)
                {
                    RetiredAttachmentsStorage.RemoveRetirePutValue(context, key, retireAtTicks);
                }

                // We may have another operation in the same transaction that would cause us to re-create
                // the missing references, let's move the actual stream delete to the end of the transaction
                context.Transaction.CheckIfShouldDeleteAttachmentStream(hash);
            }
        }

        private void DeleteTombstoneIfNeeded(DocumentsOperationContext context, Slice keySlice)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(_documentDatabase.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);
            table.DeleteByKey(keySlice);
        }

        private void CreateTombstone(DocumentsOperationContext context, Slice keySlice, long attachmentEtag,
            string changeVector, long lastModifiedTicks, int flags)
        {
            var newEtag = _documentsStorage.GenerateNextEtag();

            var table = context.Transaction.InnerTransaction.OpenTable(_documentDatabase.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);

            if (table.VerifyKeyExists(keySlice))
                return; // attachments (and attachment tombstones) are immutable, we can safely ignore this

            using (table.Allocate(out TableValueBuilder tvb))
            using (Slice.From(context.Allocator, changeVector, out var cv))
            {
                tvb.Add(keySlice.Content.Ptr, keySlice.Size);
                tvb.Add(Bits.SwapBytes(newEtag));
                tvb.Add(Bits.SwapBytes(attachmentEtag));
                tvb.Add(context.GetTransactionMarker());
                tvb.Add((byte)Tombstone.TombstoneType.Attachment);
                tvb.Add(null, 0);
                tvb.Add(flags);
                tvb.Add(cv.Content.Ptr, cv.Size);
                tvb.Add(lastModifiedTicks);
                table.Insert(tvb);
            }
        }

        private void DeleteAttachmentsOfDocumentInternal(DocumentsOperationContext context, Slice prefixSlice, string changeVector,
            long lastModifiedTicks, DocumentFlags flags = DocumentFlags.None, bool storageOnly = false)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            {
                table.DeleteByPrimaryKeyPrefix(prefixSlice, before =>
                {
                    using (TableValueToSlice(context, (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType, ref before.Reader, out Slice key))
                    using (TableValueToSlice(context, (int)AttachmentsTable.Hash, ref before.Reader, out Slice hash))
                    {
                        var etag = TableValueToEtag((int)AttachmentsTable.Etag, ref before.Reader);

                        var attachmentFlags = TableValueToAttachmentFlags((int)AttachmentsTable.Flags, ref before.Reader);
                        var retireAtTicks = TableValueToLong((int)AttachmentsTable.RetireAt, ref before.Reader);

                        // TODO: egor organize this (its checked 2nd time inside DeleteInternal)
                        if (storageOnly == false)
                        {
                            var dbRecord = _documentDatabase.ReadDatabaseRecord();
                            if (dbRecord.RetiredAttachments is { Disabled: false })
                            {
                                if (dbRecord.RetiredAttachments.PurgeOnDelete == false)
                                {
                                    // we cannot delete from cloud since PurgeOnDelete is false
                                    storageOnly = true;
                                }
                            }
                            else
                            {
                                // we cannot delete from retired since there is no configuration
                                storageOnly = true;
                            }
                        }
                        else
                        {
                            // this is revision
                        }

                        //storageOnly = false;
                        var collection = TableValueToId(context, (int)AttachmentsTable.Collection, ref before.Reader);
                        DeleteInternal(context, key, etag, hash, changeVector, lastModifiedTicks, flags, attachmentFlags, retireAtTicks, collection, storageOnly);
                    }
                });
            }
        }

        public void DeleteRevisionAttachments(DocumentsOperationContext context, Document revision, ChangeVector changeVector, long lastModifiedTicks, DocumentFlags flags = DocumentFlags.None)
        {
            using (Slice.From(context.Allocator, revision.ChangeVector, out Slice changeVectorSlice))
            using (GetAttachmentPrefix(context, revision.LowerId.Buffer, revision.LowerId.Size, AttachmentType.Revision, changeVectorSlice, out Slice prefixSlice))
            {
                DeleteAttachmentsOfDocumentInternal(context, prefixSlice, changeVector.Version, lastModifiedTicks, flags, storageOnly: true);
            }
        }

        public void DeleteAttachmentsOfDocument(DocumentsOperationContext context, Slice lowerId, string changeVector,
            long lastModifiedTicks, DocumentFlags flags = DocumentFlags.None)
        {
            using (GetAttachmentPrefix(context, lowerId.Content.Ptr, lowerId.Size, AttachmentType.Document, Slices.Empty, out Slice prefixSlice))
            {
                DeleteAttachmentsOfDocumentInternal(context, prefixSlice, changeVector, lastModifiedTicks, flags);
            }
        }

        public StreamsTempFile GetTempFile(string prefix)
        {
            var name = $"attachment.{Guid.NewGuid():N}.{prefix}";
            var tempPath = _documentsStorage.Environment.Options.DataPager.Options.TempPath.Combine(name);

            return new StreamsTempFile(tempPath.FullPath, _documentDatabase.DocumentsStorage.Environment.Options.Encryption.IsEnabled);
        }

        public static (LazyStringValue DocId, LazyStringValue AttachmentName) GetDocIdAndAttachmentName(JsonOperationContext context,
            LazyStringValue attachmentKey)
        {
            var p = attachmentKey.Buffer;
            var size = attachmentKey.Size;

            ExtractDocIdAndAttachmentNameFromTombstone(p, size, out int sizeOfDocId, out int attachmentNameIndex, out int sizeOfAttachmentName);

            var doc = context.AllocateStringValue(null, p, sizeOfDocId);
            var name = context.AllocateStringValue(null, p + attachmentNameIndex, sizeOfAttachmentName);

            return (doc, name);
        }

        public static (string DocId, string AttachmentName) ExtractDocIdAndAttachmentNameFromTombstone(Slice attachmentTombstoneId)
        {
            var p = attachmentTombstoneId.Content.Ptr;
            var size = attachmentTombstoneId.Size;

            ExtractDocIdAndAttachmentNameFromTombstone(p, size, out int sizeOfDocId, out int attachmentNameIndex, out int sizeOfAttachmentName);

            var doc = Encodings.Utf8.GetString(p, sizeOfDocId);
            var name = Encodings.Utf8.GetString(p + attachmentNameIndex, sizeOfAttachmentName);

            return (doc, name);
        }


        public static int GetSizeOfDocId(ReadOnlySpan<byte> key)
        {
            int sizeOfDocId = 0;
            for (; sizeOfDocId < key.Length; sizeOfDocId++)
            {
                if (key[sizeOfDocId] == SpecialChars.RecordSeparator)
                    break;
            }

            return sizeOfDocId;
        }

        private static void ExtractDocIdAndAttachmentNameFromTombstone(byte* p, int size, out int sizeOfDocId, out int attachmentNameIndex, out int sizeOfAttachmentName)
        {
            sizeOfDocId = GetSizeOfDocId(new ReadOnlySpan<byte>(p, size));

            attachmentNameIndex = sizeOfDocId +
                                  1 + // separator
                                  1 + // type: d
                                  1;

            sizeOfAttachmentName = 0;

            for (; sizeOfAttachmentName < size - (sizeOfDocId + 3); sizeOfAttachmentName++)
            {
                if (p[attachmentNameIndex + sizeOfAttachmentName] == SpecialChars.RecordSeparator)
                    break;
            }
        }

        public static IEnumerable<BlittableJsonReaderObject> GetAttachmentsFromDocumentMetadata(BlittableJsonReaderObject document)
        {
            if (document.TryGet(Raven.Client.Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                metadata.TryGet(Raven.Client.Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments))
            {
                foreach (BlittableJsonReaderObject attachment in attachments)
                {
                    yield return attachment;
                }
            }
        }

        public void RemoveAttachmentStreamsWithoutReferences(DocumentsOperationContext context, List<Slice> attachmentHashesToMaybeDelete)
        {
            foreach (var hash in attachmentHashesToMaybeDelete)
                DeleteAttachmentStream(context, hash);
        }
    }
}
