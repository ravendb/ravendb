using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.DocumentsStorage;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents
{
    public unsafe class AttachmentsStorage
    {
        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;

        internal static readonly Slice AttachmentsSlice;
        internal static readonly Slice AttachmentsMetadataSlice;
        public static readonly Slice AttachmentsEtagSlice;
        private static readonly Slice AttachmentsHashSlice;
        private static readonly Slice AttachmentsTombstonesSlice;

        internal static readonly TableSchema AttachmentsSchema = new TableSchema();
        public static readonly string AttachmentsTombstones = "Attachments.Tombstones";


        private enum AttachmentsTable
        {
            /* AND is a record separator. 
             * We are you using the record separator in order to avoid loading another files that has the same key prefix, 
                e.g. fitz(record-separator)profile.png and fitz0(record-separator)profile.png, without the record separator we would have to load also fitz0 and filter it. */
            LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType = 0,
            Etag = 1,
            Name = 2, // format of lazy string key is detailed in GetLowerIdSliceAndStorageKey
            ContentType = 3, // format of lazy string key is detailed in GetLowerIdSliceAndStorageKey
            Hash = 4, // base64 hash
            TransactionMarker = 5,
            ChangeVector = 6
        }

        static AttachmentsStorage()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Attachments", ByteStringType.Immutable, out AttachmentsSlice);
                Slice.From(ctx, "AttachmentsMetadata", ByteStringType.Immutable, out AttachmentsMetadataSlice);
                Slice.From(ctx, "AttachmentsEtag", ByteStringType.Immutable, out AttachmentsEtagSlice);
                Slice.From(ctx, "AttachmentsHash", ByteStringType.Immutable, out AttachmentsHashSlice);
                Slice.From(ctx, AttachmentsTombstones, ByteStringType.Immutable, out AttachmentsTombstonesSlice);
            }

            AttachmentsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType,
                Count = 1
            });
            AttachmentsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)AttachmentsTable.Etag,
                Name = AttachmentsEtagSlice
            });
            AttachmentsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)AttachmentsTable.Hash,
                Count = 1,
                Name = AttachmentsHashSlice
            });
        }

        public AttachmentsStorage(DocumentDatabase documentDatabase, Transaction tx)
        {
            _documentDatabase = documentDatabase;
            _documentsStorage = documentDatabase.DocumentsStorage;
            LoggingSource.Instance.GetLogger<AttachmentsStorage>(documentDatabase.Name);

            tx.CreateTree(AttachmentsSlice);
            AttachmentsSchema.Create(tx, AttachmentsMetadataSlice, 44);
            TombstonesSchema.Create(tx, AttachmentsTombstonesSlice, 16);

        }
        public static long ReadLastEtag(Transaction tx)
        {
            var table = tx.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            var last = table.ReadLast(AttachmentsSchema.FixedSizeIndexes[AttachmentsEtagSlice]);
            if (last == null)
                return 0;

            return TableValueToEtag((int)AttachmentsTable.Etag, ref last.Reader);
        }

        public void AssertFixedSizeTrees(Transaction tx)
        {
            tx.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice).AssertValidFixedSizeTrees();

            TombstonesSchema.Create(tx, AttachmentsTombstonesSlice, 16);
            tx.OpenTable(TombstonesSchema, AttachmentsTombstonesSlice).AssertValidFixedSizeTrees();
        }

        public IEnumerable<ReplicationBatchItem> GetAttachmentsFrom(DocumentsOperationContext context, long etag)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            foreach (var result in table.SeekForwardFrom(AttachmentsSchema.FixedSizeIndexes[AttachmentsEtagSlice], etag, 0))
            {
                var attachment = TableValueToAttachment(context, ref result.Reader);

                var stream = GetAttachmentStream(context, attachment.Base64Hash);
                if (stream == null)
                    ThrowMissingAttachment(attachment.Name);

                attachment.Stream = stream;

                yield return AttachmentReplicationItem.From(context, attachment);
            }
        }

        public static long GetCountOfAttachmentsForHash(DocumentsOperationContext context, Slice hash)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            return table.GetCountOfMatchesFor(AttachmentsSchema.Indexes[AttachmentsHashSlice], hash);
        }

        public IEnumerable<DynamicJsonValue> GetAttachmentsMetadataForDocumentWithCounts(DocumentsOperationContext context, string lowerDocumentId)
        {
            using (Slice.From(context.Allocator, lowerDocumentId, out Slice lowerDocumentIdSlice))
            using (GetAttachmentPrefix(context, lowerDocumentIdSlice, AttachmentType.Document, Slices.Empty, out Slice prefixSlice))
            {
                foreach (var attachment in GetAttachmentsForDocument(context, prefixSlice))
                {
                    yield return new DynamicJsonValue
                    {
                        [nameof(AttachmentName.Name)] = attachment.Name,
                        [nameof(AttachmentName.Hash)] = attachment.Base64Hash.ToString(),
                        [nameof(AttachmentName.ContentType)] = attachment.ContentType,
                        [nameof(AttachmentName.Size)] = attachment.Size,
                        ["Count"] = GetCountOfAttachmentsForHash(context, attachment.Base64Hash)
                    };
                }
            }
        }

        public AttachmentDetails PutAttachment(DocumentsOperationContext context, string documentId, string name, string contentType,
            string hash, string expectedChangeVector, Stream stream, bool updateDocument = true)
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
                // This will validate that we cannot put an attachment on a conflicted document
                var hasDoc = TryGetDocumentTableValueReaderForAttachment(context, documentId, name, lowerDocumentId, out TableValueReader tvr);
                if (hasDoc == false)
                    throw new InvalidOperationException($"Cannot put attachment {name} on a non existent document '{documentId}'.");
                if (TableValueToFlags((int)DocumentsTable.Flags, ref tvr).HasFlag(DocumentFlags.Artificial))
                    throw new InvalidOperationException($"Cannot put attachment {name} on artificial document '{documentId}'.");

                using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, name, out Slice lowerName, out Slice namePtr))
                using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, contentType, out Slice lowerContentType, out Slice contentTypePtr))
                using (Slice.From(context.Allocator, hash, out Slice base64Hash)) // Hash is a base64 string, so this is a special case that we do not need to escape
                using (GetAttachmentKey(context, lowerDocumentId.Content.Ptr, lowerDocumentId.Size, lowerName.Content.Ptr, lowerName.Size, base64Hash,
                    lowerContentType.Content.Ptr, lowerContentType.Size, AttachmentType.Document, Slices.Empty, out Slice keySlice))
                {
                    Debug.Assert(base64Hash.Size == 44, $"Hash size should be 44 but was: {keySlice.Size}");

                    DeleteTombstoneIfNeeded(context, keySlice);

                    var changeVector = _documentsStorage.GetNewChangeVector(context, attachmentEtag);
                    Debug.Assert(changeVector != null);

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
                    }

                    if (table.ReadByKey(keySlice, out TableValueReader oldValue))
                    {
                        // This is an update to the attachment with the same stream and content type
                        // Just updating the etag and casing of the name and the content type.

                        if (expectedChangeVector != null)
                        {
                            var oldChangeVector = TableValueToChangeVector(context, (int)AttachmentsTable.ChangeVector, ref oldValue);
                            if (string.Compare(expectedChangeVector, oldChangeVector, StringComparison.Ordinal) != 0)
                                ThrowConcurrentException(documentId, name, expectedChangeVector, oldChangeVector);
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

                        // We already asserted that the document is not in conflict, so we might have just one partial key, not more.
                        using (GetAttachmentPartialKey(context, keySlice, base64Hash.Size, lowerContentType.Size, out Slice partialKeySlice))
                        {
                            if (table.SeekOnePrimaryKeyPrefix(partialKeySlice, out TableValueReader partialTvr))
                            {
                                if (expectedChangeVector != null)
                                {
                                    var oldChangeVector = TableValueToChangeVector(context, (int)AttachmentsTable.ChangeVector, ref partialTvr);
                                    if (string.Compare(expectedChangeVector, oldChangeVector, StringComparison.Ordinal) != 0)
                                        ThrowConcurrentException(documentId, name, expectedChangeVector, oldChangeVector);
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
                                            DeleteInternal(context, existingKey, existingEtag, existingHash, changeVector, lastModifiedTicks);
                                        }
                                    }
                                }

                                table.Delete(partialTvr.Id);
                            }
                        }

                        if (string.IsNullOrEmpty(expectedChangeVector) == false)
                        {
                            ThrowConcurrentExceptionOnMissingAttachment(documentId, name, expectedChangeVector);
                        }

                        using (Slice.From(context.Allocator, changeVector, out var changeVectorSlice))
                        using (table.Allocate(out TableValueBuilder tvb))
                        {
                            SetTableValue(tvb, changeVectorSlice);
                            table.Insert(tvb);
                        }

                        if (putStream)
                        {
                            PutAttachmentStream(context, keySlice, base64Hash, stream);
                        }
                    }

                    _documentDatabase.Metrics.Attachments.PutsPerSec.MarkSingleThreaded(1);

                    if (updateDocument)
                        UpdateDocumentAfterAttachmentChange(context, lowerDocumentId, documentId, tvr, changeVector);

                    return new AttachmentDetails
                    {
                        ChangeVector = changeVector,
                        ContentType = contentType,
                        Name = name,
                        DocumentId = documentId,
                        Hash = hash,
                        Size = stream.Length
                    };
                }
            }
        }

        /// <summary>
        /// Should be used only from replication or smuggler.
        /// </summary>
        public void PutDirect(DocumentsOperationContext context, Slice key, Slice name, Slice contentType, Slice base64Hash, string changeVector = null)
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

                table.Set(tvb);
            }

            _documentDatabase.Metrics.Attachments.PutsPerSec.MarkSingleThreaded(1);
        }

        /// <summary>
        /// Update the document with an etag which is bigger than the attachmentEtag
        /// We need to call this after we already put the attachment, so it can version also this attachment
        /// </summary>
        private string UpdateDocumentAfterAttachmentChange(DocumentsOperationContext context, Slice lowerDocumentId, string documentId,
            TableValueReader tvr, string changeVector)
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

                var flags = TableValueToFlags((int)DocumentsTable.Flags, ref copyTvr);
                flags = flags.Strip(DocumentFlags.FromClusterTransaction | DocumentFlags.Resolved);

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

                data = context.ReadObject(data, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                return _documentsStorage.Put(context, documentId, null, data, null, null, flags, NonPersistentDocumentFlags.ByAttachmentUpdate).ChangeVector;
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
                return UpdateDocumentAfterAttachmentChange(context, lowerDocumentId, documentId, tvr, null);
            }
        }

        public void RevisionAttachments(DocumentsOperationContext context, Slice lowerId, Slice changeVector)
        {
            using (GetAttachmentPrefix(context, lowerId.Content.Ptr, lowerId.Size, AttachmentType.Document, Slices.Empty, out Slice prefixSlice))
            {
                var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
                var currentAttachments = new List<(LazyStringValue name, LazyStringValue contentType, Slice base64Hash)>();
                foreach (var sr in table.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
                {
                    var tableValueHolder = sr.Value;
                    var name = TableValueToId(context, (int)AttachmentsTable.Name, ref tableValueHolder.Reader);
                    var contentType = TableValueToId(context, (int)AttachmentsTable.ContentType, ref tableValueHolder.Reader);

                    var ptr = tableValueHolder.Reader.Read((int)AttachmentsTable.Hash, out int size);
                    Slice.From(context.Allocator, ptr, size, out Slice base64Hash);

                    currentAttachments.Add((name, contentType, base64Hash));
                }
                foreach (var attachment in currentAttachments)
                {
                    PutRevisionAttachment(context, lowerId.Content.Ptr, lowerId.Size, changeVector, attachment);
                    attachment.name.Dispose();
                    attachment.contentType.Dispose();
                    attachment.base64Hash.Release(context.Allocator);
                }
            }
        }

        private void PutRevisionAttachment(DocumentsOperationContext context, byte* lowerId, int lowerIdSize, Slice changeVector,
            (LazyStringValue Name, LazyStringValue ContentType, Slice Base64Hash) attachment)
        {
            var attachmentEtag = _documentsStorage.GenerateNextEtag();

            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, attachment.Name, out Slice lowerName, out Slice namePtr))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, attachment.ContentType, out Slice lowerContentType, out Slice contentTypePtr))
            using (GetAttachmentKey(context, lowerId, lowerIdSize, lowerName.Content.Ptr, lowerName.Size, attachment.Base64Hash,
                lowerContentType.Content.Ptr, lowerContentType.Size, AttachmentType.Revision, changeVector, out Slice keySlice))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(keySlice.Content.Ptr, keySlice.Size);
                tvb.Add(Bits.SwapBytes(attachmentEtag));
                tvb.Add(namePtr);
                tvb.Add(contentTypePtr);
                tvb.Add(attachment.Base64Hash);
                tvb.Add(context.GetTransactionMarker());
                tvb.Add(changeVector.Content.Ptr, changeVector.Size);
                table.Set(tvb);
            }
        }

        public void PutAttachmentStream(DocumentsOperationContext context, Slice key, Slice base64Hash, Stream stream)
        {
            stream.Position = 0; // We might retry a merged command, so it is a safe place to reset the position here to zero.

            var tree = context.Transaction.InnerTransaction.CreateTree(AttachmentsSlice);
            var existingStream = tree.ReadStream(base64Hash);
            if (existingStream == null)
                tree.AddStream(base64Hash, stream, tag: key);

            _documentDatabase.Metrics.Attachments.BytesPutsPerSec.MarkSingleThreaded(stream.Length);
        }

        private void DeleteAttachmentStream(DocumentsOperationContext context, Slice hash, int expectedCount = 1)
        {
            if (GetCountOfAttachmentsForHash(context, hash) == expectedCount)
            {
                var tree = context.Transaction.InnerTransaction.CreateTree(AttachmentsSlice);
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

                if (includeStreams == false)
                {
                    attachment.Size = GetAttachmentStreamLength(context, attachment.Base64Hash);
                }
                else
                {
                    var stream = GetAttachmentStream(context, attachment.Base64Hash);
                    if (stream == null)
                        throw new FileNotFoundException($"Attachment's stream {attachment.Name} on {prefixSlice.ToString()} was not found. This should not happen and is likely a bug.");

                    attachment.Stream = stream;
                    attachment.Size = stream.Length;
                }

                yield return attachment;
            }
        }

        public IEnumerable<Attachment> GetAttachmentsForDocument(DocumentsOperationContext context, AttachmentType type, LazyStringValue documentId)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            using (DocumentIdWorker.GetLower(context.Allocator, documentId, out var lowerDocumentIdSlice))
            using (GetAttachmentPrefix(context, lowerDocumentIdSlice, type, Slices.Empty, out Slice prefixSlice))
            {
                foreach (var sr in table.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
                {
                    var attachment = TableValueToAttachment(context, ref sr.Value.Reader);
                    if (attachment == null)
                        continue;

                    attachment.Size = GetAttachmentStreamLength(context, attachment.Base64Hash);

                    yield return attachment;
                }
            }
        }

        public DynamicJsonArray GetAttachmentsMetadataForDocument(DocumentsOperationContext context, Slice lowerDocumentId)
        {
            var attachments = new DynamicJsonArray();
            using (GetAttachmentPrefix(context, lowerDocumentId, AttachmentType.Document, Slices.Empty, out Slice prefixSlice))
            {
                foreach (var attachment in GetAttachmentsForDocument(context, prefixSlice))
                {
                    attachments.Add(new DynamicJsonValue
                    {
                        [nameof(AttachmentName.Name)] = attachment.Name,
                        [nameof(AttachmentName.Hash)] = attachment.Base64Hash.ToString(),
                        [nameof(AttachmentName.ContentType)] = attachment.ContentType,
                        [nameof(AttachmentName.Size)] = attachment.Size
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
            var streamsCount = tree.State.NumberOfEntries;

            return (count, streamsCount);
        }

        public Attachment GetAttachment(DocumentsOperationContext context, string documentId, string name, AttachmentType type, string changeVector)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Argument is null or whitespace", nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Argument is null or whitespace", nameof(name));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Get", nameof(context));
            if (type != AttachmentType.Document && string.IsNullOrWhiteSpace(changeVector))
                throw new ArgumentException($"Change Vector cannot be empty for attachment type {type}", nameof(changeVector));

            var attachment = GetAttachmentDirect(context, documentId, name, type, changeVector);
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

        private Attachment GetAttachmentDirect(DocumentsOperationContext context, string documentId, string name, AttachmentType type, string changeVector)
        {
            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice lowerId))
            using (DocumentIdWorker.GetSliceFromId(context, name, out Slice lowerName))
            using (GetAttachmentPartialKey(context, lowerId.Content.Ptr, lowerId.Size, lowerName.Content.Ptr, lowerName.Size, type, changeVector, out Slice partialKeySlice))
            {
                var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

                if (table.SeekOnePrimaryKeyPrefix(partialKeySlice, out TableValueReader tvr) == false)
                    return null;

                return TableValueToAttachment(context, ref tvr);
            }
        }

        public Stream GetAttachmentStream(DocumentsOperationContext context, Slice hashSlice)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(AttachmentsSlice);
            return tree.ReadStream(hashSlice);
        }

        public Stream GetAttachmentStream(DocumentsOperationContext context, Slice hashSlice, out string tag)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(AttachmentsSlice);
            tag = tree.GetStreamTag(hashSlice);
            return tree.ReadStream(hashSlice);
        }

        private static long GetAttachmentStreamLength(DocumentsOperationContext context, Slice hashSlice)
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

        private enum KeyType
        {
            Key,
            PartialKey,
            Prefix
        }

        private static Attachment TableValueToAttachment(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            var result = new Attachment
            {
                StorageId = tvr.Id,
                Key = TableValueToString(context, (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType, ref tvr),
                Etag = TableValueToEtag((int)AttachmentsTable.Etag, ref tvr),
                ChangeVector = TableValueToChangeVector(context, (int)AttachmentsTable.ChangeVector, ref tvr),
                Name = TableValueToId(context, (int)AttachmentsTable.Name, ref tvr),
                ContentType = TableValueToId(context, (int)AttachmentsTable.ContentType, ref tvr)
            };

            TableValueToSlice(context, (int)AttachmentsTable.Hash, ref tvr, out result.Base64Hash);

            result.TransactionMarker = *(short*)tvr.Read((int)AttachmentsTable.TransactionMarker, out int _);

            return result;
        }

        private static void ThrowMissingAttachment(string attachmentName)
        {
            throw new FileNotFoundException($"Attachment's stream '{attachmentName}' was not found. This should never happen.");
        }

        private static void ThrowConcurrentException(string documentId, string name, string expectedChangeVector, string oldChangeVector)
        {
            throw new ConcurrencyException(
                $"Attachment {name} of '{documentId}' has change vector {oldChangeVector}, but Put was called with {(expectedChangeVector.Length == 0 ? "expecting new document" : "change vector " + expectedChangeVector)}. Optimistic concurrency violation, transaction will be aborted.")
            {
                ActualChangeVector = oldChangeVector,
                ExpectedChangeVector = expectedChangeVector
            };
        }

        private static void ThrowConcurrentExceptionOnMissingAttachment(string documentId, string name, string expectedChangeVector)
        {
            throw new ConcurrencyException(
                $"Attachment {name} of '{documentId}' does not exist, but Put was called with change vector '{expectedChangeVector}'. Optimistic concurrency violation, transaction will be aborted.")
            {
                ExpectedChangeVector = expectedChangeVector
            };
        }

        public AttachmentDetails CopyAttachment(DocumentsOperationContext context, string documentId, string name, string destinationId, string destinationName, LazyStringValue changeVector, AttachmentType attachmentType)
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
            return PutAttachment(context, destinationId, destinationName, attachment.ContentType, hash, string.Empty, attachment.Stream);
        }

        public AttachmentDetails MoveAttachment(DocumentsOperationContext context, string sourceDocumentId, string sourceName, string destinationDocumentId, string destinationName, LazyStringValue changeVector)
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

            var attachment = GetAttachment(context, sourceDocumentId, sourceName, AttachmentType.Document, changeVector);
            if (attachment == null)
                AttachmentDoesNotExistException.ThrowFor(sourceDocumentId, sourceName);

            var hash = attachment.Base64Hash.ToString();
            var result = PutAttachment(context, destinationDocumentId, destinationName, attachment.ContentType, hash, string.Empty, attachment.Stream);
            DeleteAttachment(context, sourceDocumentId, sourceName, changeVector);

            return result;
        }

        public void DeleteAttachment(DocumentsOperationContext context, string documentId, string name, LazyStringValue expectedChangeVector, bool updateDocument = true)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Argument is null or whitespace", nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Argument is null or whitespace", nameof(name));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Get", nameof(context));

            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice lowerDocumentId))
            {
                var hasDoc = TryGetDocumentTableValueReaderForAttachment(context, documentId, name, lowerDocumentId, out TableValueReader docTvr);
                if (hasDoc == false)
                {
                    if (expectedChangeVector != null)
                        throw new ConcurrencyException($"Document {documentId} does not exist, " +
                                                       $"but delete was called with change vector '{expectedChangeVector}' to remove attachment {name}. " +
                                                       "Optimistic concurrency violation, transaction will be aborted.");

                    // this basically mean that we tried to delete attachment whose document doesn't exist.
                    return;
                }

                var tombstoneEtag = _documentsStorage.GenerateNextEtag();
                var changeVector = _documentsStorage.GetNewChangeVector(context, tombstoneEtag);
                context.LastDatabaseChangeVector = changeVector;

                using (DocumentIdWorker.GetSliceFromId(context, name, out Slice lowerName))
                using (GetAttachmentPartialKey(context, lowerDocumentId.Content.Ptr, lowerDocumentId.Size, lowerName.Content.Ptr, lowerName.Size,
                    AttachmentType.Document, null, out Slice partialKeySlice))
                {
                    var lastModifiedTicks = _documentDatabase.Time.GetUtcNow().Ticks;
                    DeleteAttachmentDirect(context, partialKeySlice, true, name, expectedChangeVector, changeVector, lastModifiedTicks);
                }

                if (updateDocument)
                    UpdateDocumentAfterAttachmentChange(context, lowerDocumentId, documentId, docTvr, changeVector);
            }
        }

        public void DeleteAttachmentConflicts(DocumentsOperationContext context, Slice lowerId, BlittableJsonReaderObject document,
            BlittableJsonReaderObject conflictDocument, string changeVector)
        {
            if (conflictDocument.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject conflictMetadata) == false ||
                conflictMetadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray conflictAttachments) == false)
                return;

            if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
            {
                attachments = null;
            }

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
                DeleteAttachmentDirect(context, keySlice, false, null, null, changeVector, lastModifiedTicks);
            }
        }

        public void DeleteAttachmentDirect(DocumentsOperationContext context, Slice key, bool isPartialKey, string name,
            string expectedChangeVector, string changeVector, long lastModifiedTicks)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            if (isPartialKey ?
                table.SeekOnePrimaryKeyPrefix(key, out TableValueReader tvr) == false :
                table.ReadByKey(key, out tvr) == false)
            {
                if (expectedChangeVector != null)
                    throw new ConcurrencyException($"Attachment {name} with key '{key}' does not exist, " +
                                                   $"but delete was called with change vector '{expectedChangeVector}'. " +
                                                   "Optimistic concurrency violation, transaction will be aborted.");

                // This basically means that we tried to delete attachment that doesn't exist.
                long attachmentEtag;
                var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, AttachmentsTombstonesSlice);
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

                CreateTombstone(context, key, attachmentEtag, changeVector, lastModifiedTicks);
                return;
            }

            var currentChangeVector = TableValueToChangeVector(context, (int)AttachmentsTable.ChangeVector, ref tvr);
            var etag = TableValueToEtag((int)AttachmentsTable.Etag, ref tvr);

            using (isPartialKey ?
                TableValueToSlice(context, (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType, ref tvr, out key)
              : default(ByteStringContext.InternalScope))
            using (TableValueToSlice(context, (int)AttachmentsTable.Hash, ref tvr, out Slice hash))
            {
                if (expectedChangeVector != null && currentChangeVector != expectedChangeVector)
                {
                    throw new ConcurrencyException($"Attachment {name} with key '{key}' has change vector '{currentChangeVector}', but Delete was called with change vector '{expectedChangeVector}'. Optimistic concurrency violation, transaction will be aborted.")
                    {
                        ActualChangeVector = currentChangeVector,
                        ExpectedChangeVector = expectedChangeVector
                    };
                }

                DeleteInternal(context, key, etag, hash, changeVector, lastModifiedTicks);
            }

            table.Delete(tvr.Id);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DeleteInternal(DocumentsOperationContext context, Slice key, long etag, Slice hash, string changeVector, long lastModifiedTicks)
        {
            CreateTombstone(context, key, etag, changeVector, lastModifiedTicks);

            // we are running just before the delete, so we may still have 1 entry there, the one just
            // about to be deleted
            DeleteAttachmentStream(context, hash);
        }

        private static void DeleteTombstoneIfNeeded(DocumentsOperationContext context, Slice keySlice)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, AttachmentsTombstonesSlice);
            table.DeleteByKey(keySlice);
        }

        private void CreateTombstone(DocumentsOperationContext context, Slice keySlice, long attachmentEtag, string changeVector, long lastModifiedTicks)
        {
            var newEtag = _documentsStorage.GenerateNextEtag();

            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, AttachmentsTombstonesSlice);
            using (table.Allocate(out TableValueBuilder tvb))
            using (Slice.From(context.Allocator, changeVector, out var cv))
            {
                tvb.Add(keySlice.Content.Ptr, keySlice.Size);
                tvb.Add(Bits.SwapBytes(newEtag));
                tvb.Add(Bits.SwapBytes(attachmentEtag));
                tvb.Add(context.GetTransactionMarker());
                tvb.Add((byte)Tombstone.TombstoneType.Attachment);
                tvb.Add(null, 0);
                tvb.Add((int)DocumentFlags.None);
                tvb.Add(cv.Content.Ptr, cv.Size);
                tvb.Add(lastModifiedTicks);
                table.Insert(tvb);
            }
        }

        private void DeleteAttachmentsOfDocumentInternal(DocumentsOperationContext context, Slice prefixSlice, string changeVector, long lastModifiedTicks)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            {
                table.DeleteByPrimaryKeyPrefix(prefixSlice, before =>
                {
                    using (TableValueToSlice(context, (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType, ref before.Reader, out Slice key))
                    using (TableValueToSlice(context, (int)AttachmentsTable.Hash, ref before.Reader, out Slice hash))
                    {
                        var etag = TableValueToEtag((int)AttachmentsTable.Etag, ref before.Reader);
                        DeleteInternal(context, key, etag, hash, changeVector, lastModifiedTicks);
                    }
                });
            }
        }

        public void DeleteRevisionAttachments(DocumentsOperationContext context, Document revision, string changeVector, long lastModifiedTicks)
        {
            using (Slice.From(context.Allocator, revision.ChangeVector, out Slice changeVectorSlice))
            using (GetAttachmentPrefix(context, revision.LowerId.Buffer, revision.LowerId.Size, AttachmentType.Revision, changeVectorSlice, out Slice prefixSlice))
            {
                DeleteAttachmentsOfDocumentInternal(context, prefixSlice, changeVector, lastModifiedTicks);
            }
        }

        public void DeleteAttachmentsOfDocument(DocumentsOperationContext context, Slice lowerId, string changeVector, long lastModifiedTicks)
        {
            using (GetAttachmentPrefix(context, lowerId.Content.Ptr, lowerId.Size, AttachmentType.Document, Slices.Empty, out Slice prefixSlice))
            {
                DeleteAttachmentsOfDocumentInternal(context, prefixSlice, changeVector, lastModifiedTicks);
            }
        }

        public StreamsTempFile GetTempFile(string prefix)
        {
            var name = $"attachment.{Guid.NewGuid():N}.{prefix}";
            var tempPath = _documentsStorage.Environment.Options.DataPager.Options.TempPath.Combine(name);

            return new StreamsTempFile(tempPath.FullPath, _documentDatabase.DocumentsStorage.Environment);
        }

        public static (LazyStringValue DocId, LazyStringValue AttachmentName) ExtractDocIdAndAttachmentNameFromTombstone(JsonOperationContext context,
            LazyStringValue attachmentTombstoneId)
        {
            var p = attachmentTombstoneId.Buffer;
            var size = attachmentTombstoneId.Size;

            int sizeOfDocId = 0;
            for (; sizeOfDocId < size; sizeOfDocId++)
            {
                if (p[sizeOfDocId] == SpecialChars.RecordSeparator)
                    break;
            }

            var attachmentNameIndex = sizeOfDocId +
                                      1 + // separator
                                      1 + // type: d
                                      1; // separator

            int sizeOfAttachmentName = 0;

            for (; sizeOfAttachmentName < size - (sizeOfDocId + 3); sizeOfAttachmentName++)
            {
                if (p[attachmentNameIndex + sizeOfAttachmentName] == SpecialChars.RecordSeparator)
                    break;
            }

            var doc = context.AllocateStringValue(null, p, sizeOfDocId);
            var name = context.AllocateStringValue(null, p + attachmentNameIndex, sizeOfAttachmentName);

            return (doc, name);
        }
    }
}
