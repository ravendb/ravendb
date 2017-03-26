using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Exceptions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Versioning;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using ConcurrencyException = Voron.Exceptions.ConcurrencyException;

namespace Raven.Server.Documents
{
    public unsafe class AttachmentsStorage
    {
        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;
        private readonly Logger _logger;

        private static readonly Slice AttachmentsSlice;
        private static readonly Slice AttachmentsMetadataSlice;
        public static readonly Slice AttachmentsEtagSlice;
        private static readonly Slice AttachmentsHashSlice;
        private static readonly Slice AttachmentsTombstonesSlice;

        private static readonly TableSchema AttachmentsSchema = new TableSchema();
        public static readonly string AttachmentsTombstones = "Attachments.Tombstones";

        // The attachments schema is as follows
        // 5 fields (lowered document id AND record separator AND lowered name, etag, name, content type, last modified)
        // We are you using the record separator in order to avoid loading another files that has the same key prefix, 
        //      e.g. fitz(record-separator)profile.png and fitz0(record-separator)profile.png, without the record separator we would have to load also fitz0 and filter it.
        // format of lazy string key is detailed in GetLowerKeySliceAndStorageKey
        private enum AttachmentsTable
        {
            LoweredDocumentIdAndLoweredNameAndType = 0,
            Etag = 1,
            Name = 2,
            ContentType = 3,
            Hash = 4,
            TransactionMarker = 5,
        }

        static AttachmentsStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "Attachments", ByteStringType.Immutable, out AttachmentsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "AttachmentsMetadata", ByteStringType.Immutable, out AttachmentsMetadataSlice);
            Slice.From(StorageEnvironment.LabelsContext, "AttachmentsEtag", ByteStringType.Immutable, out AttachmentsEtagSlice);
            Slice.From(StorageEnvironment.LabelsContext, "AttachmentsHash", ByteStringType.Immutable, out AttachmentsHashSlice);
            Slice.From(StorageEnvironment.LabelsContext, AttachmentsTombstones, ByteStringType.Immutable, out AttachmentsTombstonesSlice);

            AttachmentsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)AttachmentsTable.LoweredDocumentIdAndLoweredNameAndType,
                Count = 1,
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
            _logger = LoggingSource.Instance.GetLogger<AttachmentsStorage>(documentDatabase.Name);

            tx.CreateTree(AttachmentsSlice);
            AttachmentsSchema.Create(tx, AttachmentsMetadataSlice, 32);
            DocumentsStorage.TombstonesSchema.Create(tx, AttachmentsTombstonesSlice, 16);
        }

        public IEnumerable<ReplicationBatchItem> GetAttachmentsFrom(DocumentsOperationContext context, long etag)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            foreach (var result in table.SeekForwardFrom(AttachmentsSchema.FixedSizeIndexes[AttachmentsEtagSlice], etag, 0))
            {
                var attachment = TableValueToAttachment(context, ref result.Reader);

                var stream = GetAttachmentStream(context, attachment.Base64Hash);
                if (stream == null)
                    throw new FileNotFoundException($"Attachment's stream {attachment.Name} was not found. This should never happen.");
                attachment.Stream = stream;

                yield return ReplicationBatchItem.From(attachment);
            }
        }

        private long GetCountOfAttachmentsForHash(DocumentsOperationContext context, Slice hash)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            return table.GetCountOfMatchesFor(AttachmentsSchema.Indexes[AttachmentsHashSlice], hash);
        }

        public AttachmentResult PutAttachment(
            DocumentsOperationContext context,
            string documentId,
            string name,
            string contentType,
            string hash,
            long? expectedEtag,
            Stream stream)
        {
            if (context.Transaction == null)
            {
                DocumentsStorage.ThrowRequiresTransaction();
                return default(AttachmentResult); // never reached
            }

            // Attachment etag should be generated before updating the document
            var attachmenEtag = _documentsStorage.GenerateNextEtag();

            Slice lowerDocumentId;
            DocumentKeyWorker.GetSliceFromKey(context, documentId, out lowerDocumentId);

            TableValueReader tvr;
            var hasDoc = TryGetDocumentTableValueReaderForAttachment(context, documentId, name, lowerDocumentId, out tvr);
            if (hasDoc == false)
                throw new InvalidOperationException($"Cannot put attachment {name} on a non existent document '{documentId}'.");

            byte* lowerName;
            int lowerNameSize;
            byte* namePtr;
            int nameSize;
            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, name, out lowerName, out lowerNameSize, out namePtr, out nameSize);

            Slice keySlice, contentTypeSlice, hashSlice;
            using (GetAttachmentKey(context, lowerDocumentId.Content.Ptr, lowerDocumentId.Size, lowerName, lowerNameSize, AttachmentType.Document, null, out keySlice))
            using (DocumentKeyWorker.GetStringPreserveCase(context, contentType, out contentTypeSlice))
            using (Slice.From(context.Allocator, hash, out hashSlice)) // Hash is a base64 string, so this is a special case that we do not need to escape
            {
                var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
                TableValueBuilder tbv;
                using (table.Allocate(out tbv))
                {
                    var transactionMarker = context.GetTransactionMarker();

                    tbv.Add(keySlice.Content.Ptr, keySlice.Size);
                    tbv.Add(Bits.SwapBytes(attachmenEtag));
                    tbv.Add(namePtr, nameSize);
                    tbv.Add(contentTypeSlice.Content.Ptr, contentTypeSlice.Size);
                    tbv.Add(hashSlice.Content.Ptr, hashSlice.Size);
                    tbv.Add(transactionMarker);

                    TableValueReader oldValue;
                    if (table.ReadByKey(keySlice, out oldValue))
                    {
                        // TODO: Support overwrite
                        throw new NotImplementedException("Cannot overwrite an existing attachment.");

                        /*
                        var oldEtag = TableValueToEtag(context, 1, ref oldValue);
                        if (expectedEtag != null && oldEtag != expectedEtag)
                            throw new ConcurrencyException($"Attachment {name} has etag {oldEtag}, but Put was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.")
                            {
                                ActualETag = oldEtag,
                                ExpectedETag = expectedEtag ?? -1
                            };

                        table.Update(oldValue.Id, tbv);*/
                    }
                    else
                    {
                        if (expectedEtag.HasValue && expectedEtag.Value != 0)
                        {
                            ThrowConcurrentExceptionOnMissingAttacment(documentId, name, expectedEtag.Value);
                        }

                        table.Insert(tbv);
                    }
                }

                PutAttachmentStream(context, keySlice, hashSlice, stream);

                _documentDatabase.Metrics.AttachmentPutsPerSecond.MarkSingleThreaded(1);

                // Update the document with an etag which is bigger than the attachmenEtag
                // We need to call this after we already put the attachment, so it can version also this attachment
                _documentsStorage.UpdateDocumentAfterAttachmentChange(context, documentId, tvr);
            }

            return new AttachmentResult
            {
                Etag = attachmenEtag,
                ContentType = contentType,
                Name = name,
                DocumentId = documentId,
                Hash = hash,
            };
        }

        public void PutFromReplication(DocumentsOperationContext context, Slice key, Slice name, Slice contentType, 
            Slice base64Hash, short transactionMarker)
        {
            // Attachment etag should be generated before updating the document
            var attachmenEtag = _documentsStorage.GenerateNextEtag();

            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            TableValueBuilder tbv;
            using (table.Allocate(out tbv))
            {
                tbv.Add(key.Content.Ptr, key.Size);
                tbv.Add(Bits.SwapBytes(attachmenEtag));
                tbv.Add(name.Content.Ptr, name.Size);
                tbv.Add(contentType.Content.Ptr, contentType.Size);
                tbv.Add(base64Hash.Content.Ptr, base64Hash.Size);
                tbv.Add(transactionMarker);

                table.Set(tbv);
            }

            _documentDatabase.Metrics.AttachmentPutsPerSecond.MarkSingleThreaded(1);
        }

        public void RevisionAttachments(DocumentsOperationContext context, byte* lowerKey, int lowerKeySize, ChangeVectorEntry[] changeVector)
        {
            Slice prefixSlice;
            using (GetAttachmentPrefix(context, lowerKey, lowerKeySize, AttachmentType.Document, null, out prefixSlice))
            {
                var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
                var currentAttachments = new List<Tuple<Slice, Slice, Slice>>();
                foreach (var sr in table.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
                {
                    Slice name, contentType, base64Hash;

                    int size;
                    var ptr = sr.Reader.Read((int)AttachmentsTable.Name, out size);
                    Slice.From(context.Allocator, ptr, size, out name);

                    ptr = sr.Reader.Read((int)AttachmentsTable.ContentType, out size);
                    Slice.From(context.Allocator, ptr, size, out contentType);

                    ptr = sr.Reader.Read((int)AttachmentsTable.Hash, out size);
                    Slice.From(context.Allocator, ptr, size, out base64Hash);

                    currentAttachments.Add(new Tuple<Slice, Slice, Slice>(name, contentType, base64Hash));
                }
                foreach (var attachment in currentAttachments)
                {
                    PutRevisionAttachment(context, lowerKey, lowerKeySize, changeVector, attachment);
                    attachment.Item1.Release(context.Allocator);
                    attachment.Item2.Release(context.Allocator);
                    attachment.Item3.Release(context.Allocator);
                }
            }
        }

        private void PutRevisionAttachment(DocumentsOperationContext context, byte* lowerKey, int lowerKeySize, 
            ChangeVectorEntry[] changeVector, Tuple<Slice, Slice, Slice> attachment)
        {
            var attachmenEtag = _documentsStorage.GenerateNextEtag();

            int size;
            // See format of the lazy string key in the GetLowerKeySliceAndStorageKey method
            byte offset;
            var ptr = attachment.Item1.Content.Ptr;
            size = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, 0, out offset);
            var name = context.AllocateStringValue(null, ptr + offset, size).ToString();

            Slice lowerName, keySlice;
            using (DocumentKeyWorker.GetSliceFromKey(context, name, out lowerName))
            using (GetAttachmentKey(context, lowerKey, lowerKeySize, lowerName.Content.Ptr, lowerName.Size, AttachmentType.Revision, changeVector, out keySlice))
            {
                var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
                TableValueBuilder tbv;
                using (table.Allocate(out tbv))
                {
                    var transactionMarker = context.GetTransactionMarker();

                    tbv.Add(keySlice.Content.Ptr, keySlice.Size);
                    tbv.Add(Bits.SwapBytes(attachmenEtag));
                    tbv.Add(attachment.Item1);
                    tbv.Add(attachment.Item2);
                    tbv.Add(attachment.Item3);
                    tbv.Add(transactionMarker);
                    table.Set(tbv);
                }
            }
        }

        public void PutAttachmentStream(DocumentsOperationContext context, Slice key, Slice base64Hash, Stream stream)
        {
            var tree = context.Transaction.InnerTransaction.CreateTree(AttachmentsSlice);
            var existingStream = tree.ReadStream(base64Hash);
            if (existingStream == null)
                tree.AddStream(base64Hash, stream, tag: key);

            _documentDatabase.Metrics.AttachmentBytesPutsPerSecond.MarkSingleThreaded(stream.Length);
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
            string name, Slice loweredKey, out TableValueReader tvr)
        {
            bool hasDoc;
            try
            {
                hasDoc = _documentsStorage.GetTableValueReaderForDocument(context, loweredKey, out tvr);
            }
            catch (DocumentConflictException e)
            {
                throw new InvalidOperationException($"Cannot put/delete an attachment {name} on a document '{documentId}' when it has an unresolved conflict.", e);
            }
            return hasDoc;
        }

        public IEnumerable<Attachment> GetAttachmentsForDocument(DocumentsOperationContext context, Slice prefixSlice)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            foreach (var sr in table.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
            {
                var attachment = TableValueToAttachment(context, ref sr.Reader);
                if (attachment == null)
                    continue;

                attachment.Size = GetAttachmentStreamLength(context, attachment.Base64Hash);

                yield return attachment;
            }
        }

        public (long attachmentCount, long streamsCount) GetNumberOfAttachments(DocumentsOperationContext context)
        {
            // We count in also versioned attachments

            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            var count = table.NumberOfEntries;

            var tree = context.Transaction.InnerTransaction.CreateTree(AttachmentsSlice);
            var streamsCount = tree.State.NumberOfEntries;

            return (count, streamsCount);
        }

        public Attachment GetAttachment(DocumentsOperationContext context, string documentId, string name, 
            AttachmentType type, ChangeVectorEntry[] changeVector)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Argument is null or whitespace", nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Argument is null or whitespace", nameof(name));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Get", nameof(context));
            if (type != AttachmentType.Document && changeVector == null)
                throw new ArgumentException($"Change Vector cannot be null for attachment type {type}", nameof(changeVector));

            Slice lowerKey, lowerName, keySlice;
            using (DocumentKeyWorker.GetSliceFromKey(context, documentId, out lowerKey))
            using (DocumentKeyWorker.GetSliceFromKey(context, name, out lowerName))
            using (GetAttachmentKey(context, lowerKey.Content.Ptr, lowerKey.Size, lowerName.Content.Ptr, lowerName.Size,
                type, changeVector, out keySlice))
            {
                var attachment = GetAttachment(context, keySlice);
                if (attachment == null)
                    return null;

                var stream = GetAttachmentStream(context, attachment.Base64Hash);
                if (stream == null)
                    throw new FileNotFoundException($"Attachment's stream {name} on {documentId} was not found. This should not happen and is likely a bug.");
                attachment.Stream = stream;

                return attachment;
            }
        }

        public Attachment GetAttachment(DocumentsOperationContext context, Slice keySlice)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            TableValueReader tvr;
            if (table.ReadByKey(keySlice, out tvr) == false)
                return null;

            return TableValueToAttachment(context, ref tvr);
        }

        private Stream GetAttachmentStream(DocumentsOperationContext context, Slice hashSlice)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(AttachmentsSlice);
            return tree.ReadStream(hashSlice);
        }

        private long GetAttachmentStreamLength(DocumentsOperationContext context, Slice hashSlice)
        {
            var tree = context.Transaction.InnerTransaction.ReadTree(AttachmentsSlice);
            var info = tree.GetStreamInfo(hashSlice, false);
            if (info == null)
                return -1;
            return info->TotalSize;
        }

        /*
        // Document key: {lowerDocumentId|d|lowerName}
        // Conflict key: {lowerDocumentId|c|changeVector|lowerName}
        // Revision key: {lowerDocumentId|r|changeVector|lowerName}
        // 
        // TODO: We'll solve conflicts using the hash value in the table value reader. No need to put it also in the key.
        //
        // Document prefix: {lowerDocumentId|d|}
        // Conflict prefix: {lowerDocumentId|c|changeVector|}
        // Revision prefix: {lowerDocumentId|r|changeVector|}
        */

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReleaseMemory GetAttachmentKey(DocumentsOperationContext context, byte* lowerKey, int lowerKeySize,
            byte* lowerName, int lowerNameSize, AttachmentType type, ChangeVectorEntry[] changeVector, out Slice keySlice)
        {
            return GetAttachmentKeyInternal(context, lowerKey, lowerKeySize, lowerName, lowerNameSize, false, type, changeVector, out keySlice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReleaseMemory GetAttachmentPrefix(DocumentsOperationContext context, byte* lowerKey, int lowerKeySize,
            AttachmentType type, ChangeVectorEntry[] changeVector, out Slice prefixSlice)
        {
            return GetAttachmentKeyInternal(context, lowerKey, lowerKeySize, null, 0, true, type, changeVector, out prefixSlice);
        }

        private ReleaseMemory GetAttachmentKeyInternal(DocumentsOperationContext context, byte* lowerKey, int lowerKeySize,
            byte* lowerName, int lowerNameSize, bool isPrefix, AttachmentType type, ChangeVectorEntry[] changeVector, out Slice keySlice)
        {
            var changeVectorSize = 0;

            var size = lowerKeySize + 3;
            if (type != AttachmentType.Document)
            {
                changeVectorSize = sizeof(ChangeVectorEntry) * changeVector.Length;
                size += changeVectorSize + 1;
            }
            if (isPrefix == false)
            {
                size += lowerNameSize;
            }

            var keyMem = context.Allocator.Allocate(size);

            Memory.Copy(keyMem.Ptr, lowerKey, lowerKeySize);
            var pos = lowerKeySize;
            keyMem.Ptr[pos++] = VersioningStorage.RecordSeperator;

            switch (type)
            {
                case AttachmentType.Document:
                    keyMem.Ptr[pos++] = (byte)'d';
                    break;
                case AttachmentType.Revision:
                    keyMem.Ptr[pos++] = (byte)'r';
                    break;
                case AttachmentType.Conflict:
                    keyMem.Ptr[pos++] = (byte)'c';
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
            keyMem.Ptr[pos++] = VersioningStorage.RecordSeperator;

            if (type != AttachmentType.Document)
            {
                fixed (ChangeVectorEntry* pChangeVector = changeVector)
                {
                    Memory.Copy(keyMem.Ptr + pos, (byte*)pChangeVector, changeVectorSize);
                }
                pos += changeVectorSize;
                keyMem.Ptr[pos++] = VersioningStorage.RecordSeperator;
            }

            if (isPrefix == false)
                Memory.Copy(keyMem.Ptr + pos, lowerName, lowerNameSize);

            keySlice = new Slice(SliceOptions.Key, keyMem);
            return new ReleaseMemory(keyMem, context);
        }

        private Attachment TableValueToAttachment(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            var result = new Attachment
            {
                StorageId = tvr.Id
            };

            result.LoweredKey = DocumentsStorage.TableValueToString(context, (int)AttachmentsTable.LoweredDocumentIdAndLoweredNameAndType, ref tvr);
            result.Etag = DocumentsStorage.TableValueToEtag((int)AttachmentsTable.Etag, ref tvr);
            result.Name = DocumentsStorage.TableValueToKey(context, (int)AttachmentsTable.Name, ref tvr);
            result.ContentType = DocumentsStorage.TableValueToKey(context, (int)AttachmentsTable.ContentType, ref tvr);

            DocumentsStorage.TableValueToSlice(context, (int)AttachmentsTable.Hash, ref tvr, out result.Base64Hash);

            int size;
            result.TransactionMarker = *(short*)tvr.Read((int)AttachmentsTable.TransactionMarker, out size);

            return result;
        }

        private static void ThrowConcurrentExceptionOnMissingAttacment(string documentId, string name, long expectedEtag)
        {
            throw new ConcurrencyException(
                $"Attachment {name} of '{documentId}' does not exist, but Put was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.")
            {
                ExpectedETag = expectedEtag
            };
        }

        public void DeleteAttachment(DocumentsOperationContext context, string documentId, string name, long? expectedEtag)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Argument is null or whitespace", nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Argument is null or whitespace", nameof(name));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Get", nameof(context));

            Slice lowerDocumentId, lowerName;
            using (DocumentKeyWorker.GetSliceFromKey(context, documentId, out lowerDocumentId))
            using (DocumentKeyWorker.GetSliceFromKey(context, name, out lowerName))
            {
                Slice keySlice;
                using (GetAttachmentKey(context, lowerDocumentId.Content.Ptr, lowerDocumentId.Size, lowerName.Content.Ptr, lowerName.Size, AttachmentType.Document, null, out keySlice))
                {
                    TableValueReader docTvr;
                    var hasDoc = TryGetDocumentTableValueReaderForAttachment(context, documentId, name, lowerDocumentId, out docTvr);
                    if (hasDoc == false)
                    {
                        if (expectedEtag != null)
                            throw new ConcurrencyException($"Document {documentId} does not exist, " +
                                                           $"but delete was called with etag {expectedEtag} to remove attachment {name}. " +
                                                           $"Optimistic concurrency violation, transaction will be aborted.");

                        // this basically mean that we tried to delete attachment whose document doesn't exist.
                        return;
                    }

                    DeleteAttachmentDirect(context, keySlice, name, expectedEtag);

                    _documentsStorage.UpdateDocumentAfterAttachmentChange(context, documentId, docTvr);
                }
            }
        }

        public void DeleteAttachmentDirect(DocumentsOperationContext context, Slice key, string name, long? expectedEtag)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            TableValueReader tvr;
            if (table.ReadByKey(key, out tvr) == false)
            {
                if (expectedEtag != null)
                    throw new ConcurrencyException($"Attachment {name} with key '{key}' does not exist, " +
                                                   $"but delete was called with etag {expectedEtag}. " +
                                                   $"Optimistic concurrency violation, transaction will be aborted.");

                // this basically means that we tried to delete attachment that doesn't exist.
                return;
            }

            var etag = DocumentsStorage.TableValueToEtag((int)AttachmentsTable.Etag, ref tvr);
            if (expectedEtag != null && etag != expectedEtag)
            {
                throw new ConcurrencyException($"Attachment {name} with key '{key}' has etag {etag}, but Delete was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.")
                {
                    ActualETag = etag,
                    ExpectedETag = (long)expectedEtag
                };
            }

            Slice hashSlice;
            using (DocumentsStorage.TableValueToSlice(context, (int)AttachmentsTable.Hash, ref tvr, out hashSlice))
            {
                DeleteAttachmentStream(context, hashSlice);
            }

            table.Delete(tvr.Id);

            CreateTombstone(context, key, etag);
        }

        private void CreateTombstone(DocumentsOperationContext context, Slice keySlice, long attachmentEtag)
        {
            var newEtag = _documentsStorage.GenerateNextEtag();

            var table = context.Transaction.InnerTransaction.OpenTable(DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);
            TableValueBuilder tbv;
            using (table.Allocate(out tbv))
            {
                tbv.Add(keySlice.Content.Ptr, keySlice.Size);
                tbv.Add(Bits.SwapBytes(newEtag));
                tbv.Add(Bits.SwapBytes(attachmentEtag));
                tbv.Add(context.GetTransactionMarker());
                tbv.Add((byte)DocumentTombstone.TombstoneType.Attachment);
                tbv.Add(null, 0);
                tbv.Add((int)DocumentFlags.None);
                tbv.Add(null, 0);
                tbv.Add(null, 0);

                table.Insert(tbv);
            }
        }

        private void DeleteAttachmentsOfDocumentInternal(DocumentsOperationContext context, Slice prefixSlice)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            {
                table.DeleteByPrimaryKeyPrefix(prefixSlice, before =>
                {
                    Slice hashSlice;
                    using (DocumentsStorage.TableValueToSlice(context, (int)AttachmentsTable.Hash, ref before.Reader, out hashSlice))
                    {
                        // we are running just before the delete, so we may still have 1 entry there, the one just
                        // about to be deleted
                        DeleteAttachmentStream(context, hashSlice);
                    }
                });
            }
        }

        public void DeleteRevisionAttachments(DocumentsOperationContext context, Document revision)
        {
            Slice prefixSlice;
            using (GetAttachmentPrefix(context, revision.LoweredKey.Buffer, revision.LoweredKey.Size,
                AttachmentType.Revision, revision.ChangeVector, out prefixSlice))
            {
                DeleteAttachmentsOfDocumentInternal(context, prefixSlice);
            }
        }

        public void DeleteAttachmentsOfDocument(DocumentsOperationContext context, Slice loweredKey)
        {
            Slice prefixSlice;
            using (GetAttachmentPrefix(context, loweredKey.Content.Ptr, loweredKey.Size, AttachmentType.Document, null, out prefixSlice))
            {
                DeleteAttachmentsOfDocumentInternal(context, prefixSlice);
            }
        }

        public ReleaseTempFile GetTempFile(out FileStream file, bool fromReplication = false)
        {
            var name = $"attachment.{Guid.NewGuid():N}.put";
            if (fromReplication)
                name = "replication-" + name;
            var tempPath = Path.Combine(_documentsStorage.Environment.Options.DataPager.Options.TempPath, name);
            file = new FileStream(tempPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.DeleteOnClose | FileOptions.SequentialScan);
            return new ReleaseTempFile(tempPath, file);
        }

        public struct ReleaseTempFile : IDisposable
        {
            private readonly string _tempFile;
            private readonly FileStream _file;

            public ReleaseTempFile(string tempFile, FileStream file)
            {
                _tempFile = tempFile;
                _file = file;
            }

            public void Dispose()
            {
                _file.Dispose();

                // Linux does not clean the file, so we should clean it manually
                IOExtensions.DeleteFile(_tempFile);
            }
        }
    }
}