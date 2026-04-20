using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Impl;
using Voron.Util;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public class From62000 : ISchemaUpdate
    {
        public int From => 62_000;
        public int To => 72_000;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        private static readonly string AttachmentsMetadata = "AttachmentsMetadata";
        private static readonly string Attachments = "Attachments";
        private static readonly string AttachmentsEtag = "AttachmentsEtag";
        private static readonly string AttachmentsHash = "AttachmentsHash";
        private static readonly string AttachmentsFlagAndHash = "AttachmentsFlagAndHash";
        private static readonly int _oldSchemaSize = 7;

        internal static int NumberOfAttachmentsToMigrateInSingleTransaction = PlatformDetails.Is32Bits ? 2048 : 32768;
        
        private static readonly Slice AttachmentsEtagSlice;
        private static readonly Slice AttachmentsHashSlice;
        private static readonly Slice AttachmentsFlagAndHashSlice;

        static From62000()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, AttachmentsEtag, ByteStringType.Immutable, out AttachmentsEtagSlice);
                Slice.From(ctx, AttachmentsHash, ByteStringType.Immutable, out AttachmentsHashSlice);
                Slice.From(ctx, AttachmentsFlagAndHash, ByteStringType.Immutable, out AttachmentsFlagAndHashSlice);
            }
        }

        /*
         *  We are only updating the attachments table, so we can add missing fields in the table, and correctly populate the dynamic index.
         *  The documents table is not updated, because we are not adding RemoteParameters to document if the parameters are null, this saves us document table space and schema update.
         */
        public bool Update(UpdateStep step)
        {
            var isSharded = step.DocumentsStorage is ShardedDocumentsStorage;
            step.WriteTx.CreateTree(AttachmentsFlagAndHashSlice, isIndexTree: true);
            
            TableSchema attachmentsSchemaBase = new TableSchema();
            
            attachmentsSchemaBase.DefineKey(new TableSchema.IndexDef
            {
                StartIndex = (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType,
                Count = 1
            });
            attachmentsSchemaBase.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
            {
                StartIndex = (int)AttachmentsTable.Etag,
                Name = AttachmentsEtagSlice
            });
            attachmentsSchemaBase.DefineIndex(new TableSchema.IndexDef
            {
                StartIndex = (int)AttachmentsTable.Hash,
                Count = 1,
                Name = AttachmentsHashSlice
            });

            var dynamicIndex = new TableSchema.DynamicKeyIndexDef
            {
                GenerateKey = GenerateFlagAndHashForAttachments,
                IsGlobal = true,
                Name = AttachmentsFlagAndHashSlice,
                SupportDuplicateKeys = true
            };

            attachmentsSchemaBase.DefineIndex(dynamicIndex);

            if (isSharded)
            {
                attachmentsSchemaBase.DefineIndex(new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = AttachmentsStorage.GenerateBucketAndHashForAttachments,
                    IsGlobal = true,
                    Name = Raven.Server.Documents.Schemas.Attachments.AttachmentsBucketAndHashSlice,
                    SupportDuplicateKeys = true
                });

                attachmentsSchemaBase.DefineIndex(new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = AttachmentsStorage.GenerateBucketAndEtagIndexKeyForAttachments,
                    OnEntryChanged = AttachmentsStorage.UpdateBucketStatsForAttachments,
                    IsGlobal = true,
                    Name = Raven.Server.Documents.Schemas.Attachments.AttachmentsBucketAndEtagSlice
                });
            }

            UpdateSchemaInternal(step, attachmentsSchemaBase, dynamicIndex, isSharded, PopulateAttachmentsFlagAndHashDynamicIndex);
            UpdateSchemaInternal(step, attachmentsSchemaBase, dynamicIndex, isSharded, AttachmentsTableSchemaUpdate);

            return true;
        }

        private void UpdateSchemaInternal(UpdateStep step, TableSchema attachmentsSchemaBase, TableSchema.DynamicKeyIndexDef dynamicIndex, bool isSharded,
            Func<TableSchema.DynamicKeyIndexDef, Table, Slice, TableValueBuilder, bool> update)
        {
            var skip = 0L;
            var processed = 0L;
            var done = false;

            while (done == false)
            {
                using (step.AllocateDocumentsOperationContext(out DocumentsOperationContext context))
                {
                    if (isSharded)
                    {
                        step.WriteTx.Owner = context;
                        step.WriteTx.OnBeforeCommit += ((ShardedDocumentsStorage)step.DocumentsStorage).OnBeforeCommit;
                        step.WriteTx.LowLevelTransaction.OnRollBack += ((ShardedDocumentsStorage)step.DocumentsStorage).OnFailure;
                    }

                    context.TransactionMarkerOffset = (short)step.WriteTx.LowLevelTransaction.Id;
                    var commit = false;
                    var readTable = step.ReadTx.OpenTable(attachmentsSchemaBase, AttachmentsMetadata);
                    if (readTable != null)
                    {
                        var writeTable = step.WriteTx.OpenTable(attachmentsSchemaBase, AttachmentsMetadata);

                        var streamsTree = step.ReadTx.ReadTree(Attachments);

                        foreach (var read in readTable.SeekByPrimaryKey(Slices.BeforeAllKeys, skip))
                        {
                            using (TableValueReaderUtil.CloneTableValueReader(context, read))
                            {
                                Attachment attachmentOld = TableValueToAttachmentOld(context, streamsTree, ref read.Reader, out var scope);

                                using (scope)
                                using (AttachmentOldToTableValue(context, writeTable, attachmentOld, out var tvb, out var pkSlice))
                                {
                                    update.Invoke(dynamicIndex, writeTable, pkSlice, tvb);
                                }
                            }

                            if (++processed >= NumberOfAttachmentsToMigrateInSingleTransaction)
                            {
                                skip += processed;
                                processed = 0;
                                commit = true;
                                break;
                            }
                        }

                        if (commit)
                        {
                            step.Commit(context);
                            step.RenewTransactions();
                            continue;
                        }

                        done = true;
                    }

                    if (isSharded)
                    {
                        // for sharded we need to manually commit here, since it is using the context in the tx.OnBeforeCommit event
                        step.Commit(context);
                        step.RenewTransactions();
                        step.WriteTx.Owner = null;
                        step.WriteTx.OnBeforeCommit -= ((ShardedDocumentsStorage)step.DocumentsStorage).OnBeforeCommit;
                        step.WriteTx.LowLevelTransaction.OnRollBack -= ((ShardedDocumentsStorage)step.DocumentsStorage).OnFailure;
                    }
                }
            }
        }

        private static bool PopulateAttachmentsFlagAndHashDynamicIndex(TableSchema.DynamicKeyIndexDef dynamicIndex, Table writeTable, Slice pkSlice, TableValueBuilder tvb)
        {
            if (writeTable.TryFindIdFromPrimaryKey(pkSlice, out var id))
            {
                // populate value into the new dynamic index
                using (dynamicIndex.GetValue(writeTable._tx, tvb, out Slice newVal))
                {
                    var indexTree = writeTable.GetTree(dynamicIndex);
                    writeTable.AddValueToDynamicIndex(id, dynamicIndex, indexTree, newVal, TreeNodeFlags.Data);
                }
            }

            return true;
        }

        private bool AttachmentsTableSchemaUpdate(TableSchema.DynamicKeyIndexDef dynamicIndex, Table writeTable, Slice pkSlice, TableValueBuilder tvb)
        {
            writeTable.Set(tvb, forceUpdate: true);
            return true;
        }

        private unsafe IDisposable AttachmentOldToTableValue(DocumentsOperationContext context, Table writeTable, Attachment attachment, out TableValueBuilder tvb, out Slice keySlice)
        {
            var toDispose = new List<IDisposable>();

            toDispose.Add(writeTable.Allocate(out tvb));
            toDispose.Add(Slice.From(context.Allocator, attachment.ChangeVector, out var changeVectorSlice));
            toDispose.Add(Slice.From(context.Allocator, attachment.Key.Buffer, attachment.Key.Size, ByteStringType.Immutable, out keySlice));
            toDispose.Add(DocumentIdWorker.GetLowerIdSliceAndStorageKeyForBackwardCompatibility(context, attachment.Name, out Slice lowerName1, out Slice namePtr));
            toDispose.Add(DocumentIdWorker.GetLowerIdSliceAndStorageKeyForBackwardCompatibility(context, attachment.ContentType, out Slice lowerName2, out Slice contentTypePtr));

            tvb.Add(keySlice.Content.Ptr, keySlice.Size);
            tvb.Add(Bits.SwapBytes(attachment.Etag));
            tvb.Add(namePtr);
            tvb.Add(contentTypePtr);
            tvb.Add(attachment.Base64Hash.Content.Ptr, attachment.Base64Hash.Size);
            tvb.Add(attachment.TransactionMarker);
            tvb.Add(changeVectorSlice.Content.Ptr, changeVectorSlice.Size);

            tvb.Add(attachment.Size);
            tvb.Add(Bits.SwapBytes((int)Client.Documents.Attachments.RemoteAttachmentFlags.None));
            tvb.Add(-1L);
            tvb.Add(Slices.Empty);

            return new DisposableAction(() =>
            {
                foreach (var item in toDispose)
                {
                    item.Dispose();
                }
            });
        }

        private static unsafe long GetAttachmentStreamLength(Tree tree, Slice hashSlice)
        {
            var info = tree.GetStreamInfo(hashSlice, false);
            if (info == null)
                return -1;
            return info->TotalSize;
        }

        private unsafe Attachment TableValueToAttachmentOld(DocumentsOperationContext context, Tree tree, ref TableValueReader tvr, out ByteStringContext<ByteStringMemoryCache>.InternalScope scope)
        {
            var result = new Attachment
            {
                StorageId = tvr.Id,
                Key = DocumentsStorage.TableValueToString(context, (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType, ref tvr),
                Etag = DocumentsStorage.TableValueToEtag((int)AttachmentsTable.Etag, ref tvr),
                ChangeVector = DocumentsStorage.TableValueToChangeVector(context, (int)AttachmentsTable.ChangeVector, ref tvr),
                Name = DocumentsStorage.TableValueToId(context, (int)AttachmentsTable.Name, ref tvr),
                ContentType = DocumentsStorage.TableValueToId(context, (int)AttachmentsTable.ContentType, ref tvr),
                TransactionMarker = *(short*)tvr.Read((int)AttachmentsTable.TransactionMarker, out int _)
            };

            scope = DocumentsStorage.TableValueToSlice(context, (int)AttachmentsTable.Hash, ref tvr, out result.Base64Hash);

            result.Size = GetAttachmentStreamLength(tree, result.Base64Hash);

            return result;
        }

        [StorageIndexEntryKeyGenerator]
        internal static unsafe ByteStringContext.Scope GenerateFlagAndHashForAttachments(Transaction tx, ref TableValueReader tvr, out Slice slice)
        {
            var hashPtr = tvr.Read((int)AttachmentsTable.Hash, out var hashSize);

            int flags = tvr.Count == _oldSchemaSize ? (int)RemoteAttachmentFlags.None : *(int*)tvr.Read((int)AttachmentsTable.Flags, out var size);
            var scope = tx.Allocator.Allocate(sizeof(int) + 1 + hashSize, out var buffer); // flag + record separator + hash

            var span = new Span<byte>(buffer.Ptr, buffer.Length);
            MemoryMarshal.AsBytes(new Span<int>(ref flags)).CopyTo(span);
            buffer.Ptr[sizeof(int)] = SpecialChars.RecordSeparator;
            new ReadOnlySpan<byte>(hashPtr, hashSize).CopyTo(span[(sizeof(int) + 1)..]);

            slice = new Slice(buffer);
            return scope;
        }

        internal enum AttachmentsTable
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
            ChangeVector = 6,
            Size = 7,
            Flags = 8,
            RemoteAt = 9,
            Identifier = 10,
        }
    }
}
