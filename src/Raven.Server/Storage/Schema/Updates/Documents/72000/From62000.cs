using System;
using System.Collections.Generic;
using Raven.Server.Documents;
using Raven.Server.Documents.Schemas;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Platform;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Util;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public class From62000 : ISchemaUpdate
    {
        public int From => 62_000;
        public int To => 72_000;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        internal static int NumberOfAttachmentsToMigrateInSingleTransaction = PlatformDetails.Is32Bits ? 2048 : 32768;

        /*
         *  We are only updating the attachments table, so we can add missing fields in the table, and correctly populate the dynamic index.
         *  The documents table is not updated, because we are not adding RetireParameters to document if the parameters are null, this saves us document table space and schema update.
         */
        public bool Update(UpdateStep step)
        {
            step.WriteTx.CreateTree(Attachments.AttachmentsFlagAndHashSlice, isIndexTree: true);

            var skip = 0L;
            var processed = 0L;
            var done = false;

            var dynamicIndex = Attachments.AttachmentsSchemaBase.DynamicKeyIndexes[Attachments.AttachmentsFlagAndHashSlice];

            // we need to remove the dynamic index so we can remove the old value for which we are missing an entry inside the dynamic index
            Attachments.AttachmentsSchemaBase.DynamicKeyIndexes.Remove(Attachments.AttachmentsFlagAndHashSlice);

            try
            {
                while (done == false)
                {
                    using (step.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        context.TransactionMarkerOffset = (short)step.WriteTx.LowLevelTransaction.Id;
                        var commit = false;
                        var readTable = step.ReadTx.OpenTable(Attachments.AttachmentsSchemaBase, Attachments.AttachmentsMetadataSlice);
                        if (readTable != null)
                        {
                            Table writeTable = step.WriteTx.OpenTable(Attachments.AttachmentsSchemaBase, Attachments.AttachmentsMetadataSlice);
                            var streamsTree = step.ReadTx.ReadTree(Attachments.AttachmentsSlice);

                            foreach (var read in readTable.SeekByPrimaryKey(Slices.BeforeAllKeys, skip))
                            {
                                using (TableValueReaderUtil.CloneTableValueReader(context, read))
                                {
                                    Attachment attachmentOld = TableValueToAttachmentOld(context, streamsTree, ref read.Reader, out var scope);

                                    using (scope)
                                    using (AttachmentOldToTableValue(context, writeTable, attachmentOld, out var tvb, out var pkSlice))
                                    {
                                        writeTable.DeleteByKey(pkSlice);

                                        var id = writeTable.Insert(tvb);

                                        using (dynamicIndex.GetValue(writeTable._tx, tvb, out Slice newVal))
                                        {
                                            var indexTree = writeTable.GetTree(dynamicIndex);
                                            writeTable.AddValueToDynamicIndex(id, dynamicIndex, indexTree, newVal, TreeNodeFlags.Data);
                                        }
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
                    }
                }
            }
            finally
            {
                // now we add the populated dynamic index back
                Attachments.AttachmentsSchemaBase.DynamicKeyIndexes.Add(Attachments.AttachmentsFlagAndHashSlice, dynamicIndex);
            }

            return true;
        }

        private unsafe IDisposable AttachmentOldToTableValue(DocumentsOperationContext context, Table writeTable, Attachment attachment, out TableValueBuilder tvb, out Slice keySlice)
        {
            var toDispose = new List<IDisposable>();

            toDispose.Add(writeTable.Allocate(out tvb));
            toDispose.Add(Slice.From(context.Allocator, attachment.ChangeVector, out var changeVectorSlice));
            toDispose.Add(Slice.From(context.Allocator, attachment.Key.Buffer, attachment.Key.Size, ByteStringType.Immutable, out keySlice));
            toDispose.Add(DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, attachment.Name, out Slice lowerName1, out Slice namePtr));
            toDispose.Add(DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, attachment.ContentType, out Slice lowerName2, out Slice contentTypePtr));

            tvb.Add(keySlice.Content.Ptr, keySlice.Size);
            tvb.Add(Bits.SwapBytes(attachment.Etag));
            tvb.Add(namePtr);
            tvb.Add(contentTypePtr);
            tvb.Add(attachment.Base64Hash.Content.Ptr, attachment.Base64Hash.Size);
            tvb.Add(attachment.TransactionMarker);
            tvb.Add(changeVectorSlice.Content.Ptr, changeVectorSlice.Size);

            tvb.Add(attachment.Size);
            tvb.Add(Bits.SwapBytes((int)Client.Documents.Attachments.RetiredAttachmentFlags.None));
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
                Key = DocumentsStorage.TableValueToString(context, (int)Attachments.AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType, ref tvr),
                Etag = DocumentsStorage.TableValueToEtag((int)Attachments.AttachmentsTable.Etag, ref tvr),
                ChangeVector = DocumentsStorage.TableValueToChangeVector(context, (int)Attachments.AttachmentsTable.ChangeVector, ref tvr),
                Name = DocumentsStorage.TableValueToId(context, (int)Attachments.AttachmentsTable.Name, ref tvr),
                ContentType = DocumentsStorage.TableValueToId(context, (int)Attachments.AttachmentsTable.ContentType, ref tvr),
                TransactionMarker = *(short*)tvr.Read((int)Attachments.AttachmentsTable.TransactionMarker, out int _)
            };

            scope = DocumentsStorage.TableValueToSlice(context, (int)Attachments.AttachmentsTable.Hash, ref tvr, out result.Base64Hash);

            result.Size = GetAttachmentStreamLength(tree, result.Base64Hash);

            return result;
        }
    }
}
