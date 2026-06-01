using System;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Documents.Schemas.Attachments;
using static Raven.Server.Documents.Schemas.Revisions;

namespace SlowTests.Server.Documents.Revisions
{
    // Voron-level seeders writing legacy L-shape rows -- copy-equivalent to v6.2 pre-PR-22358 storage.
    internal static class RevisionLegacyRowSeeder
    {
        internal static unsafe void SeedLegacyRevisionRow(
            DocumentsOperationContext context,
            DocumentDatabase database,
            string id,
            string collection,
            Raven.Server.Utils.ChangeVector compoundCv,
            long etag)
        {
            CollectionName collectionName = database.DocumentsStorage.ExtractCollectionName(context, collection);
            Table table = database.DocumentsStorage.RevisionsStorage.EnsureRevisionTableCreated(
                context.Transaction.InnerTransaction, collectionName);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, id, out Slice lowerId, out Slice idSlice))
            using (RevisionsStorage.BuildRevisionKey(context.Allocator, compoundCv, out RevisionKey key))
            using (BlittableJsonReaderObject docBlittable = BuildBlittableDocument(context, name: "Legacy"))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(key.Raw.Content.Ptr, key.Raw.Size);          // 0  raw cv.Version (legacy shape)
                tvb.Add(lowerId);                                               // 1
                tvb.Add(SpecialChars.RecordSeparator);                          // 2
                tvb.Add(Bits.SwapBytes(etag));                                  // 3
                tvb.Add(idSlice);                                               // 4
                tvb.Add(docBlittable.BasePointer, docBlittable.Size);           // 5
                tvb.Add((int)DocumentFlags.Revision);                           // 6
                tvb.Add(0L);                                                    // 7 NotDeletedRevisionMarker
                long ticks = DateTime.UtcNow.Ticks;
                tvb.Add(ticks);                                                 // 8
                tvb.Add(context.GetTransactionMarker());                        // 9
                tvb.Add(0);                                                     // 10 Resolved
                tvb.Add(Bits.SwapBytes(ticks));                                 // 11

                table.Insert(tvb);
            }
        }

        internal static unsafe void SeedLegacyRevisionTombstoneRow(
            DocumentsOperationContext context,
            DocumentDatabase database,
            string docId,
            string collection,
            Raven.Server.Utils.ChangeVector deletedRevisionCv,
            string tombstoneOwnCv,
            long deletedRevisionEtag)
        {
            CollectionName collectionName = database.DocumentsStorage.ExtractCollectionName(context, collection);
            Table table = context.Transaction.InnerTransaction.OpenTable(
                database.DocumentsStorage.TombstonesSchema, RevisionsTombstonesSlice);
            long newEtag = database.DocumentsStorage.GenerateNextEtag();

            using (RevisionsStorage.BuildRevisionKeys(context, deletedRevisionCv, docId, out RevisionKeys keys))
            using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
            using (Slice.From(context.Allocator, tombstoneOwnCv, out Slice cv))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                // Field 0: raw composite PK (legacy shape).
                tvb.Add(keys.Tombstone.RawComposite.Content.Ptr, keys.Tombstone.RawComposite.Size);
                tvb.Add(Bits.SwapBytes(newEtag));                               // 1 Etag
                tvb.Add(Bits.SwapBytes(deletedRevisionEtag));                   // 2 DeletedEtag
                tvb.Add(context.GetTransactionMarker());                        // 3 TransactionMarker
                tvb.Add((byte)Tombstone.TombstoneType.Revision);                // 4 Type
                tvb.Add(collectionSlice);                                       // 5 Collection
                tvb.Add((int)DocumentFlags.None);                               // 6 Flags
                tvb.Add(cv.Content.Ptr, cv.Size);                               // 7 ChangeVector
                tvb.Add(DateTime.UtcNow.Ticks);                                 // 8 LastModified

                table.Insert(tvb);
            }
        }

        internal static unsafe void SeedLegacyRevisionAttachmentRow(
            DocumentsOperationContext context,
            DocumentDatabase database,
            string docId,
            Raven.Server.Utils.ChangeVector parentRevisionCv,
            string attachmentName,
            string base64Hash,
            string contentType,
            string attachmentOwnCv)
        {
            Table table = context.Transaction.InnerTransaction.OpenTable(
                database.DocumentsStorage.AttachmentsStorage.AttachmentsSchema, AttachmentsMetadataSlice);
            long newEtag = database.DocumentsStorage.GenerateNextEtag();

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, docId, out Slice lowerId, out _))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, attachmentName, out Slice lowerName, out Slice nameStorage))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, contentType, out Slice lowerCt, out Slice ctStorage))
            using (Slice.From(context.Allocator, base64Hash, out Slice hashSlice))
            using (RevisionsStorage.BuildRevisionKey(context.Allocator, parentRevisionCv, out RevisionKey parentRevKey))
            using (AttachmentsStorage.BuildRevisionAttachmentKey(
                       context, in parentRevKey,
                       lowerId.Content.Ptr, lowerId.Size,
                       lowerName.Content.Ptr, lowerName.Size,
                       hashSlice,
                       lowerCt.Content.Ptr, lowerCt.Size,
                       out RevisionAttachmentKey pair))
            using (Slice.From(context.Allocator, attachmentOwnCv, out Slice cvSlice))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(pair.RawComposite.Content.Ptr, pair.RawComposite.Size); // 0 raw composite PK
                tvb.Add(Bits.SwapBytes(newEtag));                                            // 1 Etag
                tvb.Add(nameStorage.Content.Ptr, nameStorage.Size);                          // 2 Name
                tvb.Add(ctStorage.Content.Ptr, ctStorage.Size);                              // 3 ContentType
                tvb.Add(hashSlice.Content.Ptr, hashSlice.Size);                              // 4 Hash
                tvb.Add(context.GetTransactionMarker());                                     // 5 TransactionMarker
                tvb.Add(cvSlice.Content.Ptr, cvSlice.Size);                                  // 6 ChangeVector
                // v7.2 schema baseline -- pre-22358 RA rows already had Size/Flags/RemoteAt/Identifier;
                // AttachmentsFlagAndHashSlice dynamic index requires Flags so the row must include it.
                tvb.Add(0L);                                                                 // 7 Size (test row has no stream)
                tvb.Add(Bits.SwapBytes((int)Raven.Client.Documents.Attachments.RemoteAttachmentFlags.None)); // 8 Flags
                tvb.Add(-1L);                                                                // 9 RemoteAt (sentinel for "no remote")
                tvb.Add(Slices.Empty.Content.Ptr, Slices.Empty.Size);                        // 10 Identifier

                table.Insert(tvb);
            }
        }

        internal static unsafe void SeedLegacyRevisionAttachmentTombstoneRow(
            DocumentsOperationContext context,
            DocumentDatabase database,
            string docId,
            Raven.Server.Utils.ChangeVector parentRevisionCv,
            string attachmentName,
            string base64Hash,
            string contentType,
            string tombstoneOwnCv,
            long attachmentEtag)
        {
            Table table = context.Transaction.InnerTransaction.OpenTable(
                database.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);
            long newEtag = database.DocumentsStorage.GenerateNextEtag();

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, docId, out Slice lowerId, out _))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, attachmentName, out Slice lowerName, out _))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, contentType, out Slice lowerCt, out _))
            using (Slice.From(context.Allocator, base64Hash, out Slice hashSlice))
            using (RevisionsStorage.BuildRevisionKey(context.Allocator, parentRevisionCv, out RevisionKey parentRevKey))
            using (AttachmentsStorage.BuildRevisionAttachmentKey(
                       context, in parentRevKey,
                       lowerId.Content.Ptr, lowerId.Size,
                       lowerName.Content.Ptr, lowerName.Size,
                       hashSlice,
                       lowerCt.Content.Ptr, lowerCt.Size,
                       out RevisionAttachmentKey pair))
            using (Slice.From(context.Allocator, tombstoneOwnCv, out Slice cv))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(pair.RawComposite.Content.Ptr, pair.RawComposite.Size); // 0 raw composite PK
                tvb.Add(Bits.SwapBytes(newEtag));                                            // 1 Etag
                tvb.Add(Bits.SwapBytes(attachmentEtag));                                     // 2 DeletedEtag
                tvb.Add(context.GetTransactionMarker());                                     // 3 TransactionMarker
                tvb.Add((byte)Tombstone.TombstoneType.Attachment);                           // 4 Type
                tvb.Add(null, 0);                                                            // 5 Collection (mirrors CreateRevisionAttachmentTombstone)
                tvb.Add((int)DocumentFlags.None);                                            // 6 Flags
                tvb.Add(cv.Content.Ptr, cv.Size);                                            // 7 ChangeVector
                tvb.Add(DateTime.UtcNow.Ticks);                                              // 8 LastModified

                table.Insert(tvb);
            }
        }

        internal static BlittableJsonReaderObject BuildBlittableDocument(DocumentsOperationContext context, string name)
        {
            DynamicJsonValue djv = new DynamicJsonValue
            {
                ["Name"] = name,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Collection] = "Users"
                }
            };
            return context.ReadObject(djv, "doc");
        }
    }
}
