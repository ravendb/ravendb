using System;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Replication.ReplicationItems
{
    public sealed class TombstoneReplicationItem
    {
        public static ReplicationBatchItem From(DocumentsOperationContext context, Tombstone doc)
        {
            switch (doc.Type)
            {
                case Tombstone.TombstoneType.Document:
                    return new DocumentReplicationItem
                    {
                        Type = ReplicationBatchItem.ReplicationItemType.DocumentTombstone,
                        Etag = doc.Etag,
                        Id = doc.LowerId,
                        TransactionMarker = doc.TransactionMarker,
                        ChangeVector = doc.ChangeVector,
                        Collection = doc.Collection,
                        Flags = doc.Flags,
                        LastModifiedTicks = doc.LastModified.Ticks
                    };

                case Tombstone.TombstoneType.Attachment:
                    return BuildAttachmentTombstone(context, doc);

                case Tombstone.TombstoneType.Revision:
                    return BuildRevisionTombstone(context, doc);

                default:
                    throw new ArgumentOutOfRangeException(nameof(doc.Type));
            }
        }

        private static RevisionTombstoneReplicationItem BuildRevisionTombstone(DocumentsOperationContext context, Tombstone doc)
        {
            // Sender-side Legacy-form rebuild: RawComposite carries rawCv regardless of on-disk form.
            using (RevisionsStorage.BuildRevisionTombstoneKeyForExternal(context, doc, out RevisionTombstoneKey tombstoneKey))
            {
                return new RevisionTombstoneReplicationItem
                {
                    Type = ReplicationBatchItem.ReplicationItemType.RevisionTombstone,
                    Etag = doc.Etag,
                    TransactionMarker = doc.TransactionMarker,
                    ChangeVector = doc.ChangeVector,
                    Collection = doc.Collection,
                    Flags = doc.Flags,
                    Id = context.GetLazyString(tombstoneKey.RawComposite)
                };
            }
        }

        private static unsafe AttachmentTombstoneReplicationItem BuildAttachmentTombstone(DocumentsOperationContext context, Tombstone doc)
        {
            var item = new AttachmentTombstoneReplicationItem
            {
                Type = ReplicationBatchItem.ReplicationItemType.AttachmentTombstone,
                Etag = doc.Etag,
                TransactionMarker = doc.TransactionMarker,
                ChangeVector = doc.ChangeVector,
                Flags = doc.Flags,
                LastModifiedTicks = doc.LastModified.Ticks,
            };

            var attachmentsStorage = context.DocumentDatabase.DocumentsStorage.AttachmentsStorage;
            if (AttachmentsStorage.AttachmentKey.GetAttachmentType(doc.LowerId) == AttachmentType.Revision)
            {
                // Sender-side Legacy-form rebuild for revision-attachment tombstones.
                item.ToDispose(attachmentsStorage.BuildAttachmentRevisionTombstoneKey(context, doc, out item.Key));
            }
            else
            {
                item.ToDispose(Slice.From(context.Allocator, doc.LowerId.Buffer, doc.LowerId.Size, ByteStringType.Immutable, out item.Key));
            }
            return item;
        }
    }
}
