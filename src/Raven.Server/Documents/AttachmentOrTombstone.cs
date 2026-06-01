using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents
{
    // At most one of Attachment / Tombstone is non-null; both null means Missing.
    public readonly struct AttachmentOrTombstone
    {
        public readonly Attachment Attachment;
        public readonly Tombstone Tombstone;

        private AttachmentOrTombstone(Attachment attachment, Tombstone tombstone)
        {
            Attachment = attachment;
            Tombstone = tombstone;
        }

        public static AttachmentOrTombstone Of(Attachment attachment) => new(attachment, tombstone: null);

        public static AttachmentOrTombstone Of(Tombstone tombstone) => new(attachment: null, tombstone);

        public static AttachmentOrTombstone Empty => default;

        public bool Missing => Attachment == null && Tombstone == null;

        public string ChangeVector => Attachment?.ChangeVector ?? Tombstone?.ChangeVector;

        // Live table first, then tombstones.
        public static AttachmentOrTombstone GetAttachmentOrTombstone(DocumentsOperationContext context, Slice attachmentKey)
        {
            AttachmentsStorage storage = context.DocumentDatabase.DocumentsStorage.AttachmentsStorage;
            Attachment attachment = storage.GetAttachmentByKey(context, attachmentKey);
            if (attachment != null)
                return Of(attachment);

            Tombstone tombstone = storage.GetAttachmentTombstoneByKey(context, attachmentKey);
            return tombstone != null ? Of(tombstone) : Empty;
        }

        // Dual-form live, then dual-form tombstones (each per-pair helper handles both probes internally).
        internal static AttachmentOrTombstone GetRevisionAttachmentOrTombstone(DocumentsOperationContext context, in RevisionAttachmentKey pair)
        {
            AttachmentsStorage storage = context.DocumentDatabase.DocumentsStorage.AttachmentsStorage;
            Attachment attachment = storage.GetRevisionAttachmentByPair(context, in pair);
            if (attachment != null)
                return Of(attachment);

            Tombstone tombstone = storage.GetRevisionAttachmentTombstoneByPair(context, in pair);
            return tombstone != null ? Of(tombstone) : Empty;
        }
    }
}
