using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents
{
    public class AttachmentOrTombstone
    {
        public Attachment Attachment;
        public Tombstone Tombstone;

        public bool Missing => Attachment == null && Tombstone == null;
        public string ChangeVector => Attachment?.ChangeVector ?? Tombstone?.ChangeVector;

        public static AttachmentOrTombstone GetAttachmentOrTombstone(DocumentsOperationContext context, Slice attachmentKey)
        {
            var attachment = AttachmentsStorage.GetAttachmentByKey(context, attachmentKey);
            if (attachment != null)
            {
                return new AttachmentOrTombstone
                {
                    Attachment = attachment
                };
            }

            var tombstone = AttachmentsStorage.GetAttachmentTombstoneByKey(context, attachmentKey);
            if (tombstone != null)
            {
                return new AttachmentOrTombstone
                {
                    Tombstone = tombstone
                };
            }

            return new AttachmentOrTombstone();
        }
    }
}
