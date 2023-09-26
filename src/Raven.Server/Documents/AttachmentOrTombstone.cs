using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Voron;

namespace Raven.Server.Documents
{
    public class AttachmentOrTombstone
    {
        public Attachment Attachment;
        public Tombstone Tombstone;
        public bool Missing => Attachment == null && Tombstone == null;

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

        public static ConflictStatus GetConflictStatus(string remoteAsString, AttachmentOrTombstone attachmentOrTombstone, out string existingChangeVector)
        {
            existingChangeVector = attachmentOrTombstone.Attachment?.ChangeVector ?? attachmentOrTombstone.Tombstone?.ChangeVector;
            return ChangeVectorUtils.GetConflictStatus(remoteAsString, existingChangeVector);
        }
    }
}
