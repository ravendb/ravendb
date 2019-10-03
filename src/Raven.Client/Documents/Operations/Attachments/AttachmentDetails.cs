using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.Attachments
{
    public class AttachmentsDetails
    {
        public List<AttachmentAdvancedDetails> AttachmentsMetadata;
    }

    public class AttachmentAdvancedDetails : AttachmentDetails
    {
        public int Index;
        public int Read = default;
    }

    public class AttachmentDetails : AttachmentName
    {
        public string ChangeVector;
        public string DocumentId;
    }

    public class AttachmentName
    {
        public string Name;
        public string Hash;
        public string ContentType;
        public long Size;
    }
}
