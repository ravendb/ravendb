namespace Raven.Client.Documents.Operations.Attachments
{
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