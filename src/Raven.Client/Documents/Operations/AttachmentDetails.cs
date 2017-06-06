namespace Raven.Client.Documents.Operations
{
    public class AttachmentDetails : AttachmentName
    {
        public long Etag;
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