namespace Raven.Client.Documents.Operations.Attachments
{
    internal class AttachmentStreamDetails
    {
        public int Read;
        public long Size;
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

    public class AttachmentRequest
    {
        public AttachmentRequest(string documentId, string name)
        {
            DocumentId = documentId;
            Name = name;
        }

        public string Name { get; }
        public string DocumentId { get; }
    }
}
