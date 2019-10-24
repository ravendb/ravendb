using System;

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

    public class AttachmentRequest
    {
        public AttachmentRequest(string documentId, string name)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException($"{nameof(documentId)} cannot be null or whitespace.");

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException($"{nameof(name)} cannot be null or whitespace.");

            DocumentId = documentId;
            Name = name;
        }

        public string Name { get; }
        public string DocumentId { get; }
    }
}
