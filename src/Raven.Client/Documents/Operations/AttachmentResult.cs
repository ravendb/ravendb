using System.IO;

namespace Raven.Client.Documents.Operations
{
    public class AttachmentResult
    {
        public Stream Stream;
        public string ContentType;
        public long Etag;
        public string Name;
        public string DocumentId;
    }
}