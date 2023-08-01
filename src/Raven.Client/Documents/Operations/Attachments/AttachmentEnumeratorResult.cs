using System.IO;

namespace Raven.Client.Documents.Operations.Attachments
{
    public sealed class AttachmentEnumeratorResult
    {
        public Stream Stream { get; }

        public AttachmentDetails Details { get; }

        public AttachmentEnumeratorResult(AttachmentDetails details, Stream stream)
        {
            Details = details;
            Stream = stream;
        }
    }
}
