using System.IO;

namespace Raven.Client.Documents.Operations.Attachments
{
    /// <summary>
    /// Represents the result of an attachment enumeration operation, containing the attachment stream and its details.
    /// </summary>
    /// <remarks>
    /// This class encapsulates the metadata and the binary content of an attachment retrieved during enumeration.
    /// </remarks>
    public sealed class AttachmentEnumeratorResult
    {
        /// <summary>
        /// The stream containing the binary content of the attachment.
        /// </summary>
        public Stream Stream { get; }

        /// <summary>
        /// Gets The details of the attachment, including its metadata.
        /// </summary>
        public AttachmentDetails Details { get; }

        public AttachmentEnumeratorResult(AttachmentDetails details, Stream stream)
        {
            Details = details;
            Stream = stream;
        }
    }
}
