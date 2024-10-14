using System;
using System.IO;

namespace Raven.Client.Documents.Operations.Attachments
{
    /// <summary>
    /// Represents the result of a get attachment operation, containing the attachment stream and details.
    /// </summary>
    /// <remarks>
    /// This class provides access to the binary content of the attachment and its associated metadata.
    /// </remarks>
    public sealed class AttachmentResult : IDisposable
    {
        /// <summary>
        /// The stream containing the binary content of the attachment.
        /// </summary>
        public Stream Stream;

        /// <summary>
        /// The details of the attachment, including its metadata.
        /// </summary>
        public AttachmentDetails Details;

        public void Dispose()
        {
            Stream?.Dispose();
            Stream = null;
        }
    }
}
