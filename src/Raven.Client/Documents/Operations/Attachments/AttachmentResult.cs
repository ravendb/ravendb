using System.IO;
using System.Net.Http;
using Raven.Client.Documents.Commands;

namespace Raven.Client.Documents.Operations.Attachments
{
    /// <summary>
    /// Represents the result of a get attachment operation, containing the attachment stream and details.
    /// </summary>
    /// <remarks>
    /// This class provides access to the binary content of the attachment and its associated metadata.
    /// </remarks>
    public sealed class AttachmentResult : StreamResult
    {
        /// <summary>
        /// The details of the attachment, including its metadata.
        /// </summary>
        public AttachmentDetails Details;

        public AttachmentResult(Stream stream, HttpResponseMessage response) : base(stream, response)
        {

        }
    }
}
