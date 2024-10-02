using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Attachments
{
    /// <summary>
    /// Contains details about an attachment, including its change vector and associated document ID.
    /// </summary>
    /// <remarks>
    /// This class inherits from <see cref="AttachmentName"/> and provides additional metadata
    /// necessary for managing attachment operations.
    /// </remarks>
    public class AttachmentDetails : AttachmentName
    {
        /// <summary>
        /// The change vector of the attachment for concurrency control.
        /// </summary>
        public string ChangeVector;

        /// <summary>
        /// The ID of the document associated with the attachment.
        /// </summary>
        public string DocumentId;
    }

    internal sealed class AttachmentNameWithCount : AttachmentName
    {
        public long Count { get; set; }

        internal override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Count)] = Count;

            return json;
        }
    }

    /// <summary>
    /// Represents the basic information of an attachment, including its name, hash, content type, and size.
    /// </summary>
    /// <remarks>
    /// This class serves as a base for more detailed attachment information, providing essential properties
    /// for managing attachments within the database.
    /// </remarks>
    public class AttachmentName
    {
        /// <summary>
        /// The name of the attachment.
        /// </summary>
        public string Name;

        /// <summary>
        /// The hash of the attachment content for integrity verification.
        /// </summary>
        public string Hash;

        /// <summary>
        /// The MIME type of the attachment.
        /// </summary>
        public string ContentType;

        /// <summary>
        /// The size of the attachment in bytes.
        /// </summary>
        public long Size;

        internal virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Hash)] = Hash,
                [nameof(ContentType)] = ContentType,
                [nameof(Size)] = Size
            };
        }
    }

    /// <summary>
    /// Represents a request to retrieve an attachment associated with a document.
    /// </summary>
    /// <remarks>
    /// This class encapsulates the necessary identifiers for an attachment operation,
    /// ensuring that valid parameters are provided during instantiation.
    /// </remarks>
    public sealed class AttachmentRequest
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="AttachmentRequest"/> class.
        /// </summary>
        /// <param name="documentId">The ID of the document associated with the attachment.</param>
        /// <param name="name">The name of the attachment.</param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="documentId"/> or <paramref name="name"/> is null or whitespace.
        /// </exception>
        public AttachmentRequest(string documentId, string name)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException($"{nameof(documentId)} cannot be null or whitespace.");

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException($"{nameof(name)} cannot be null or whitespace.");

            DocumentId = documentId;
            Name = name;
        }

        /// <summary>
        /// Gets the name of the attachment.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the ID of the document associated with the attachment.
        /// </summary>
        public string DocumentId { get; }
    }
}
