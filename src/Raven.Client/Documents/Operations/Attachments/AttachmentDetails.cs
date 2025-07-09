using System;
using System.IO;
using Raven.Client.Documents.Attachments;
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
        public long RegularHashes { get; set; }
        public long RetiredCount { get; set; }
        public long Count { get; set; }

        internal override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(RegularHashes)] = RegularHashes;
            json[nameof(RetiredCount)] = RetiredCount;
            json[nameof(Count)] = Count;

            return json;
        }
    }

    public interface IStoreAttachmentParameters
    {
        /// <summary>
        /// The name of the attachment.
        /// </summary>
        string Name { get; set; }

        /// <summary>
        /// The stream of the attachment.
        /// </summary>
        Stream Stream { get; set; }

        /// <summary>
        /// The change vector of the attachment for concurrency control.
        /// </summary>
        string ChangeVector { get; set; }

        /// <summary>
        /// The MIME type of the attachment.
        /// </summary>
        string ContentType { get; set; }

        /// <summary>
        /// The date to upload the attachment to cloud.
        /// </summary>
        DateTime? RetireAt { get; set; }
    }

    /// <summary>
    /// The parameters for storing an attachment in the database.
    /// </summary>
    public class StoreAttachmentParameters : IStoreAttachmentParameters
    {
        /// <inheritdoc />
        public string Name { get; set; }
        /// <inheritdoc />
        public Stream Stream { get; set; }

        /// <inheritdoc />
        public string ChangeVector { get; set; }

        /// <inheritdoc />
        public string ContentType { get; set; }

        /// <inheritdoc />
        public DateTime? RetireAt { get; set; }

        public StoreAttachmentParameters(string name, Stream stream)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name), "Attachment name cannot be null or whitespace.");
            if (stream == null)
                throw new ArgumentNullException(nameof(stream), "Attachment stream cannot be null.");

            Name = name;
            Stream = stream;
        }

        internal StoreAttachmentParameters()
        {
            // Parameterless constructor for serialization
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
        public AttachmentFlags Flags;
        public DateTime? RetireAt;

        internal virtual DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Hash)] = Hash,
                [nameof(ContentType)] = ContentType,
                [nameof(Size)] = Size
            };
            json[nameof(Flags)] = Flags.ToString();
            json[nameof(RetireAt)] = RetireAt;
            return json;
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


        internal AttachmentFlags Flags;
    }
}
