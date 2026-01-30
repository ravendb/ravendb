using System;
using System.IO;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    /// <summary>
    /// Represents a command for putting (storing) an attachment on a document within a batch operation.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This command is used in batch operations to attach files to documents. It supports both standard local 
    /// attachments and remote attachments that are scheduled for upload to cloud storage (Amazon S3 or Azure Blob Storage).
    /// </para>
    /// <para>
    /// This class implements <see cref="ICommandData"/> and is typically used internally by the
    /// document session when saving changes that include attachment operations.
    /// </para>
    /// </remarks>
    public sealed class PutAttachmentCommandData : ICommandData
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PutAttachmentCommandData"/> class for a local attachment.
        /// </summary>
        /// <param name="documentId">The ID of the document to attach the file to.</param>
        /// <param name="name">The name of the attachment.</param>
        /// <param name="stream">The stream containing the attachment data. Must be seekable and have a known length.</param>
        /// <param name="contentType">The MIME content type of the attachment (e.g., "image/jpeg", "application/pdf").</param>
        /// <param name="changeVector">
        /// Optional change vector for optimistic concurrency control. If provided, the operation will only succeed
        /// if the document's change vector matches. Pass <c>null</c> to skip concurrency checks.
        /// </param>
        /// <remarks>
        /// <para>
        /// This constructor is used for standard local attachments that will be stored directly in the database
        /// without remote cloud storage. The stream must be seekable and have a known length, as these properties 
        /// are required for proper attachment storage.
        /// </para>
        /// <para>
        /// Stream validation is performed during construction. If the stream does not meet requirements,
        /// an exception will be thrown.
        /// </para>
        /// </remarks>
        public PutAttachmentCommandData(string documentId, string name, Stream stream, string contentType, string changeVector)
            : this(documentId, name, stream, contentType, changeVector, size: null, remoteAttachmentParameters: null, hash: null, fromEtl: false)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PutAttachmentCommandData"/> class for a remote attachment.
        /// </summary>
        /// <param name="documentId">The ID of the document to attach the file to.</param>
        /// <param name="name">The name of the attachment.</param>
        /// <param name="stream">The stream containing the attachment data. Must be seekable and have a known length.</param>
        /// <param name="contentType">The MIME content type of the attachment (e.g., "image/jpeg", "application/pdf").</param>
        /// <param name="changeVector">
        /// Optional change vector for optimistic concurrency control. If provided, the operation will only succeed
        /// if the document's change vector matches. Pass <c>null</c> to skip concurrency checks.
        /// </param>
        /// <param name="remoteAttachmentParameters">
        /// Parameters specifying the remote storage configuration and upload schedule. When provided, the attachment
        /// will be stored locally first with remote metadata, then uploaded to the cloud storage provider by a background process.
        /// </param>
        /// <remarks>
        /// <para>
        /// This constructor is used for attachments that should be uploaded to cloud storage (Amazon S3 or Azure Blob Storage).
        /// </para>
        /// </remarks>
        public PutAttachmentCommandData(string documentId, string name, Stream stream, string contentType, string changeVector, RemoteAttachmentParameters remoteAttachmentParameters)
            : this(documentId, name, stream, contentType, changeVector, size: null, remoteAttachmentParameters: remoteAttachmentParameters, hash: null, fromEtl: false)
        {
        }

        internal PutAttachmentCommandData(string documentId, string name, Stream stream, string contentType, string changeVector, long? size, RemoteAttachmentParameters remoteAttachmentParameters, string hash, bool fromEtl)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            Id = documentId;
            Name = name;
            Stream = stream;
            ContentType = contentType;
            ChangeVector = changeVector;
            FromEtl = fromEtl;
            RemoteParameters = remoteAttachmentParameters;
            Hash = hash;

            // when this is called from ETL we know the size in advance
            SizeInBytes = size;

            PutAttachmentCommandHelper.TryValidateStream(stream, RemoteParameters);
        }

        /// <summary>
        /// Gets the ID of the document to which the attachment will be added.
        /// </summary>
        /// <value>The document ID.</value>
        public string Id { get; }

        /// <summary>
        /// Gets the name of the attachment.
        /// </summary>
        /// <remarks>
        /// The attachment name is used as a unique identifier for the attachment within the context of its parent document.
        /// Multiple attachments can be added to a single document, each with a unique name.
        /// </remarks>
        /// <value>The attachment name.</value>
        public string Name { get; }

        /// <summary>
        /// Gets the stream containing the attachment data.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The stream must be seekable and have a known length for proper attachment storage. The attachment data
        /// from this stream is always stored locally in the database initially.
        /// </para>
        /// <para>
        /// If remote parameters are provided, the stream content is stored locally first, then the background
        /// upload process will read it from local storage to upload to cloud storage.
        /// </para>
        /// <para>
        /// The stream is not disposed by this class; the caller is responsible for proper disposal.
        /// </para>
        /// </remarks>
        /// <value>The stream containing the attachment content.</value>
        public Stream Stream { get; }

        /// <summary>
        /// Gets the change vector for optimistic concurrency control.
        /// </summary>
        /// <remarks>
        /// <para>
        /// When specified, the attachment operation will only succeed if the document's current change vector
        /// matches this value. This ensures that the document hasn't been modified since it was last read.
        /// </para>
        /// <para>
        /// If <c>null</c>, no concurrency check is performed, and the attachment will be added regardless
        /// of the document's current state.
        /// </para>
        /// </remarks>
        /// <value>The change vector for concurrency control, or <c>null</c> to skip the check.</value>
        public string ChangeVector { get; }

        /// <summary>
        /// Gets the MIME content type of the attachment.
        /// </summary>
        /// <remarks>
        /// The content type indicates the nature and format of the attachment data. Common examples include:
        /// <list type="bullet">
        /// <item><description>"image/jpeg" for JPEG images</description></item>
        /// <item><description>"application/pdf" for PDF documents</description></item>
        /// <item><description>"video/mp4" for MP4 videos</description></item>
        /// <item><description>"text/plain" for text files</description></item>
        /// </list>
        /// </remarks>
        /// <value>The MIME content type, or <c>null</c> if not specified.</value>
        public string ContentType { get; }

        /// <summary>
        /// Gets the command type identifier.
        /// </summary>
        /// <remarks>
        /// This property always returns <see cref="CommandType.AttachmentPUT"/> to identify this command
        /// as an attachment put operation in batch processing.
        /// </remarks>
        /// <value><see cref="CommandType.AttachmentPUT"/>.</value>
        public CommandType Type { get; } = CommandType.AttachmentPUT;

        /// <summary>
        /// Gets the remote attachment parameters for cloud storage upload configuration.
        /// </summary>
        /// <remarks>
        /// <para>
        /// When this property is not <c>null</c>, the attachment is stored locally in the database with these
        /// remote parameters as metadata. A background work process monitors attachments marked with remote
        /// parameters and uploads them to the configured cloud storage provider (Amazon S3 or Azure Blob Storage)
        /// at the scheduled time specified in the parameters.
        /// </para>
        /// <para>
        /// The two-phase process:
        /// <list type="number">
        /// <item><description><strong>Immediate:</strong> Attachment is stored locally with remote metadata</description></item>
        /// <item><description><strong>Background:</strong> Upload to cloud storage occurs asynchronously</description></item>
        /// </list>
        /// </para>
        /// <para>
        /// After successful upload, the database retains only metadata and a reference to the remote location,
        /// significantly reducing database size for large attachments while keeping them accessible through
        /// transparent retrieval from cloud storage.
        /// </para>
        /// </remarks>
        /// <value>
        /// The remote attachment parameters, or <c>null</c> for standard local-only attachment storage.
        /// </value>
        public RemoteAttachmentParameters RemoteParameters { get; }

        internal string Hash { get; }

        internal bool FromEtl { get; }

        internal long? SizeInBytes { get; }

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            var djv = new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(Name)] = Name,
                [nameof(ContentType)] = ContentType,
                [nameof(ChangeVector)] = ChangeVector,
                [nameof(Type)] = Type.ToString(),
                [nameof(FromEtl)] = FromEtl,
                [nameof(RemoteParameters)] = RemoteParameters?.ToJson(),
                [nameof(Hash)] = Hash
            };

            if (SizeInBytes.HasValue)
            {
                djv[nameof(SizeInBytes)] = SizeInBytes.Value;
            }

            return djv;
        }

        /// <summary>
        /// Called before the session saves changes to allow the command to perform any necessary preparation.
        /// </summary>
        /// <param name="session">The session that is saving changes.</param>
        /// <remarks>
        /// This method is part of the <see cref="ICommandData"/> interface contract. For attachment commands,
        /// no special preparation is needed before save, so this method has no implementation.
        /// </remarks>
        public void OnBeforeSaveChanges(InMemoryDocumentSessionOperations session)
        {
        }
    }
}
