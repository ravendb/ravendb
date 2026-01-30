using System.IO;

namespace Raven.Client.Documents.Operations.Attachments;

/// <summary>
/// Defines the contract for parameters used when storing an attachment in the database.
/// </summary>
/// <remarks>
/// This interface specifies the required and optional properties for attachment storage operations.
/// Implementations can be used with various attachment storage methods including session operations,
/// bulk insert operations, and direct store operations.
/// </remarks>
public interface IStoreAttachmentParameters
{
    /// <summary>
    /// Gets the name of the attachment.
    /// </summary>
    /// <remarks>
    /// The attachment name is used to uniquely identify the attachment within the context of its parent document.
    /// This property is required and cannot be null or whitespace.
    /// </remarks>
    string Name { get; }

    /// <summary>
    /// Gets the stream containing the attachment data.
    /// </summary>
    /// <remarks>
    /// The stream provides the binary content of the attachment to be stored in the database.
    /// This property is required and cannot be null. The caller is responsible for managing the stream's lifetime.
    /// </remarks>
    Stream Stream { get; }

    /// <summary>
    /// Gets or sets the change vector of the attachment for optimistic concurrency control.
    /// </summary>
    /// <remarks>
    /// The change vector can be used to ensure that the attachment is stored only if the current version matches the expected version.
    /// This is useful for preventing concurrent modification conflicts. If null, no concurrency check is performed.
    /// </remarks>
    string ChangeVector { get; set; }

    /// <summary>
    /// Gets or sets the MIME type (Content-Type) of the attachment.
    /// </summary>
    /// <remarks>
    /// Specifies the media type of the attachment content (e.g., "image/png", "application/pdf", "text/plain").
    /// If not specified, RavenDB may attempt to infer the content type from the attachment name or default to "application/octet-stream".
    /// </remarks>
    string ContentType { get; set; }

    /// <summary>
    /// Gets or sets the parameters for scheduling the attachment upload to remote cloud storage.
    /// </summary>
    /// <remarks>
    /// When specified, this property instructs RavenDB to upload the attachment to a configured remote storage destination
    /// at the scheduled time. This is useful for offloading large attachments to cloud storage providers like Amazon S3 or Azure Blob Storage.
    /// If null, the attachment is stored only in the local RavenDB database. See <see cref="RemoteAttachmentParameters"/> for configuration details.
    /// </remarks>
    RemoteAttachmentParameters RemoteParameters { get; set; }
}
