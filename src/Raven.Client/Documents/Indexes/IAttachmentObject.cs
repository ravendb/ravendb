using System;
using System.IO;
using System.Text;
using Raven.Client.Documents.Attachments;

namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// Represents an attachment object accessible within index definitions for indexing attachment content and metadata.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This interface is used primarily in static index definitions to access attachment data during indexing.
    /// It provides access to both attachment metadata (name, content type, size, hash) and content
    /// (as string or stream), enabling full-text search and custom processing of attachments.
    /// </para>
    /// <para>
    /// The interface also supports remote attachments stored in cloud storage (Amazon S3 or Azure Blob Storage)
    /// by exposing remote-specific metadata such as upload schedule, storage location identifier, and status flags.
    /// </para>
    /// <para>
    /// <strong>Remote Attachments Limitation:</strong>
    /// </para>
    /// <para>
    /// Remote attachments that have been uploaded to cloud storage (Amazon S3 or Azure Blob Storage) cannot
    /// have their content accessed during indexing. Attempting to call <see cref="GetContentAsString()"/>,
    /// <see cref="GetContentAsString(Encoding)"/>, or <see cref="GetContentAsStream()"/> on a remote attachment
    /// will throw <see cref="RemoteAttachmentIndexingException"/>.
    /// </para>
    /// <para>
    /// This limitation exists because:
    /// <list type="bullet">
    /// <item><description>Remote attachment content is no longer stored locally in the database</description></item>
    /// <item><description>Retrieving content from cloud storage during indexing would introduce significant latency</description></item>
    /// <item><description>Frequent cloud storage access would incur costs and rate limiting concerns</description></item>
    /// <item><description>Indexing operations need to be fast and deterministic</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// <strong>Best Practices:</strong>
    /// </para>
    /// <list type="bullet">
    /// <item><description>Check <see cref="RemoteFlags"/> before accessing content to avoid exceptions</description></item>
    /// <item><description>Index only attachment metadata (name, content type, size, hash) for remote attachments</description></item>
    /// <item><description>Index content only for local attachments where <see cref="RemoteFlags"/> is <see cref="RemoteAttachmentFlags.None"/></description></item>
    /// </list>
    /// </remarks>
    public interface IAttachmentObject
    {
        /// <summary>
        /// Gets the scheduled date and time when the attachment should be uploaded to cloud storage.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This property is only populated for attachments that have been marked for remote storage
        /// using <see cref="RemoteAttachmentParameters"/>. It indicates when the background upload process
        /// should attempt to upload the attachment to the configured cloud storage provider.
        /// </para>
        /// <para>
        /// For standard local attachments or attachments that have already been uploaded to remote storage,
        /// this property returns <c>null</c>.
        /// </para>
        /// </remarks>
        /// <value>
        /// The scheduled upload time in UTC, or <c>null</c> if the attachment is not scheduled for remote upload
        /// or has already been uploaded.
        /// </value>
        /// <seealso cref="RemoteFlags"/>
        /// <seealso cref="RemoteIdentifier"/>
        public DateTime? RemoteAt { get; }

        /// <summary>
        /// Gets flags indicating the location and characteristics of the attachment.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This property uses the <see cref="RemoteAttachmentFlags"/> enum to indicate whether the attachment
        /// is stored locally in the database or has been uploaded to remote cloud storage.
        /// </para>
        /// <para>
        /// Possible values:
        /// <list type="bullet">
        /// <item><description><see cref="RemoteAttachmentFlags.None"/> - The attachment is stored locally in the database</description></item>
        /// <item><description><see cref="RemoteAttachmentFlags.Remote"/> - The attachment has been uploaded to cloud storage</description></item>
        /// </list>
        /// </para>
        /// <para>
        /// This information is useful in index definitions to differentiate between local and remote attachments,
        /// enabling different processing logic or filtering based on storage location.
        /// </para>
        /// </remarks>
        /// <value>
        /// A <see cref="RemoteAttachmentFlags"/> value indicating the attachment's storage location and status.
        /// </value>
        /// <seealso cref="RemoteIdentifier"/>
        /// <seealso cref="RemoteAt"/>
        public RemoteAttachmentFlags RemoteFlags { get; }

        /// <summary>
        /// Gets the identifier of the remote storage destination where the attachment is or will be stored.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This property contains the identifier (key/name) of the destination configuration defined in
        /// <see cref="RemoteAttachmentsConfiguration"/>. This identifier specifies which cloud storage
        /// destination (S3 or Azure) the attachment is associated with.
        /// </para>
        /// <para>
        /// The identifier is used by the background upload process to determine which cloud storage
        /// configuration to use when uploading the attachment, and by the retrieval process to locate
        /// the attachment when accessing remote content.
        /// </para>
        /// <para>
        /// For standard local attachments that are not scheduled for remote storage,
        /// this property returns <c>null</c>.
        /// </para>
        /// </remarks>
        /// <value>
        /// The remote storage destination identifier, or <c>null</c> if the attachment is not associated
        /// with remote storage.
        /// </value>
        /// <seealso cref="RemoteFlags"/>
        /// <seealso cref="RemoteAt"/>
        public string RemoteIdentifier { get; }

        /// <summary>
        /// Gets the name of the attachment.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The attachment name serves as a unique identifier within the context of its parent document.
        /// Multiple attachments can be associated with a single document, each with a unique name.
        /// </para>
        /// <para>
        /// This property is commonly used in index definitions to create searchable fields based on
        /// attachment names or to filter documents by attachment name patterns.
        /// </para>
        /// </remarks>
        /// <value>The attachment name.</value>
        public string Name { get; }

        /// <summary>
        /// Gets the hash of the attachment content.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The hash is a base64-encoded string representing a cryptographic hash of the attachment content.
        /// </para>
        /// <para>
        /// The hash is calculated automatically by RavenDB when the attachment is stored and remains constant
        /// as long as the content doesn't change.
        /// </para>
        /// </remarks>
        /// <value>The hash of the attachment content.</value>
        public string Hash { get; }

        /// <summary>
        /// Gets the MIME content type of the attachment.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The content type indicates the nature and format of the attachment data. 
        /// </para>
        /// <para>
        /// This property is useful in index definitions for filtering or categorizing documents based on
        /// the types of attachments they contain.
        /// </para>
        /// </remarks>
        /// <value>The MIME content type, or <c>null</c> if not specified.</value>
        public string ContentType { get; }

        /// <summary>
        /// Gets the size of the attachment in bytes.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This property represents the total size of the attachment content in bytes. It can be used in
        /// index definitions for:
        /// <list type="bullet">
        /// <item><description>Filtering documents by attachment size (e.g., documents with large attachments)</description></item>
        /// <item><description>Calculating total attachment storage per document</description></item>
        /// <item><description>Creating size-based categories or statistics</description></item>
        /// </list>
        /// </para>
        /// <para>
        /// For remote attachments that have been uploaded to cloud storage, this property still reflects
        /// the original size of the attachment content, even though the content itself may no longer be
        /// stored locally in the database.
        /// </para>
        /// </remarks>
        /// <value>The size of the attachment in bytes.</value>
        public long Size { get; }

        /// <summary>
        /// Gets the attachment content as a string using UTF-8 encoding.
        /// </summary>
        /// <returns>
        /// The attachment content decoded as a UTF-8 string.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method retrieves the attachment content and decodes it using UTF-8 encoding.
        /// </para>
        /// <para>
        /// <strong>Remote Attachments Restriction:</strong>
        /// </para>
        /// <para>
        /// This method <strong>cannot be called for remote attachments</strong>. If the attachment has been
        /// uploaded to cloud storage (<see cref="RemoteFlags"/> is <see cref="RemoteAttachmentFlags.Remote"/>),
        /// calling this method will throw an exception.
        /// </para>
        /// <para>
        /// Always check <see cref="RemoteFlags"/> before calling this method to avoid exceptions:
        /// </para>
        /// <code>
        /// if (attachment.RemoteFlags == RemoteAttachmentFlags.None)
        /// {
        ///     var content = attachment.GetContentAsString();
        /// }
        /// </code>
        /// <para>
        /// <strong>Performance Considerations:</strong>
        /// </para>
        /// <list type="bullet">
        /// <item><description>Reading large attachments as strings can be memory-intensive</description></item>
        /// <item><description>The result may be cached by the implementation to avoid re-reading the stream</description></item>
        /// </list>
        /// <para>
        /// For binary attachments or large files, consider using <see cref="GetContentAsStream"/> instead
        /// to avoid loading the entire content into memory as a string.
        /// </para>
        /// </remarks>
        /// <exception cref="InvalidOperationException">
        /// Thrown when attempting to access content of a remote attachment that has been uploaded to cloud storage
        /// and is no longer available locally. The specific exception type may vary by implementation.
        /// </exception>
        /// <seealso cref="GetContentAsString(Encoding)"/>
        /// <seealso cref="GetContentAsStream"/>
        /// <seealso cref="RemoteFlags"/>
        public string GetContentAsString();

        /// <summary>
        /// Gets the attachment content as a string using the specified encoding.
        /// </summary>
        /// <param name="encoding">The encoding to use for decoding the attachment content.</param>
        /// <returns>
        /// The attachment content decoded using the specified encoding.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method retrieves the attachment content and decodes it using the specified encoding.
        /// It provides flexibility for handling text attachments that may not use UTF-8 encoding.
        /// </para>
        /// <para>
        /// <strong>Remote Attachments Restriction:</strong>
        /// </para>
        /// <para>
        /// This method <strong>cannot be called for remote attachments</strong>. If the attachment has been
        /// uploaded to cloud storage (<see cref="RemoteFlags"/> is <see cref="RemoteAttachmentFlags.Remote"/>),
        /// calling this method will throw an exception before any encoding validation occurs.
        /// </para>
        /// <para>
        /// Always check <see cref="RemoteFlags"/> before calling this method to avoid exceptions:
        /// </para>
        /// <code>
        /// if (attachment.RemoteFlags == RemoteAttachmentFlags.None)
        /// {
        ///     var content = attachment.GetContentAsString(encoding);
        /// }
        /// </code>
        /// <para>
        /// <strong>Performance Considerations:</strong>
        /// </para>
        /// <list type="bullet">
        /// <item><description>Reading large attachments as strings can be memory-intensive</description></item>
        /// </list>
        /// </remarks>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="encoding"/> is <c>null</c>.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when attempting to access content of a remote attachment that has been uploaded to cloud storage
        /// and is no longer available locally. This exception is thrown before encoding validation. The specific 
        /// exception type may vary by implementation.
        /// </exception>
        /// <seealso cref="GetContentAsString()"/>
        /// <seealso cref="GetContentAsStream"/>
        /// <seealso cref="RemoteFlags"/>
        public string GetContentAsString(Encoding encoding);

        /// <summary>
        /// Gets the attachment content as a stream for reading binary or large content.
        /// </summary>
        /// <returns>
        /// A <see cref="Stream"/> containing the attachment content. The caller must not dispose the stream
        /// as it is owned by the attachment object.
        /// </returns>
        /// <remarks>
        /// <para>
        /// This method provides access to the attachment's underlying stream for reading binary content or
        /// for memory-efficient processing of large attachments.
        /// </para>
        /// <para>
        /// <strong>Remote Attachments Restriction:</strong>
        /// </para>
        /// <para>
        /// This method <strong>cannot be called for remote attachments</strong>. If the attachment has been
        /// uploaded to cloud storage (<see cref="RemoteFlags"/> is <see cref="RemoteAttachmentFlags.Remote"/>),
        /// calling this method will throw an exception.
        /// </para>
        /// <para>
        /// Always check <see cref="RemoteFlags"/> before calling this method to avoid exceptions:
        /// </para>
        /// <code>
        /// if (attachment.RemoteFlags == RemoteAttachmentFlags.None)
        /// {
        ///     var stream = attachment.GetContentAsStream();
        /// }
        /// </code>
        /// <para>
        /// <strong>Important Stream Handling:</strong>
        /// </para>
        /// <list type="bullet">
        /// <item><description>The returned stream is owned by the attachment object and must NOT be disposed by the caller</description></item>
        /// <item><description>Disposing the stream will cause subsequent operations to fail</description></item>
        /// <item><description>The stream position may be reset to the beginning before returning</description></item>
        /// <item><description>This is the preferred method for large attachments or binary content</description></item>
        /// </list>
        /// </remarks>
        /// <exception cref="InvalidOperationException">
        /// Thrown when attempting to access content of a remote attachment that has been uploaded to cloud storage
        /// and is no longer available locally. The specific exception type may vary by implementation.
        /// </exception>
        /// <seealso cref="GetContentAsString()"/>
        /// <seealso cref="GetContentAsString(Encoding)"/>
        /// <seealso cref="RemoteFlags"/>
        public Stream GetContentAsStream();
    }
}
