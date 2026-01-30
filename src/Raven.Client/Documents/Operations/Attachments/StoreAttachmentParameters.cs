using System;
using System.IO;

namespace Raven.Client.Documents.Operations.Attachments;

/// <summary>
/// The parameters for storing an attachment in the database.
/// </summary>
public class StoreAttachmentParameters : IStoreAttachmentParameters
{
    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public Stream Stream { get; }

    /// <inheritdoc />
    public string ChangeVector { get; set; }

    /// <inheritdoc />
    public string ContentType { get; set; }

    /// <inheritdoc />
    public RemoteAttachmentParameters RemoteParameters { get; set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="StoreAttachmentParameters"/> class.
    /// </summary>
    /// <param name="name">The name of the attachment to store. Cannot be null or whitespace.</param>
    /// <param name="stream">The stream containing the attachment data. Cannot be null.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="name"/> is null or whitespace, or when <paramref name="stream"/> is null.
    /// </exception>
    /// <remarks>
    /// Use this constructor to create parameters for storing an attachment with the specified name and stream.
    /// Optional properties such as <see cref="ChangeVector"/>, <see cref="ContentType"/>, and <see cref="RemoteParameters"/>
    /// can be set after construction to provide additional control over the attachment storage behavior.
    /// </remarks>
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
