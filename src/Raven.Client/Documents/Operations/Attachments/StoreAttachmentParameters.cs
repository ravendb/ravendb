using System;
using System.IO;

namespace Raven.Client.Documents.Operations.Attachments;

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
    public RetireAttachmentParameters RetireParameters { get; set; }

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