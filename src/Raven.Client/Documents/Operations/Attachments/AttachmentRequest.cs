using System;

namespace Raven.Client.Documents.Operations.Attachments;

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