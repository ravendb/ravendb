using System;

namespace Raven.Client.Exceptions.Documents.Attachments;

public sealed class InvalidAttachmentRangeException : RavenException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidAttachmentRangeException"/> class.
    /// </summary>
    public InvalidAttachmentRangeException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidAttachmentRangeException"/> class.
    /// </summary>
    /// <param name="message">The message.</param>
    public InvalidAttachmentRangeException(string message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="InvalidAttachmentRangeException"/> class.
    /// </summary>
    /// <param name="message">The message.</param>
    /// <param name="inner">The inner.</param>
    public InvalidAttachmentRangeException(string message, Exception inner) : base(message, inner)
    {
    }

    public static InvalidAttachmentRangeException ThrowFor(string documentId, string attachmentName, long? rangeFrom, long? rangeTo)
    {
        throw new InvalidAttachmentRangeException($"There requested range '{rangeFrom}-{rangeTo}' is invalid for '{attachmentName}' in document '{documentId}'.");
    }
}
