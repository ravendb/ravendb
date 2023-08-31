using System;

namespace Raven.Client.Exceptions.Documents.Attachments
{
    public sealed class AttachmentDoesNotExistException : RavenException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AttachmentDoesNotExistException"/> class.
        /// </summary>
        public AttachmentDoesNotExistException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AttachmentDoesNotExistException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public AttachmentDoesNotExistException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AttachmentDoesNotExistException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public AttachmentDoesNotExistException(string message, Exception inner) : base(message, inner)
        {
        }

        public static AttachmentDoesNotExistException ThrowFor(string documentId, string attachmentName)
        {
            throw new AttachmentDoesNotExistException($"There is no attachment with '{attachmentName}' name for document '{documentId}'.");
        }
    }
}
