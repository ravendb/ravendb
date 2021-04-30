using System;

namespace Raven.Client.Exceptions.Documents
{
    /// <summary>
    /// This exception is raised when stored document has a collection mismatch
    /// </summary>
    public class DocumentCollectionMismatchException : RavenException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentCollectionMismatchException"/> class.
        /// </summary>
        public DocumentCollectionMismatchException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentCollectionMismatchException"/> class.
        /// </summary>
        /// <param name="message">The exception message.</param>
        public DocumentCollectionMismatchException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentCollectionMismatchException"/> class.
        /// </summary>
        /// <param name="message">The exception message.</param>
        /// <param name="inner">The inner exception.</param>
        public DocumentCollectionMismatchException(string message, Exception inner) : base(message, inner)
        {
        }

        public static DocumentCollectionMismatchException ThrowFor(string documentId, string oldCollection, string newCollection)
        {
            throw new DocumentCollectionMismatchException($"Changing '{documentId}' from '{oldCollection}' to '{newCollection}' via update is not supported. Delete it and recreate the document '{documentId}'.");
        }
    }
}
