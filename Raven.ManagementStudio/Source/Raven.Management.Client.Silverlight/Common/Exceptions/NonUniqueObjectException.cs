namespace Raven.Management.Client.Silverlight.Common.Exceptions
{
    using System;

    /// <summary>
    /// This exception is thrown when a separate instance of an entity is added to the session
    /// when a different entity with the same key already exists within the session.
    /// </summary>
    public class NonUniqueObjectException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="NonUniqueObjectException"/> class.
        /// </summary>
        public NonUniqueObjectException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NonUniqueObjectException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public NonUniqueObjectException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NonUniqueObjectException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public NonUniqueObjectException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}