using System;
using System.Runtime.Serialization;

namespace Raven.NewClient.Abstractions.Exceptions
{
    /// <summary>
    /// This exception is raised when the server is asked to perform an operation on an unsupported storage type.
    /// </summary>
    public class StorageNotSupportedException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StorageNotSupportedException"/> class.
        /// </summary>
        public StorageNotSupportedException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StorageNotSupportedException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public StorageNotSupportedException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StorageNotSupportedException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public StorageNotSupportedException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
