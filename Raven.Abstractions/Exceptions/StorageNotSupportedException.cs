using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.Exceptions
{
    /// <summary>
    /// This exception is raised when the server is asked to perform an operation on an unsupported storage type.
    /// </summary>
    [Serializable]
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

        protected StorageNotSupportedException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
