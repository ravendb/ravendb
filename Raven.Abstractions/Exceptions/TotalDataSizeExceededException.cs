using System;
using System.Runtime.Serialization;

namespace Raven.Abstractions.Exceptions
{
    /// <summary>
    /// Exception thrown when first time index population exceeds configured threshold
    /// </summary>
    [Serializable]
    public class TotalDataSizeExceededException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TotalDataSizeExceededException"/> class.
        /// </summary>
        public TotalDataSizeExceededException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TotalDataSizeExceededException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public TotalDataSizeExceededException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TotalDataSizeExceededException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public TotalDataSizeExceededException(string message, Exception inner) : base(message, inner)
        {
        }

#if !DNXCORE50
        /// <summary>
        /// Initializes a new instance of the <see cref="TotalDataSizeExceededException"/> class.
        /// </summary>
        /// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo"/> that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The <see cref="T:System.Runtime.Serialization.StreamingContext"/> that contains contextual information about the source or destination.</param>
        /// <exception cref="T:System.ArgumentNullException">The <paramref name="info"/> parameter is null. </exception>
        /// <exception cref="T:System.Runtime.Serialization.SerializationException">The class name is null or <see cref="P:System.Exception.HResult"/> is zero (0). </exception>

        protected TotalDataSizeExceededException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
#endif
    }
}
