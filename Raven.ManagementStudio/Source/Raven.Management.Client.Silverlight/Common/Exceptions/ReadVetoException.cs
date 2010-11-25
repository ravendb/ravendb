namespace Raven.Management.Client.Silverlight.Common.Exceptions
{
    using System;

    /// <summary>
    /// This exception is raised whenever a trigger vetos the read by the session
    /// </summary>
    public class ReadVetoException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ReadVetoException"/> class.
        /// </summary>
        public ReadVetoException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ReadVetoException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public ReadVetoException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ReadVetoException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public ReadVetoException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}