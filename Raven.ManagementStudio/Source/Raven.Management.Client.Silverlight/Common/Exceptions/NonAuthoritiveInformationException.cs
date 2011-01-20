namespace Raven.Management.Client.Silverlight.Common.Exceptions
{
    using System;

    /// <summary>
    /// This exception is raised when a non authoritive information is encountered
    /// </summary>
    public class NonAuthoritiveInformationException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="NonAuthoritiveInformationException"/> class.
        /// </summary>
        public NonAuthoritiveInformationException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NonAuthoritiveInformationException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public NonAuthoritiveInformationException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NonAuthoritiveInformationException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public NonAuthoritiveInformationException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}