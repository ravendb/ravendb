using System;

namespace Raven.NewClient.Client.Http
{
    /// <summary>
    /// This exception is raised when a bad request is made to the server
    /// </summary>
    public class InternalServerErrorException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="InternalServerErrorException"/> class.
        /// </summary>
        public InternalServerErrorException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InternalServerErrorException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public InternalServerErrorException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InternalServerErrorException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public InternalServerErrorException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}