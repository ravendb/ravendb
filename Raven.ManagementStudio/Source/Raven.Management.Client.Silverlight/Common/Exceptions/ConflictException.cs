namespace Raven.Management.Client.Silverlight.Common.Exceptions
{
    using System;

    /// <summary>
    /// This exception occurs when a (replication) conflict is encountered.
    /// Usually this required a user to manually resolve the conflict.
    /// </summary>
    public class ConflictException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ConflictException"/> class.
        /// </summary>
        public ConflictException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConflictException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public ConflictException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConflictException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public ConflictException(string message, Exception inner) : base(message, inner)
        {
        }

        /// <summary>
        /// Gets or sets the conflicted version ids.
        /// </summary>
        /// <value>The conflicted version ids.</value>
        public string[] ConflictedVersionIds { get; set; }
    }
}