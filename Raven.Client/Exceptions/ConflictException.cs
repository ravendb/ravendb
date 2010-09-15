using System;
using System.Runtime.Serialization;

namespace Raven.Client.Exceptions
{
	/// <summary>
	/// This exception occurs when a (replication) conflict is encountered.
	/// Usually this required a user to manually resolve the conlict.
	/// </summary>
    [Serializable]
    public class ConflictException : Exception
    {
		/// <summary>
		/// Gets or sets the conflicted version ids.
		/// </summary>
		/// <value>The conflicted version ids.</value>
        public string[] ConflictedVersionIds { get; set; }

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

        protected ConflictException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
}