using System;
using System.Runtime.Serialization;

namespace Raven.Database.Exceptions
{
	/// <summary>
	/// This exception is raised when a query is made against a non existing index
	/// </summary>
	[Serializable]
	public class IndexDoesNotExistsException : Exception
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="IndexDoesNotExistsException"/> class.
		/// </summary>
		public IndexDoesNotExistsException()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="IndexDoesNotExistsException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		public IndexDoesNotExistsException(string message) : base(message)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="IndexDoesNotExistsException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		/// <param name="inner">The inner.</param>
		public IndexDoesNotExistsException(string message, Exception inner) : base(message, inner)
		{
		}

		protected IndexDoesNotExistsException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}