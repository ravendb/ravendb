using System;
using System.Runtime.Serialization;

namespace Raven.Database.Exceptions
{
	/// <summary>
	/// This exception is raised when a concurrency conflict is encountered
	/// </summary>
	[Serializable]
	public class ConcurrencyException : Exception
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="ConcurrencyException"/> class.
		/// </summary>
		public ConcurrencyException()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ConcurrencyException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		public ConcurrencyException(string message) : base(message)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ConcurrencyException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		/// <param name="inner">The inner.</param>
		public ConcurrencyException(string message, Exception inner) : base(message, inner)
		{
		}

		protected ConcurrencyException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}

		public Guid ExpectedETag { get; set; }
		public Guid ActualETag { get; set; }
	}
}