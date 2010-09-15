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

		/// <summary>
		/// Initializes a new instance of the <see cref="ConcurrencyException"/> class.
		/// </summary>
		/// <param name="info">The <see cref="T:System.Runtime.Serialization.SerializationInfo"/> that holds the serialized object data about the exception being thrown.</param>
		/// <param name="context">The <see cref="T:System.Runtime.Serialization.StreamingContext"/> that contains contextual information about the source or destination.</param>
		/// <exception cref="T:System.ArgumentNullException">The <paramref name="info"/> parameter is null. </exception>
		/// <exception cref="T:System.Runtime.Serialization.SerializationException">The class name is null or <see cref="P:System.Exception.HResult"/> is zero (0). </exception>
		protected ConcurrencyException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}

		public Guid ExpectedETag { get; set; }
		public Guid ActualETag { get; set; }
	}
}