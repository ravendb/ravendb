using System;
using System.Runtime.Serialization;

namespace Raven.Client.Exceptions
{
	/// <summary>
	/// This exception is raised when a non authoritive information is encountered
	/// </summary>
	[Serializable]
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
		public NonAuthoritiveInformationException(string message) : base(message)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="NonAuthoritiveInformationException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		/// <param name="inner">The inner.</param>
		public NonAuthoritiveInformationException(string message, Exception inner) : base(message, inner)
		{
		}

		protected NonAuthoritiveInformationException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}