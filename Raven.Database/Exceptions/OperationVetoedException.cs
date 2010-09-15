using System;
using System.Runtime.Serialization;

namespace Raven.Database.Exceptions
{
	/// <summary>
	/// This exception is raised when an operation has been vetoed by a trigger
	/// </summary>
	[Serializable]
	public class OperationVetoedException : Exception
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="OperationVetoedException"/> class.
		/// </summary>
		public OperationVetoedException()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="OperationVetoedException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		public OperationVetoedException(string message) : base(message)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="OperationVetoedException"/> class.
		/// </summary>
		/// <param name="message">The message.</param>
		/// <param name="inner">The inner.</param>
		public OperationVetoedException(string message, Exception inner) : base(message, inner)
		{
		}

		protected OperationVetoedException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}