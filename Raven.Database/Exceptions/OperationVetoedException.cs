using System;
using System.Runtime.Serialization;

namespace Raven.Database.Exceptions
{
	[Serializable]
	public class OperationVetoedException : Exception
	{
		public OperationVetoedException()
		{
		}

		public OperationVetoedException(string message) : base(message)
		{
		}

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