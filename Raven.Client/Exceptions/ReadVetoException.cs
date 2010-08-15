using System;
using System.Runtime.Serialization;

namespace Raven.Client.Exceptions
{
	[Serializable]
	public class ReadVetoException : Exception
	{
		public ReadVetoException()
		{
		}

		public ReadVetoException(string message) : base(message)
		{
		}

		public ReadVetoException(string message, Exception inner) : base(message, inner)
		{
		}

		protected ReadVetoException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}