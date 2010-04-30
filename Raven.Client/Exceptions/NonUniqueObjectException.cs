using System;
using System.Runtime.Serialization;

namespace Raven.Client.Exceptions
{
	[Serializable]
	public class NonUniqueObjectException : Exception
	{
		public NonUniqueObjectException()
		{
		}

		public NonUniqueObjectException(string message) : base(message)
		{
		}

		public NonUniqueObjectException(string message, Exception inner) : base(message, inner)
		{
		}

		protected NonUniqueObjectException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}