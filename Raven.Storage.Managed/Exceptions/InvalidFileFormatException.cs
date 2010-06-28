using System;
using System.Runtime.Serialization;

namespace Raven.Storage.Managed.Exceptions
{
	[Serializable]
	public class InvalidFileFormatException : Exception
	{
		public InvalidFileFormatException()
		{
		}

		public InvalidFileFormatException(string message) : base(message)
		{
		}

		public InvalidFileFormatException(string message, Exception inner) : base(message, inner)
		{
		}

		protected InvalidFileFormatException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}