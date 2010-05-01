using System;
using System.Runtime.Serialization;

namespace Raven.Database.Exceptions
{
	[Serializable]
	public class IndexDoesNotExistsException : Exception
	{
		public IndexDoesNotExistsException()
		{
		}

		public IndexDoesNotExistsException(string message) : base(message)
		{
		}

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