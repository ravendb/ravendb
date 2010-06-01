using System;
using System.Runtime.Serialization;

namespace Raven.Client.Exceptions
{
	[Serializable]
	public class NonAuthoritiveInformationException : Exception
	{
		public NonAuthoritiveInformationException()
		{
		}

		public NonAuthoritiveInformationException(string message) : base(message)
		{
		}

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