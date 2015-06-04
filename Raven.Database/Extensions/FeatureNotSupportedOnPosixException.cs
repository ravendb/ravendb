using System;
using System.Runtime.Serialization;

namespace Raven.Database.Extensions
{
	[Serializable]
	public class FeatureNotSupportedOnPosixException : Exception
	{
		//
		// For guidelines regarding the creation of new exception types, see
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
		// and
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
		//

		public FeatureNotSupportedOnPosixException()
		{
		}

		public FeatureNotSupportedOnPosixException(string message) : base(message)
		{
		}

		public FeatureNotSupportedOnPosixException(string message, Exception inner) : base(message, inner)
		{
		}

		protected FeatureNotSupportedOnPosixException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}

