using System;

namespace Raven.Database.Extensions
{
	public class FeatureNotSupportedOnPosixException : Exception
	{
		public FeatureNotSupportedOnPosixException ()
		{
		}
		public FeatureNotSupportedOnPosixException (string message) : base(message)
		{
		}
		public FeatureNotSupportedOnPosixException (string message, Exception inner) : base(message, inner)
		{
		}
	}
}

