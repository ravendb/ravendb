using System;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper.Unmanaged;

namespace Raven.Database.FileSystem.Synchronization.Rdc.Wrapper
{
	internal class RdcException : Exception
	{
		public RdcException(string message, Exception innerException) :
			base(message, innerException)
		{
		}

		public RdcException(string format, params object[] args) :
			base(String.Format(format, args))
		{
		}

		public RdcException(string message, int hr, RdcError? rdcError = null) :
			base(String.Format("{0} hr: {1} rdcError: {2}", message, hr, rdcError))
		{
		}
	}
}
