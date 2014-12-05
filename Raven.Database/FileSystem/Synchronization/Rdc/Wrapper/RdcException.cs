using System;
using System.Runtime.Serialization;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper.Unmanaged;

namespace Raven.Database.FileSystem.Synchronization.Rdc.Wrapper
{
	[Serializable]
	public class RdcException : Exception
	{
		
		public RdcException()
		{
		}

		public RdcException(string message) : base(message)
		{
		}

		public RdcException(string message, Exception inner) : base(message, inner)
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
		protected RdcException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}
