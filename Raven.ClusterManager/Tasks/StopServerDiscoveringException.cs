using System;
using System.Runtime.Serialization;

namespace Raven.ClusterManager.Tasks
{
	[Serializable]
	public class StopServerDiscoveringException : Exception
	{
		//
		// For guidelines regarding the creation of new exception types, see
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
		// and
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
		//

		public StopServerDiscoveringException()
		{
		}

		public StopServerDiscoveringException(string message) : base(message)
		{
		}

		public StopServerDiscoveringException(string message, Exception inner) : base(message, inner)
		{
		}

		protected StopServerDiscoveringException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}