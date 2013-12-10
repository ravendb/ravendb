using System;
using System.Runtime.Serialization;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Client.RavenFS
{
	[Serializable]
	public class SynchronizationException : Exception
	{
		public SynchronizationException(string message)
			: base(message)
		{

		}

		[JsonConstructor]
		public SynchronizationException(string message, Exception inner)
			: base(message, inner)
		{
		}

#if !SILVERLIGHT
		protected SynchronizationException(
			SerializationInfo info,
			StreamingContext context)
			: base(info, context)
		{

		}
#endif
	}
}