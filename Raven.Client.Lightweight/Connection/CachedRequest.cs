using System;
#if SILVERLIGHT || NETFX_CORE
using Raven.Client.Silverlight.MissingFromSilverlight;
#else
using System.Collections.Specialized;
#endif
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	public class CachedRequest
	{
		public RavenJToken Data { get; set; }
		public DateTimeOffset Time;
		public NameValueCollection Headers;
		public string Database;
		public bool ForceServerCheck;
	}

	public class CachedRequestOp
	{
		public CachedRequest CachedRequest;
		public bool SkipServerCheck;
	}
}