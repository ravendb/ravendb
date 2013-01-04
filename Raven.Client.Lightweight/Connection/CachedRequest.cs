using System;
#if !SILVERLIGHT
using System.Collections.Specialized;
#else
using Raven.Client.Silverlight.MissingFromSilverlight;
#endif
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	internal class CachedRequest
	{
		public RavenJToken Data { get; set; }
		public DateTimeOffset Time;
		public NameValueCollection Headers;
	}

	internal class CachedRequestOp
	{
		public CachedRequest CachedRequest;
		public bool SkipServerCheck;
	}
}