using System;
using System.Collections.Specialized;
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