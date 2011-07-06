using System;
using System.Collections.Specialized;

namespace Raven.Client.Connection
{
	internal class CachedRequest
	{
		public string Data;
		public DateTimeOffset Time;
		public NameValueCollection Headers;
	}

	internal class CachedRequestOp
	{
		public CachedRequest CachedRequest;
		public bool SkipServerCheck;
	}
}