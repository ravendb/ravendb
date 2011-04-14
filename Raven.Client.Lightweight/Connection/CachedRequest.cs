using System.Collections.Specialized;

namespace Raven.Client.Connection
{
	internal class CachedRequest
	{
		public string Data;
		public NameValueCollection Headers;
	}
}