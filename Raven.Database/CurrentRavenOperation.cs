using System.Collections.Specialized;
using System.Threading;

namespace Raven.Database
{
	public static class CurrentRavenOperation
	{
		public static readonly ThreadLocal<NameValueCollection> Headers = new ThreadLocal<NameValueCollection>(() => new NameValueCollection());
	}
}
