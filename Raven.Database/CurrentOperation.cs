using System.Collections.Specialized;
using System.Threading;

namespace Raven.Database
{
	public static class CurrentOperation
	{
		public static readonly ThreadLocal<NameValueCollection> Headers = new ThreadLocal<NameValueCollection>(() => new NameValueCollection());
	}
}
