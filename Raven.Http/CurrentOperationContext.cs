using System.Collections.Specialized;
using System.Threading;

namespace Raven.Http
{
	public static class CurrentOperationContext
	{
		public static readonly ThreadLocal<NameValueCollection> Headers = new ThreadLocal<NameValueCollection>(() => new NameValueCollection());
	}
}
