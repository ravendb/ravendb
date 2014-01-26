using System.Collections.Specialized;
using System.Diagnostics;

namespace Raven.Database.Server
{
	public class LogHttpRequestStatsParams
	{
		public LogHttpRequestStatsParams(Stopwatch sw, NameValueCollection headers, string httpMethod, int responseStatusCode,
		                                 string requestUri, string customInfo = null)
		{
			Stopwatch = sw;
			Headers = headers;
			HttpMethod = httpMethod;
			ResponseStatusCode = responseStatusCode;
			RequestUri = requestUri;
			CustomInfo = customInfo;
		}

		public Stopwatch Stopwatch { get; private set; }

		public NameValueCollection Headers { get; private set; }

		public string HttpMethod { get; private set; }

		public int ResponseStatusCode { get; private set; }

		public string RequestUri { get; private set; }

		public string CustomInfo { get; private set; }
	}
}