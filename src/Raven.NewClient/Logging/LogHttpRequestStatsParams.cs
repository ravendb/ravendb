using System;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Net.Http.Headers;

namespace Raven.NewClient.Abstractions.Logging
{
    public class LogHttpRequestStatsParams
    {
        public LogHttpRequestStatsParams(Stopwatch sw, Lazy<HttpHeaders> headers, string httpMethod, int responseStatusCode,
                                         string requestUri, string customInfo = null, int innerRequestsCount = 0)
        {
            Stopwatch = sw;
            Headers = headers;
            HttpMethod = httpMethod;
            ResponseStatusCode = responseStatusCode;
            RequestUri = requestUri;
            CustomInfo = customInfo;
            InnerRequestsCount = innerRequestsCount;
        }

        public Stopwatch Stopwatch { get; private set; }

        public Lazy<HttpHeaders> Headers { get; private set; }

        public string HttpMethod { get; private set; }

        public int ResponseStatusCode { get; private set; }

        public string RequestUri { get; private set; }

        public string CustomInfo { get; private set; }

        public int InnerRequestsCount { get; private set; }
    }
}
