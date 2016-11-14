using System;
using System.Collections.Specialized;
using System.Linq;
using System.Net.Http;

using Raven.NewClient.Abstractions.Connection;
using Raven.NewClient.Client.Connection.Profiling;
using Raven.NewClient.Client.Metrics;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Connection
{
    public class CreateHttpJsonRequestParams
    {
        private int operationHeadersHash;

        private NameValueCollection operationsHeadersCollection;

        private string url;

        private string urlCached;

        public CreateHttpJsonRequestParams(IHoldProfilingInformation self, string url, HttpMethod method, OperationCredentials credentials, ConventionBase convention, IRequestTimeMetric requestTimeMetric = null, TimeSpan? timeout = null, long? etag = null)
        {
            Etag = etag;
            Owner = self;
            Url = url;
            Method = method;
            Credentials = credentials;
            Convention = convention;
            RequestTimeMetric = requestTimeMetric;
            Timeout = timeout;
            operationsHeadersCollection = new NameValueCollection();
            ShouldCacheRequest = convention != null ? convention.ShouldCacheRequest : urlParam => false;
        }

        public long? Etag { get; }

        public Func<string, bool> ShouldCacheRequest { get; set; }

        public bool AvoidCachingRequest { get; set; }

        public ConventionBase Convention { get; set; }

        public IRequestTimeMetric RequestTimeMetric { get; set; }

        public OperationCredentials Credentials { get; set; }

        public bool DisableAuthentication { get; set; }

        public bool DisableRequestCompression { get; set; }

        public HttpMethod Method { get; set; }

        public IHoldProfilingInformation Owner { get; set; }

        public TimeSpan? Timeout { get; set; }

        public string Url
        {
            get
            {
                if (urlCached != null)
                {
                    return urlCached;
                }
                urlCached = GenerateUrl();
                return urlCached;
            }
            set
            {
                url = value;
            }
        }


        /// <summary>
        ///     Adds the operation headers.
        /// </summary>
        /// <param name="operationsHeaders">The operations headers.</param>
        public CreateHttpJsonRequestParams AddOperationHeaders(NameValueCollection operationsHeaders)
        {
            urlCached = null;
            operationsHeadersCollection = operationsHeaders;
            foreach (string operationsHeader in operationsHeadersCollection)
            {
                operationHeadersHash = (operationHeadersHash * 397) ^ operationsHeader.GetHashCode();
                string[] values = operationsHeaders.GetValues(operationsHeader);
                if (values == null)
                {
                    continue;
                }

                foreach (string header in values.Where(header => header != null))
                {
                    operationHeadersHash = (operationHeadersHash * 397) ^ header.GetHashCode();
                }
            }
            return this;
        }

        public void UpdateHeaders(NameValueCollection headers)
        {
            if (operationsHeadersCollection != null)
            {
                foreach (string header in operationsHeadersCollection)
                {
                    try
                    {
                        headers[header] = operationsHeadersCollection[header];
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Failed to set header '" + header + "' to the value: " + operationsHeadersCollection[header], e);
                    }
                }
            }
        }

        private string GenerateUrl()
        {
            if (operationHeadersHash == 0)
            {
                return url;
            }
            if (url.Contains("?"))
            {
                return url + "&operationHeadersHash=" + operationHeadersHash;
            }
            return url + "?operationHeadersHash=" + operationHeadersHash;
        }
    }
}
