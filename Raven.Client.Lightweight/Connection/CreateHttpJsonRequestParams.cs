using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	public class CreateHttpJsonRequestParams
	{
		public CreateHttpJsonRequestParams(IHoldProfilingInformation self, string url, string method, RavenJObject metadata, ICredentials credentials, DocumentConvention convention)
		{
			Owner = self;
			Url = url;
			Method = method;
			Metadata = metadata;
			Credentials = credentials;
			Convention = convention;
			operationsHeadersColletion = new NameValueCollection();
		}

		/// <summary>
		/// Adds the operation headers.
		/// </summary>
		/// <param name="operationsHeaders">The operations headers.</param>
		public CreateHttpJsonRequestParams AddOperationHeaders(IDictionary<string, string> operationsHeaders)
		{
			urlCached = null;
			operationsHeadersDictionary = operationsHeaders;
			foreach (var operationsHeader in operationsHeaders)
			{
				operationHeadersHash = (operationHeadersHash*397) ^ operationsHeader.Key.GetHashCode();
				if (operationsHeader.Value != null)
				{
					operationHeadersHash = (operationHeadersHash * 397) ^ operationsHeader.Value.GetHashCode();
				}
			}
			return this;
		}

		/// <summary>
		/// Adds the operation headers.
		/// </summary>
		/// <param name="operationsHeaders">The operations headers.</param>
		public CreateHttpJsonRequestParams AddOperationHeaders(NameValueCollection operationsHeaders)
		{
			urlCached = null;
			operationsHeadersColletion = operationsHeaders;
			foreach (string operationsHeader in operationsHeadersColletion)
			{
				operationHeadersHash = (operationHeadersHash * 397) ^ operationsHeader.GetHashCode();
				var values = operationsHeaders.GetValues(operationsHeader);
				if (values == null) 
					continue;

				foreach (var header in values.Where(header => header != null))
				{
					operationHeadersHash = (operationHeadersHash * 397) ^ header.GetHashCode();
				}
			}
			return this;
		}

		public void UpdateHeaders(WebRequest webRequest)
		{
			if (operationsHeadersDictionary != null)
			{
				foreach (var kvp in operationsHeadersDictionary)
				{
					webRequest.Headers[kvp.Key] = kvp.Value;
				}
			}
			if(operationsHeadersColletion != null)
			{
				foreach (string header in operationsHeadersColletion)
				{
					try
					{
						webRequest.Headers[header] = operationsHeadersColletion[header];
					}
					catch (Exception e)
					{
						throw new InvalidOperationException(
							"Failed to set header '" + header + "' to the value: " + operationsHeadersColletion[header], e);
					}
				}
			}
		}

		public CreateHttpJsonRequestParams(IHoldProfilingInformation self, string url, string method, ICredentials credentials, DocumentConvention convention)
			: this(self, url, method, new RavenJObject(), credentials, convention)
		{
			
		}

		private int operationHeadersHash;
		private NameValueCollection operationsHeadersColletion;
		private IDictionary<string, string> operationsHeadersDictionary;
		public IHoldProfilingInformation Owner { get; set; }
		private string url;
		private string urlCached;
		public bool AvoidCachingRequest { get; set; }

		public string Url
		{
			get
			{
				if (urlCached != null)
					return urlCached;
				urlCached = GenerateUrl();
				return urlCached;
			}
			set { url = value; }
		}

		private string GenerateUrl()
		{
			if (operationHeadersHash == 0)
				return url;
			if (url.Contains("?"))
				return url + "&operationHeadersHash=" + operationHeadersHash;
			return url + "?operationHeadersHash=" + operationHeadersHash;
		}

		public string Method { get; set; }
		public RavenJObject Metadata { get; set; }
		public ICredentials Credentials { get; set; }
		public DocumentConvention Convention { get; set; }
	}
}
