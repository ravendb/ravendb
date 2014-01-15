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
	using Raven.Abstractions.Connection;

	public class CreateHttpJsonRequestParams
	{
		public CreateHttpJsonRequestParams(IHoldProfilingInformation self, string url, string method, RavenJObject metadata, OperationCredentials credentials, Convention convention)
		{
			Owner = self;
			Url = url;
			Method = method;
			Metadata = metadata;
			Credentials = credentials;
			Convention = convention;
			operationsHeadersCollection = new NameValueCollection();
		}

		/// <summary>
		/// Adds the operation headers.
		/// </summary>
		/// <param name="operationsHeaders">The operations headers.</param>
		public CreateHttpJsonRequestParams AddOperationHeaders(NameValueCollection operationsHeaders)
		{
			urlCached = null;
			operationsHeadersCollection = operationsHeaders;
			foreach (string operationsHeader in operationsHeadersCollection)
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
						throw new InvalidOperationException(
							"Failed to set header '" + header + "' to the value: " + operationsHeadersCollection[header], e);
					}
				}
			}
		}

		public CreateHttpJsonRequestParams(IHoldProfilingInformation self, string url, string method, OperationCredentials credentials, Convention convention)
			: this(self, url, method, new RavenJObject(), credentials, convention)
		{}

		private int operationHeadersHash;
		private NameValueCollection operationsHeadersCollection;
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
		public OperationCredentials Credentials { get; set; }

		public Convention Convention { get; set; }
		public bool DisableRequestCompression { get; set; }
	}
}
