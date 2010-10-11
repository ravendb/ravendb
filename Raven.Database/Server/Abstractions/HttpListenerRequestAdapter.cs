using System;
using System.Collections.Specialized;
using System.IO;
using System.Net;

namespace Raven.Database.Server.Abstractions
{
	public class HttpListenerRequestAdapter : IHttpRequest
	{
		private readonly HttpListenerRequest request;

	    private NameValueCollection queryString;

	    public HttpListenerRequestAdapter(HttpListenerRequest request)
		{
			this.request = request;
		    this.queryString = System.Web.HttpUtility.ParseQueryString(Uri.UnescapeDataString(request.Url.Query));
		}

		public NameValueCollection Headers
		{
			get { return request.Headers; }
		}

		public Stream InputStream
		{
			get { return request.InputStream; }
		}

		public NameValueCollection QueryString
		{
			get { return queryString; }
		}

		public Uri Url
		{
			get { return request.Url; }
		}

		public string HttpMethod
		{
			get { return request.HttpMethod; }
		}

		public string RawUrl
		{
			get { return request.RawUrl; }
		}
	}
}
