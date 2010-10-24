using System;
using System.Collections.Specialized;
using System.IO;
using System.Web;

namespace Raven.Http.Abstractions
{
	public class HttpRequestAdapter : IHttpRequest
	{
		private readonly HttpRequest request;

	    public HttpRequestAdapter(HttpRequest request)
		{
		    this.request = request;
		    Url = this.request.Url;
	        RawUrl = this.request.RawUrl;
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
			get { return request.QueryString; }
		}

	    public Uri Url { get;  set; }

	    public string HttpMethod
		{
			get { return request.HttpMethod; }
		}

	    public string RawUrl { get;  set; }
	}
}
