using System;
using System.Collections.Specialized;
using System.IO;
using System.Net;

namespace Raven.Database.Server.Abstractions
{
	public class HttpListenerResponseAdapter : IHttpResponse
	{
		private readonly HttpListenerResponse response;

		public HttpListenerResponseAdapter(HttpListenerResponse response)
		{
			this.response = response;
		}

		public NameValueCollection Headers
		{
			get { return response.Headers; }
		}

		public Stream OutputStream
		{
			get { return response.OutputStream; }
		}

		public long ContentLength64
		{
			get { return response.ContentLength64; }
			set { response.ContentLength64 = value; }
		}

		public int StatusCode
		{
			get { return response.StatusCode; }
			set { response.StatusCode = value; }
		}

		public string StatusDescription
		{
			get { return response.StatusDescription; }
			set { response.StatusDescription = value; }
		}

		public string ContentType
		{
			get { return response.ContentType; }
			set { response.ContentType = value; }
		}

		public void Redirect(string url)
		{
			response.Redirect(url);
		}

		public void Close()
		{
			response.Close();
		}
	}
}