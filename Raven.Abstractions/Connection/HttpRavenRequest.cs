using System;
using System.IO;
using System.Net;

namespace Raven.Abstractions.Connection
{
	public class HttpRavenRequest
	{
		private readonly string url;
		private readonly string method;
		private readonly ICredentials credentials;

		public volatile HttpWebRequest webRequest;

		public HttpRavenRequest(string url, string method, ICredentials credentials)
		{
			this.url = url;
			this.method = method;
			this.credentials = credentials;

			webRequest = (HttpWebRequest) WebRequest.Create(url);
			webRequest.Method = method;
			webRequest.Headers["Accept-Encoding"] = "deflate,gzip";
			webRequest.ContentType = "application/json; charset=utf-8";
			if (credentials == null)
			{
				webRequest.UseDefaultCredentials = true;
			}
			else
			{
				webRequest.Credentials = credentials;
			}
			webRequest.PreAuthenticate = true;

			ConfigureRequest(webRequest);
		}

		public ICredentials Credentials
		{
			get { return credentials; }
		}

		public string Method
		{
			get { return method; }
		}

		public string Url
		{
			get { return url; }
		}

		public void Write(Action<Stream> action)
		{
			using (var stream = webRequest.GetRequestStream())
			{
				action(stream);
				stream.Flush();
			}
			webRequest.GetResponse();
		}

		public virtual void ExecuteRequest()
		{
			using (webRequest.GetResponse())
			{
				
			}
		}
	
		public virtual void ConfigureRequest(HttpWebRequest request)
		{
			webRequest.ContentType = "application/json; charset=utf-8";
		}
	}
}