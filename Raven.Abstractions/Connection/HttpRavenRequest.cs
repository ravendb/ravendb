using System;

using System.IO;
using System.Net;
using System.Text;
using Newtonsoft.Json.Bson;
using Raven.Json.Linq;

namespace Raven.Abstractions.Connection
{
	public class HttpRavenRequest
	{
		private readonly string url;
		private readonly string method;
		private readonly ICredentials credentials;

		public volatile HttpWebRequest webRequest;
		private Stream postedStream;

		public Action<WebRequest> ConfigureRequest = delegate { };

		public HttpRavenRequest(string url, string method = "GET", ICredentials credentials = null)
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

		public void Write(Stream streamToWrite)
		{
			postedStream = streamToWrite;
			webRequest.ContentLength = streamToWrite.Length;
			using (var stream = webRequest.GetRequestStream())
			{
				streamToWrite.CopyTo(stream);
				stream.Flush();
			}
			ExecuteRequest();
		}

		public void Write(RavenJToken ravenJToken)
		{
			var streamToWrite = new MemoryStream();
			ravenJToken.WriteTo(new BsonWriter(streamToWrite));
			Write(streamToWrite);
		}

		public void Write(Action<StreamWriter> action)
		{
			using (var stream = webRequest.GetRequestStream())
			{
				using (var streamWriter = new StreamWriter(stream, Encoding.UTF8))
				{
					action(streamWriter);
					streamWriter.Flush();
				}
				stream.Flush();
			}
			ExecuteRequest();
		}

		public virtual void ExecuteRequest()
		{
			using (webRequest.GetResponse())
			{
			}
		}
		
		public Stream GetResponseStream()
		{
			using (var response = webRequest.GetResponse())
			{
				return response.GetResponseStreamWithHttpDecompression();
			}
		}
	}
}