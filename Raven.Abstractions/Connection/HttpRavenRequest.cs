using System;
using System.IO;
using System.Net;
using Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Connection
{
	public class HttpRavenRequest
	{
		private readonly string url;
		private readonly string method;
		private readonly ICredentials credentials;

		private HttpWebRequest webRequest;
		private Stream postedStream;
		private RavenJToken postedToken;

		public HttpRavenRequest(string url, string method = "GET", ICredentials credentials = null, int timeout = 15000)
		{
			this.url = url;
			this.method = method;
			this.credentials = credentials;

			webRequest = (HttpWebRequest) WebRequest.Create(url);
			webRequest.Method = method;
			webRequest.Timeout = timeout;
			webRequest.Headers["Accept-Encoding"] = "deflate,gzip";
			webRequest.ContentType = "application/json; charset=utf-8";
			webRequest.UseDefaultCredentials = true;
			webRequest.Credentials = credentials;
			webRequest.PreAuthenticate = true;
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
		}

		public void Write(RavenJToken ravenJToken)
		{
			postedToken = ravenJToken;
			WriteToken(ravenJToken, webRequest);
		}

		private void WriteToken(RavenJToken ravenJToken, HttpWebRequest httpWebRequest)
		{
			using (var stream = httpWebRequest.GetRequestStream())
			using (var streamWriter = new StreamWriter(stream))
			{
				ravenJToken.WriteTo(new JsonTextWriter(streamWriter));
				streamWriter.Flush();
				stream.Flush();
			}
		}

		public T ExecuteRequest<T>()
		{
			T result = default(T);
			SendRequestToServer(response =>
			                    	{
										using (var stream = response.GetResponseStreamWithHttpDecompression())
										using(var reader = new StreamReader(stream))
										{
											result = reader.JsonDeserialization<T>();
										}
			                    	});
			return result;
		}

		public void ExecuteRequest()
		{
			SendRequestToServer(response => { });
		}

		private void SendRequestToServer(Action<WebResponse> action)
		{
			int retries = 0;
			while (true)
			{
				try
				{
					using (var res = webRequest.GetResponse())
					{
						action(res);
					}
					return;
				}
				catch (WebException e)
				{
					if (++retries >= 3)
						throw;

					var httpWebResponse = e.Response as HttpWebResponse;
					if (httpWebResponse == null ||
						httpWebResponse.StatusCode != HttpStatusCode.Unauthorized)
						throw;

					HandleUnauthorizedResponse();
				}
			}
		}

		private void HandleUnauthorizedResponse()
		{
			// we now need to clone the request, since just calling GetRequest again wouldn't do anything

			var newWebRequest = (HttpWebRequest)WebRequest.Create(Url);
			newWebRequest.Method = webRequest.Method;
			HttpRequestHelper.CopyHeaders(webRequest, newWebRequest);
			newWebRequest.Credentials = webRequest.Credentials;
			ConfigureAuthentication(newWebRequest);

			if (postedToken != null)
			{
				WriteToken(postedToken, newWebRequest);
			}
			if (postedStream != null)
			{
				postedStream.Position = 0;
				using (var stream = newWebRequest.GetRequestStream())
				{
					postedStream.CopyTo(stream);
					stream.Flush();
				}
			}
			webRequest = newWebRequest;
		}

		private void ConfigureAuthentication(HttpWebRequest newWebRequest)
		{
		}
	}
}