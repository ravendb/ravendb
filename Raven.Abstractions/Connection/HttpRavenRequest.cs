using System;
using System.IO;
using System.Net;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Connection
{
	public class HttpRavenRequest
	{
		private readonly string url;
		private readonly string method;
		private readonly int timeout;

		private HttpWebRequest webRequest;

		private Stream postedStream;
		private RavenJToken postedToken;
		private string currentOauthToken;
		private bool writeBson;

		/// <summary>
		/// The API Key to use when authenticating against a RavenDB server that
		/// supports API Key authentication
		/// </summary>
		public string ApiKey { get; set; }

		/// <summary>
		/// The credentials to use when authenticating against a RavenDB server that
		/// supports credentials authentication
		/// </summary>
		public ICredentials Credentials { get; set; }

		public HttpWebRequest WebRequest
		{
			get { return webRequest ?? (webRequest = CreateRequest()); }
			set { webRequest = value; }
		}

		public HttpRavenRequest(string url, string method = "GET", int timeout = 15000)
		{
			this.url = url;
			this.method = method;
			this.timeout = timeout;
		}

		private HttpWebRequest CreateRequest()
		{
			var request = (HttpWebRequest)System.Net.WebRequest.Create(url);
			request.Method = method;
			request.Timeout = timeout;
			request.Headers["Accept-Encoding"] = "deflate,gzip";
			request.ContentType = "application/json; charset=utf-8";
			request.UseDefaultCredentials = true;
			request.PreAuthenticate = true;
			request.Credentials = Credentials;

			if (string.IsNullOrEmpty(currentOauthToken) == false)
				request.Headers["Authorization"] = currentOauthToken;

			return request;
		}

		public void Write(Stream streamToWrite)
		{
			postedStream = streamToWrite;
			WebRequest.ContentLength = streamToWrite.Length;
			using (var stream = WebRequest.GetRequestStream())
			{
				streamToWrite.CopyTo(stream);
				stream.Flush();
			}
		}

		public void Write(RavenJToken ravenJToken)
		{
			postedToken = ravenJToken;
			WriteToken(WebRequest);
		}


		public void WriteBson(RavenJToken ravenJToken)
		{
			writeBson = true;
			postedToken = ravenJToken;
			WriteToken(WebRequest);
		}

		private void WriteToken(WebRequest httpWebRequest)
		{
			using (var stream = httpWebRequest.GetRequestStream())
			{
				if (writeBson)
				{
					postedToken.WriteTo(new BsonWriter(stream));
				}
				else
				{
					using (var streamWriter = new StreamWriter(stream))
					{
						postedToken.WriteTo(new JsonTextWriter(streamWriter));
						streamWriter.Flush();
					}
				}
				stream.Flush();
			}
		}

		public T ExecuteRequest<T>()
		{
			T result = default(T);
			SendRequestToServer(response =>
									{
										using (var stream = response.GetResponseStreamWithHttpDecompression())
										using (var reader = new StreamReader(stream))
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
					using (var res = WebRequest.GetResponse())
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

					HandleUnauthorizedResponse(e.Response);
				}
			}
		}

		private void HandleUnauthorizedResponse(WebResponse response)
		{
			RefreshOauthToken(response);
			RecreateWebRequest();
		}

		private void RefreshOauthToken(WebResponse response)
		{
			var oauthSource = response.Headers["OAuth-Source"];
			if (string.IsNullOrEmpty(oauthSource))
				return;

			var authRequest = PrepareOAuthRequest(oauthSource);
			using (var authResponse = authRequest.GetResponse())
			using (var stream = authResponse.GetResponseStreamWithHttpDecompression())
			using (var reader = new StreamReader(stream))
			{
				currentOauthToken = "Bearer " + reader.ReadToEnd();
			}
		}

		private void RecreateWebRequest()
		{
			// we now need to clone the request, since just calling GetRequest again wouldn't do anything
			var newWebRequest = CreateRequest();
			HttpRequestHelper.CopyHeaders(WebRequest, newWebRequest);

			if (postedToken != null)
			{
				WriteToken(newWebRequest);
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
			WebRequest = newWebRequest;
		}

		private HttpWebRequest PrepareOAuthRequest(string oauthSource)
		{
			var authRequest = (HttpWebRequest)System.Net.WebRequest.Create(oauthSource);
			authRequest.Credentials = Credentials;
			authRequest.Headers["Accept-Encoding"] = "deflate,gzip";
			authRequest.Accept = "application/json;charset=UTF-8";

			authRequest.Headers["grant_type"] = "client_credentials";

			if (string.IsNullOrEmpty(ApiKey) == false)
				WebRequest.Headers["Api-Key"] = ApiKey;

			return authRequest;
		}

	}
}