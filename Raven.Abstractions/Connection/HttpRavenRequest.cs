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
		private readonly ICredentials credentials;
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

		public HttpRavenRequest(string url, string method = "GET", ICredentials credentials = null, int timeout = 15000)
		{
			this.url = url;
			this.method = method;
			this.credentials = credentials;
			this.timeout = timeout;

			webRequest = CreateRequest();
		}

		private HttpWebRequest CreateRequest()
		{
			var request = (HttpWebRequest)WebRequest.Create(url);
			request.Method = method;
			request.Timeout = timeout;
			request.Headers["Accept-Encoding"] = "deflate,gzip";
			request.ContentType = "application/json; charset=utf-8";
			request.UseDefaultCredentials = true;
			request.Credentials = credentials;
			request.PreAuthenticate = true;

			if (string.IsNullOrEmpty(currentOauthToken) == false)
				request.Headers["Authorization"] = currentOauthToken;

			return request;
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
			WriteToken(webRequest);
		}


		public void WriteBson(RavenJToken ravenJToken)
		{
			writeBson = true;
			postedToken = ravenJToken;
			WriteToken(webRequest);
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
			HttpRequestHelper.CopyHeaders(webRequest, newWebRequest);

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
			webRequest = newWebRequest;
		}

		private HttpWebRequest PrepareOAuthRequest(string oauthSource)
		{
			var authRequest = (HttpWebRequest)WebRequest.Create(oauthSource);
			authRequest.Credentials = credentials;
			authRequest.Headers["Accept-Encoding"] = "deflate,gzip";
			authRequest.Accept = "application/json;charset=UTF-8";

			authRequest.Headers["grant_type"] = "client_credentials";

			if (string.IsNullOrEmpty(ApiKey) == false)
				webRequest.Headers["Api-Key"] = ApiKey;

			return authRequest;
		}

	}
}