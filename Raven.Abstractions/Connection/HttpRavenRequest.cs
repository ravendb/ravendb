using System;
using System.IO;
using System.Net;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Connection
{
	public class HttpRavenRequestFactory
	{
		private readonly int replicationRequestTimeoutInMs;

		public HttpRavenRequestFactory(int replicationRequestTimeoutInMs)
		{
			this.replicationRequestTimeoutInMs = replicationRequestTimeoutInMs;
		}

		private bool RefreshOauthToken(RavenConnectionStringOptions options, WebResponse response)
		{
			var oauthSource = response.Headers["OAuth-Source"];
			if (string.IsNullOrEmpty(oauthSource))
				return false;

			var authRequest = PrepareOAuthRequest(options, oauthSource);
			using (var authResponse = authRequest.GetResponse())
			using (var stream = authResponse.GetResponseStreamWithHttpDecompression())
			using (var reader = new StreamReader(stream))
			{
				options.CurrentOAuthToken = "Bearer " + reader.ReadToEnd();
			}
			return true;
		}
		private HttpWebRequest PrepareOAuthRequest(RavenConnectionStringOptions options,string oauthSource)
		{
			var authRequest = (HttpWebRequest)System.Net.WebRequest.Create(oauthSource);
			authRequest.Credentials = options.Credentials;
			authRequest.Headers["Accept-Encoding"] = "deflate,gzip";
			authRequest.Accept = "application/json;charset=UTF-8";

			authRequest.Headers["grant_type"] = "client_credentials";

			if (string.IsNullOrEmpty(options.ApiKey) == false)
				authRequest.Headers["Api-Key"] = options.ApiKey;

			return authRequest;
		}

		public void ConfigureRequest(RavenConnectionStringOptions options,WebRequest request)
		{
			request.Credentials = options.Credentials ?? CredentialCache.DefaultNetworkCredentials;
			request.Timeout = replicationRequestTimeoutInMs;
			if (string.IsNullOrEmpty(options.CurrentOAuthToken) == false)
				request.Headers["Authorization"] = options.CurrentOAuthToken;
		}

		public HttpRavenRequest Create(string url, string method, RavenConnectionStringOptions connectionStringOptions)
		{
			return new HttpRavenRequest(url, method, ConfigureRequest, RefreshOauthToken, connectionStringOptions);
		}
	}

	public class HttpRavenRequest
	{
		private readonly string url;
		private readonly string method;
		private readonly Action<RavenConnectionStringOptions,WebRequest> configureRequest;
		private readonly Func<RavenConnectionStringOptions,WebResponse, bool> handleUnauthorizedResponse;
		private readonly RavenConnectionStringOptions connectionStringOptions;

		private HttpWebRequest webRequest;

		private Stream postedStream;
		private RavenJToken postedToken;
		private bool writeBson;

	
		public HttpWebRequest WebRequest
		{
			get { return webRequest ?? (webRequest = CreateRequest()); }
			set { webRequest = value; }
		}

		public HttpRavenRequest(string url, string method, Action<RavenConnectionStringOptions, WebRequest> configureRequest, Func<RavenConnectionStringOptions,WebResponse, bool> handleUnauthorizedResponse, RavenConnectionStringOptions connectionStringOptions)
		{
			this.url = url;
			this.method = method;
			this.configureRequest = configureRequest;
			this.handleUnauthorizedResponse = handleUnauthorizedResponse;
			this.connectionStringOptions = connectionStringOptions;
		}

		private HttpWebRequest CreateRequest()
		{
			var request = (HttpWebRequest)System.Net.WebRequest.Create(url);
			request.Method = method;
			request.Headers["Accept-Encoding"] = "deflate,gzip";
			request.ContentType = "application/json; charset=utf-8";
			request.UseDefaultCredentials = true;
			request.PreAuthenticate = true;
			configureRequest(connectionStringOptions, request);
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

					if (handleUnauthorizedResponse != null && handleUnauthorizedResponse(connectionStringOptions, e.Response))
					{
						RecreateWebRequest();
					}
				}
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

	}
}