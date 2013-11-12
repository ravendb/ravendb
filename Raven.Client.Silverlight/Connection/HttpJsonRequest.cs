//-----------------------------------------------------------------------
// <copyright file="HttpJsonRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Net.Browser;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Ionic.Zlib;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Client.Extensions;
using Raven.Abstractions.Connection;

namespace Raven.Client.Silverlight.Connection
{
	/// <summary>
	/// A representation of an HTTP json request to the RavenDB server
	/// Since we are using the ClientHttp stack for Silverlight, we don't need to implement
	/// caching, it is already implemented for us.
	/// Note: that the RavenDB server generates both an ETag and an Expires header to ensure proper
	/// Note: behavior from the silverlight http stack
	/// </summary>
	public class HttpJsonRequest
	{
		internal readonly string Url;
		internal readonly string Method;
		private readonly DocumentConvention conventions;

		private bool writeCalled;
		private HttpClientHandler handler;
		internal HttpClient httpClient;

		private Stream postedData;
		private int retries;
		public static readonly string ClientVersion = new AssemblyName(typeof(HttpJsonRequest).Assembly.FullName).Version.ToString();
		private bool disabledAuthRetries;
		private bool isRequestSentToServer;

		private string primaryUrl;

		private string operationUrl;

		public Action<string, string, string> HandleReplicationStatusChanges = delegate { };

		private async Task RecreateHttpClient(Action<HttpClient> result)
		{
			retries++;
			var newHttpClient = new HttpClient(new HttpClientHandler
			{
				Credentials = handler.Credentials,
			});
			// HttpJsonRequestHelper.CopyHeaders(webRequest, newWebRequest);
			result(newHttpClient);
			httpClient = newHttpClient;
			isRequestSentToServer = false;

			if (postedData == null)
				return;

			await WriteAsync(postedData);
		}

		public void DisableAuthentication()
		{
			handler.Credentials = null;
			handler.UseDefaultCredentials = false;
			disabledAuthRetries = true;
		}

		public void RemoveAuthorizationHeader()
		{
#if !SILVERLIGHT
			var headersWithoutAuthorization = new WebHeaderCollection();

			foreach (var header in webRequest.Headers.AllKeys)
			{
				if(header == "Authorization")
					continue;

				headersWithoutAuthorization[header] = webRequest.Headers[header];
			}

			webRequest.Headers = headersWithoutAuthorization;
#endif
		}

		private HttpJsonRequestFactory factory;

		private static Task noopWaitForTask = new CompletedTask();


		public TimeSpan Timeout
		{
			set { } // can't set timeout in Silverlight
		}
		/// <summary>
		/// Gets or sets the response headers.
		/// </summary>
		/// <value>The response headers.</value>
		public NameValueCollection ResponseHeaders { get; set; }

		internal HttpJsonRequest(CreateHttpJsonRequestParams requestParams, HttpJsonRequestFactory factory)
		{
			Url = requestParams.Url;
			Method = requestParams.Method;
			conventions = requestParams.Convention;
			this.factory = factory;

			noopWaitForTask = new CompletedTask();
			WaitForTask = noopWaitForTask;

			handler = new HttpClientHandler
			{

			};
			httpClient = new HttpClient(handler);
			httpClient.DefaultRequestHeaders.Add("Raven-Client-Version", ClientVersion);

			WriteMetadata(requestParams.Metadata);
			if (requestParams.Method != "GET")
				httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json") { CharSet = "utf-8" });

			if (factory.DisableRequestCompression == false && requestParams.DisableRequestCompression == false)
			{
				if (requestParams.Method == "POST" || requestParams.Method == "PUT" ||
					requestParams.Method == "PATCH" || requestParams.Method == "EVAL")
					httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Content-Encoding", "gzip");
			}
		}

		public async Task<RavenJToken> ReadResponseJsonAsync()
		{
			var result = await ReadResponseStringAsync();
			return RavenJToken.Parse(result);
		}

		public Task ExecuteRequestAsync()
		{
			return ReadResponseStringAsync();
		}

		/// <summary>
		/// Begins the read response string.
		/// </summary>
		private async Task<string> ReadResponseStringAsync()
		{
			if (isRequestSentToServer)
				throw new InvalidOperationException("Request was already sent to the server, cannot retry request.");
			isRequestSentToServer = true;

			await WaitForTask;

			if (writeCalled == false)
			{
				Task<HttpResponseMessage> sendTask;
				sendTask = httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(Method), Url))
									 .ConvertSecurityExceptionToServerNotFound()
					// .MaterializeBadRequestAsException()
									 .AddUrlIfFaulting(new Uri(Url));
				Response = await sendTask;
				SetResponseHeaders(Response);

			}


			if (Response.IsSuccessStatusCode == false)
				throw new ErrorResponseException(Response);

			return await ReadStringInternal();
			;
		}

		private void SetResponseHeaders(HttpResponseMessage response)
		{
			ResponseHeaders = new NameValueCollection();
			foreach (var header in response.Headers)
			{
				foreach (var val in header.Value)
				{
					ResponseHeaders[header.Key] = val;
				}
			}
		}

		public async Task<bool> HandleUnauthorizedResponseAsync(HttpResponseMessage unauthorizedResponse)
		{
			if (conventions.HandleUnauthorizedResponseAsync == null)
				return false;

			var unauthorizedResponseAsync = conventions.HandleUnauthorizedResponseAsync(unauthorizedResponse);
			if (unauthorizedResponseAsync == null)
				return false;

			await RecreateHttpClient(await unauthorizedResponseAsync);
			return true;
		}

		private async Task HandleForbiddenResponseAsync(HttpResponseMessage forbiddenResponse)
		{
			if (conventions.HandleForbiddenResponseAsync == null)
				return;

			var forbiddenResponseAsync = conventions.HandleForbiddenResponseAsync(forbiddenResponse);
			if (forbiddenResponseAsync == null)
				return;

			await forbiddenResponseAsync;
		}

		public async Task<byte[]> ReadResponseBytesAsync()
		{
			await WaitForTask;

			Response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(Method), Url))
			                           .ConvertSecurityExceptionToServerNotFound()
			                           .AddUrlIfFaulting(new Uri(Url));
			SetResponseHeaders(Response);
			if (Response.IsSuccessStatusCode == false)
				throw new ErrorResponseException(Response);

			// TODO: Use RetryIfNeedTo(task, ReadResponseBytesAsync)
			return ConvertStreamToBytes(await Response.GetResponseStreamWithHttpDecompression());
		}

		private static byte[] ConvertStreamToBytes(Stream input)
		{
			var buffer = new byte[16 * 1024];
			using (var ms = new MemoryStream())
			{
				int read;
				while ((read = input.Read(buffer, 0, buffer.Length)) > 0)
				{
					ms.Write(buffer, 0, read);
				}
				return ms.ToArray();
			}
		}

		private async Task<string> ReadStringInternal()
		{
			var responseStream = await Response.GetResponseStreamWithHttpDecompression();
			var reader = new StreamReader(responseStream);
			var text = reader.ReadToEnd();
			return text;
		}


		/// <summary>
		/// Gets or sets the response status code.
		/// </summary>
		/// <value>The response status code.</value>
		public HttpStatusCode ResponseStatusCode { get; set; }

		/// <summary>
		/// The task to wait all other actions on
		/// </summary>
		public Task WaitForTask { get; set; }

		public HttpResponseMessage Response { get; set; }

		private void WriteMetadata(RavenJObject metadata)
		{
			if (metadata == null)
				return;

			foreach (var prop in metadata)
			{
				if (prop.Value == null)
					continue;

				string value;
				switch (prop.Value.Type)
				{
					case JTokenType.Array:
						value = prop.Value.Value<RavenJArray>().ToString(Formatting.None);
						break;
					case JTokenType.Object:
						value = prop.Value.Value<RavenJObject>().ToString(Formatting.None);
						break;
					default:
						value = prop.Value.Value<object>().ToString();
						break;
				}
				var headerName = prop.Key;
				if (headerName == "ETag")
					headerName = "If-None-Match";
				if (headerName.StartsWith("@") ||
					headerName == Constants.LastModified ||
					headerName == Constants.RavenLastModified)
					continue;

				try
				{
					switch (headerName)
					{
						case "If-None-Match":
							httpClient.DefaultRequestHeaders.IfNoneMatch.Add(new EntityTagHeaderValue("\"" + value + "\""));
							break;
						case "Content-Length":
							break;
						case "Content-Type":
							httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue(value));
							break;
						default:
							httpClient.DefaultRequestHeaders.TryAddWithoutValidation(headerName, value);
							break;
					}
				}
				catch (Exception e)
				{
					throw new InvalidOperationException("Make sure to set the header correctly.", e);
				}
			}
		}

		/// <summary>
		/// Begins the write operation
		/// </summary>
		public async Task WriteAsync(string data)
		{
			await WaitForTask;

			writeCalled = true;
			Response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(Method), Url)
			{
													Content = new CompressedStringContent(data, factory.DisableRequestCompression),
												});

			if (Response.IsSuccessStatusCode == false)
				throw new ErrorResponseException(Response);
		}


		/// <summary>
		/// Begins the write operation
		/// </summary>
		private async Task WriteAsync(Stream stream)
		{
			writeCalled = true;
			postedData = stream;

			Response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(Method), Url)
			{
				Content = new CompressedStreamContent(stream)
			});

			if (Response.IsSuccessStatusCode == false)
				throw new ErrorResponseException(Response);
		}

		/// <summary>
		/// Adds the operation headers.
		/// </summary>
		/// <param name="operationsHeaders">The operations headers.</param>
		public HttpJsonRequest AddOperationHeaders(IDictionary<string, string> operationsHeaders)
		{
			foreach (var header in operationsHeaders)
			{
				httpClient.DefaultRequestHeaders.Add(header.Key, header.Value);
			}
			return this;
		}

		/// <summary>
		/// Adds the operation header
		/// </summary>
		public HttpJsonRequest AddOperationHeader(string key, string value)
		{
			httpClient.DefaultRequestHeaders.Add(key, value);
			return this;
		}

		public async Task<IObservable<string>> ServerPullAsync()
		{
			await WaitForTask;

			int retries = 0;
			while (true)
			{
				ErrorResponseException webException;

				try
				{
					Response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(Method), Url), HttpCompletionOption.ResponseHeadersRead);

					var stream = await Response.GetResponseStreamWithHttpDecompression();
					var observableLineStream = new ObservableLineStream(stream, () => Response.Dispose());
					SetResponseHeaders(Response);
					observableLineStream.Start();
					return (IObservable<string>)observableLineStream;
				}
				catch (ErrorResponseException e)
				{
					if (++retries >= 3 || disabledAuthRetries)
						throw;

					if (e.StatusCode != HttpStatusCode.Unauthorized &&
					    e.StatusCode != HttpStatusCode.Forbidden &&
					    e.StatusCode != HttpStatusCode.PreconditionFailed)
						throw;

					webException = e;
				}

				if (webException.StatusCode == HttpStatusCode.Forbidden)
				{
					await HandleForbiddenResponseAsync(webException.Response);
					await new CompletedTask(webException).Task; // Throws, preserving original stack
				}

				if (await HandleUnauthorizedResponseAsync(webException.Response) == false)
					await new CompletedTask(webException).Task; // Throws, preserving original stack
			}
		}

		public async Task ExecuteWriteAsync(string data)
		{
			await WriteAsync(data);
			await ExecuteRequestAsync();
		}

		public async Task ExecuteWriteAsync(Stream data)
		{
			await WriteAsync(data);
			await ExecuteRequestAsync();
		}

		public double CalculateDuration()
		{
			return 0;
		}

		public HttpJsonRequest AddReplicationStatusHeaders(string thePrimaryUrl, string currentUrl, ReplicationInformer replicationInformer, FailoverBehavior failoverBehavior, Action<string, string, string> handleReplicationStatusChanges)
		{
			if (thePrimaryUrl.Equals(currentUrl, StringComparison.OrdinalIgnoreCase))
				return this;
			if (replicationInformer.GetFailureCount(thePrimaryUrl) <= 0)
				return this; // not because of failover, no need to do this.

			var lastPrimaryCheck = replicationInformer.GetFailureLastCheck(thePrimaryUrl);
			httpClient.DefaultRequestHeaders.TryAddWithoutValidation(Constants.RavenClientPrimaryServerUrl, ToRemoteUrl(thePrimaryUrl));
			httpClient.DefaultRequestHeaders.TryAddWithoutValidation(Constants.RavenClientPrimaryServerLastCheck, lastPrimaryCheck.ToString("s"));

			primaryUrl = thePrimaryUrl;
			operationUrl = currentUrl;

			HandleReplicationStatusChanges = handleReplicationStatusChanges;

			return this;
		}

		private static string ToRemoteUrl(string primaryUrl)
		{
			var uriBuilder = new UriBuilder(primaryUrl);
			return uriBuilder.Uri.ToString();
		}

		public void PrepareForLongRequest()
		{
			Timeout = TimeSpan.FromHours(6);
			// webRequest.AllowWriteStreamBuffering = false;
		}

		public Task<Stream> GetRawRequestStream()
		{
			throw new NotImplementedException();
			//	return Task.Factory.FromAsync<Stream>(webRequest.BeginGetRequestStream, webRequest.EndGetRequestStream, null);
		}

		public Task<WebResponse> RawExecuteRequestAsync()
		{
			throw new NotImplementedException();
			/*
						if (isRequestSentToServer)
							throw new InvalidOperationException("Request was already sent to the server, cannot retry request.");

						isRequestSentToServer = true;
						webRequest.AllowReadStreamBuffering = false;
						webRequest.AllowWriteStreamBuffering = false;

						return WaitForTask.ContinueWith(_ => webRequest
																 .GetResponseAsync()
																 .ConvertSecurityExceptionToServerNotFound()
																 .AddUrlIfFaulting(webRequest.RequestUri))
																 .Unwrap();*/
		}
	}
}
