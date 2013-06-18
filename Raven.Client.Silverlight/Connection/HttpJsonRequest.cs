//-----------------------------------------------------------------------
// <copyright file="HttpJsonRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Browser;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Threading.Tasks;
using Ionic.Zlib;
using Raven.Abstractions.Util;
using Raven.Client.Silverlight.MissingFromSilverlight;
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

		private byte[] postedData;
		private int retries;
		public static readonly string ClientVersion = new AssemblyName(typeof (HttpJsonRequest).Assembly.FullName).Version.ToString();
		private bool disabledAuthRetries;

		private string primaryUrl;

		private string operationUrl;

		public Action<NameValueCollection, string, string> HandleReplicationStatusChanges = delegate { };

		public HttpResponseHeaders Headers
		{
			get { return Response.Headers; }
		}

		private async Task RecreateWebRequest(Action<HttpClient> result)
		{
			retries++;
			// we now need to clone the request, since just calling GetRequest again wouldn't do anything
			var newHttpClient = new HttpClient(new HttpClientHandler
			{
				Credentials = handler.Credentials,
			});
			// HttpJsonRequestHelper.CopyHeaders(webRequest, newWebRequest);
			result(newHttpClient);
			httpClient = newHttpClient;
			requestSendToServer = false;

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
			this.conventions = requestParams.Convention;
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
				httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json") {CharSet = "utf-8"});

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

		private bool requestSendToServer;

		/// <summary>
		/// Begins the read response string.
		/// </summary>
		private async Task<string> ReadResponseStringAsync()
		{
			if (requestSendToServer)
				throw new InvalidOperationException("Request was already sent to the server, cannot retry request.");

			requestSendToServer = true;

			await WaitForTask;

			if (writeCalled == false)
			{
				Response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(Method), Url))
				                           .ConvertSecurityExceptionToServerNotFound()
				                           .MaterializeBadRequestAsException()
				                           .AddUrlIfFaulting(new Uri(Url));
			}

			if (Response.IsSuccessStatusCode == false)
				throw new ErrorResponseException(Response);

			return await ReadStringInternal();
			//	 .ContinueWith(task => RetryIfNeedTo(task, ReadResponseStringAsync)).Unwrap()
			;
		}

		/*private Task<T> RetryIfNeedTo<T>(Task<T> task, Func<Task<T>> generator)
		{
			var exception = task.Exception.ExtractSingleInnerException() as WebException;
			if (exception == null || retries >= 3 || disabledAuthRetries)
				return task;

			var webResponse = exception.Response as HttpWebResponse;
			if (webResponse == null ||
			    (webResponse.StatusCode != HttpStatusCode.Unauthorized &&
			     webResponse.StatusCode != HttpStatusCode.Forbidden &&
			     webResponse.StatusCode != HttpStatusCode.PreconditionFailed))
				task.AssertNotFailed();

			if (webResponse.StatusCode == HttpStatusCode.Forbidden)
			{
				HandleForbiddenResponseAsync(webResponse);
				task.AssertNotFailed();
			}

			var authorizeResponse = HandleUnauthorizedResponseAsync(webResponse);

			if (authorizeResponse == null)
			{
				task.AssertNotFailed();
				return task; // never get called
			}


			return authorizeResponse
				.ContinueWith(task1 =>
				{
					task1.Wait(); // throw if error
					return generator();
				})
				.Unwrap();
		}*/

		private void HandleForbiddenResponseAsync(HttpResponseMessage forbiddenResponse)
		{
			if (conventions.HandleForbiddenResponseAsync == null)
				return;

			conventions.HandleForbiddenResponseAsync(forbiddenResponse);
		}

		public async Task HandleUnauthorizedResponseAsync(HttpResponseMessage unauthorizedResponse)
		{
			if (conventions.HandleUnauthorizedResponseAsync == null)
				return;

			var unauthorizedResponseAsync = conventions.HandleUnauthorizedResponseAsync(unauthorizedResponse);
			if (unauthorizedResponseAsync == null)
				return;

			var result = await unauthorizedResponseAsync;
			// await RecreateWebRequest(result);
			throw new NotImplementedException();
		}

		public async Task<byte[]> ReadResponseBytesAsync()
		{
			await WaitForTask;
			
			Response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(Method), Url))
			                           .ConvertSecurityExceptionToServerNotFound()
			                           .AddUrlIfFaulting(new Uri(Url));

			if (Response.IsSuccessStatusCode == false)
				throw new ErrorResponseException(Response);

			// TODO: Use RetryIfNeedTo(task, ReadResponseBytesAsync)
			return ConvertStreamToBytes(await Response.GetResponseStreamWithHttpDecompression());
		}

		private static byte[] ConvertStreamToBytes(Stream input)
		{
			var buffer = new byte[16*1024];
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

		/*private T ReadResponse<T>(Func<Stream, T> handleResponse)
		{
			var httpWebResponse = e.Response as HttpWebResponse;
			if (httpWebResponse == null ||
				httpWebResponse.StatusCode == HttpStatusCode.NotFound ||
				httpWebResponse.StatusCode == HttpStatusCode.Conflict)
				throw;

			using (var sr = new StreamReader(e.Response.GetResponseStream()))
			{
				throw new InvalidOperationException(sr.ReadToEnd(), e);
			}

			ResponseHeaders = new NameValueCollection();
			foreach (var key in response.Headers.AllKeys)
			{
				ResponseHeaders[key] = response.Headers[key];
			}

			ResponseStatusCode = ((HttpWebResponse) response).StatusCode;

			using (var responseStream = response.GetResponseStreamWithHttpDecompression())
			{
				return handleResponse(responseStream);
			}
		}*/


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
				switch (headerName)
				{
					case "Content-Length":
						break;
					case "Content-Type":
						//webRequest.ContentType = value;
						break;
					default:
						//webRequest.Headers[headerName] = value;
						break;
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
		public async Task WriteAsync(byte[] byteArray)
		{
			writeCalled = true;
			postedData = byteArray;

			using (var stream = new MemoryStream(byteArray))
			using (var dataStream = new GZipStream(stream, CompressionMode.Compress))
			{
				Response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(Method), Url)
				{
					Content = new StreamContent(dataStream)
				});

				if (Response.IsSuccessStatusCode == false)
					throw new ErrorResponseException(Response);
			}
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

		public Task<IObservable<string>> ServerPullAsync(int retries = 0)
		{
			throw new NotImplementedException();
			/*return WaitForTask.ContinueWith(__ =>
			{
				webRequest.AllowReadStreamBuffering = false;
				webRequest.AllowWriteStreamBuffering = false;
				webRequest.Headers["Requires-Big-Initial-Download"] = "True";
				return webRequest.GetResponseAsync()
				   .ContinueWith(task =>
				   {
					   var stream = task.Result.GetResponseStream();
					   var observableLineStream = new ObservableLineStream(stream, () =>
																					   {
																						   webRequest.Abort();
																						   try
																						   {
																							   task.Result.Close();
																						   }
																						   catch (Exception)
																						   {
																							   // we expect an exception, because we aborted the connection
																						   }
																					   });
					   observableLineStream.Start();
					   return (IObservable<string>)observableLineStream;
				   })
				   .ContinueWith(task =>
				   {
					   var webException = task.Exception.ExtractSingleInnerException() as WebException;
					   if (webException == null || retries >= 3 || disabledAuthRetries)
						   return task;// effectively throw

					   var httpWebResponse = webException.Response as HttpWebResponse;
					   if (httpWebResponse == null ||
							(httpWebResponse.StatusCode != HttpStatusCode.Unauthorized &&
							 httpWebResponse.StatusCode != HttpStatusCode.Forbidden &&
							 httpWebResponse.StatusCode != HttpStatusCode.PreconditionFailed))
						   return task; // effectively throw

					   if (httpWebResponse.StatusCode == HttpStatusCode.Forbidden)
					   {
						   HandleForbiddenResponseAsync(httpWebResponse);
						   return task;
					   }

					   var authorizeResponse = HandleUnauthorizedResponseAsync(httpWebResponse);

					   if (authorizeResponse == null)
						   return task; // effectively throw

					   return authorizeResponse
						   .ContinueWith(_ =>
						   {
							   _.Wait(); //throw on error
							   return ServerPullAsync(retries + 1);
						   })
						   .Unwrap();
				   }).Unwrap();
			})
				.Unwrap();*/
		}

		public async Task ExecuteWriteAsync(string data)
		{
			await WriteAsync(data);
			await ExecuteRequestAsync();
		}

		public async Task ExecuteWriteAsync(byte[] data)
		{
			await WriteAsync(data);
			await ExecuteRequestAsync();
		}

		public double CalculateDuration()
		{
			return 0;
		}

		public HttpJsonRequest AddReplicationStatusHeaders(string thePrimaryUrl, string currentUrl, ReplicationInformer replicationInformer, FailoverBehavior failoverBehavior, Action<NameValueCollection, string, string> handleReplicationStatusChanges)
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
			if (requestSendToServer)
				throw new InvalidOperationException("Request was already sent to the server, cannot retry request.");

			requestSendToServer = true;
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