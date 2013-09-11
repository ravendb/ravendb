//-----------------------------------------------------------------------
// <copyright file="HttpJsonRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Client.Linq;
using Raven.Client.Silverlight.MissingFromSilverlight;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Client.Extensions;


namespace Raven.Client.WinRT.Connection
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
		private readonly Uri url;
		private readonly DocumentConvention conventions;
		private readonly HttpMethod method;
		internal HttpClient httpClient;
		private byte[] postedData;
		private int retries;
		public static readonly string ClientVersion = new AssemblyName(typeof(HttpJsonRequest).Assembly().FullName).Version.ToString();

		private string primaryUrl;

		private string operationUrl;

		public Action<NameValueCollection, string, string> HandleReplicationStatusChanges = delegate { };


		private Task RecreateWebRequest(Action<HttpWebRequest> result)
		{
			retries++;
			// result(httpClient);

			if (postedData == null)
			{
				var taskCompletionSource = new TaskCompletionSource<object>();
				taskCompletionSource.SetResult(null);
				return taskCompletionSource.Task;
			}
			else return WriteAsync(postedData);
		}

		public Uri Url
		{
			get { return url; }
		}

		public string Method
		{
			get { return method.ToString(); }
		}

		private HttpJsonRequestFactory factory;

		private static Task noopWaitForTask = new CompletedTask();

		/// <summary>
		/// Gets or sets the response headers.
		/// </summary>
		/// <value>The response headers.</value>
		public NameValueCollection ResponseHeaders { get; set; }

		internal HttpJsonRequest(string url, string method, RavenJObject metadata, DocumentConvention conventions, HttpJsonRequestFactory factory)
		{
			this.url = new Uri(url);
			this.conventions = conventions;
			this.factory = factory;
			this.method = new HttpMethod(method);

			var handler = new HttpClientHandler
			{
				
			};
			httpClient = new HttpClient(handler);
			httpClient.DefaultRequestHeaders.Add("Raven-Client-Version", ClientVersion);

			noopWaitForTask = new CompletedTask();
			WaitForTask = noopWaitForTask;

			WriteMetadata(metadata);
			if (method != "GET")
				httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json; charset=utf-8"));

			if (factory.DisableRequestCompression)
				return;

			if (method == "POST" || method == "PUT" || method == "PATCH" || method == "EVAL")
				httpClient.DefaultRequestHeaders.Add("Content-Encoding", "gzip");
		}

		public Task<RavenJToken> ReadResponseJsonAsync()
		{
			return ReadResponseStringAsync()
				.ContinueWith(task => RavenJToken.Parse(task.Result));
		}

		public Task ExecuteRequestAsync()
		{
			return ReadResponseStringAsync();
		}

		/// <summary>
		/// Begins the read response string.
		/// </summary>
		private Task<string> ReadResponseStringAsync()
		{
			var requestMessage = new HttpRequestMessage(method, url);
			return WaitForTask.ContinueWith(_ => httpClient.SendAsync(requestMessage)
													.ConvertSecurityExceptionToServerNotFound()
													.AddUrlIfFaulting(url)
													.ContinueWith(t => ReadStringInternal(() => t.Result))
													.ContinueWith(task => RetryIfNeedTo(task, ReadResponseStringAsync))
													.Unwrap())
													.Unwrap();
		}

		private Task<T> RetryIfNeedTo<T>(Task<T> task, Func<Task<T>> generator)
		{
			var exception = task.Exception.ExtractSingleInnerException() as WebException;
			if (exception == null || retries >= 3)
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
				return task;// never get called
			}


			return authorizeResponse
				.ContinueWith(task1 =>
				{
					task1.Wait();// throw if error
					return generator();
				})
				.Unwrap();
		}

		private void HandleForbiddenResponseAsync(HttpWebResponse forbiddenResponse)
		{
			if (conventions.HandleForbiddenResponseAsync == null)
				return;

			conventions.HandleForbiddenResponseAsync(forbiddenResponse);
		}

		public Task HandleUnauthorizedResponseAsync(HttpWebResponse unauthorizedResponse)
		{
			if (conventions.HandleUnauthorizedResponseAsync == null)
				return null;

			var unauthorizedResponseAsync = conventions.HandleUnauthorizedResponseAsync(unauthorizedResponse);

			if (unauthorizedResponseAsync == null)
				return null;

			return unauthorizedResponseAsync.ContinueWith(task => RecreateWebRequest(task.Result)).Unwrap();
		}

		public Task<byte[]> ReadResponseBytesAsync()
		{
			return WaitForTask.ContinueWith(_ => httpClient.SendAsync(new HttpRequestMessage(method, url))
													.ConvertSecurityExceptionToServerNotFound()
													.AddUrlIfFaulting(url)
													.ContinueWith(t => ReadResponse(() => t.Result, ConvertStreamToBytes))
													.ContinueWith(task => RetryIfNeedTo(task, ReadResponseBytesAsync))
													.Unwrap())
													.Unwrap();
		}

		static byte[] ConvertStreamToBytes(Stream input)
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

		private string ReadStringInternal(Func<HttpResponseMessage> getResponse)
		{
			return ReadResponse(getResponse, responseStream =>
			{
				var reader = new StreamReader(responseStream);
				var text = reader.ReadToEnd();
				return text;
			}
			);

		}

		private T ReadResponse<T>(Func<HttpResponseMessage> getResponse, Func<Stream, T> handleResponse)
		{
			HttpResponseMessage response;
			try
			{
				response = getResponse();
			}
			catch (WebException e)
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
			}

			ResponseHeaders = new NameValueCollection();
			foreach (var header in response.Headers)
			{
				ResponseHeaders[header.Key] = string.Join(";", header.Value);
			}

			ResponseStatusCode = response.StatusCode;

			using (var responseStream = response.GetResponseStreamWithHttpDecompression())
			{
				return handleResponse(responseStream);
			}
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
						httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue(value));
						break;
					default:
						httpClient.DefaultRequestHeaders.Add(headerName, value);
						break;
				}
			}
		}

		/// <summary>
		/// Begins the write operation
		/// </summary>
		public Task WriteAsync(string data)
		{
			return WaitForTask.ContinueWith(_ => httpClient.GetStreamAsync(url)
			                                               .ContinueWith(task =>
			                                               {
				                                               Stream dataStream = factory.DisableRequestCompression == false ?
					                                                                   new GZipStream(task.Result, CompressionMode.Compress) :
					                                                                   task.Result;
				                                               var streamWriter = new StreamWriter(dataStream, Encoding.UTF8);
				                                               return streamWriter.WriteAsync(data)
				                                                                  .ContinueWith(writeTask =>
				                                                                  {
					                                                                  streamWriter.Dispose();
					                                                                  dataStream.Dispose();
					                                                                  task.Result.Dispose();
					                                                                  return writeTask;
				                                                                  }).Unwrap();
			                                               }).Unwrap())
			                  .Unwrap();
		}

		/// <summary>
		/// Begins the write operation
		/// </summary>
		public Task WriteAsync(byte[] byteArray)
		{
			postedData = byteArray;
			return WaitForTask.ContinueWith(_ => httpClient.GetStreamAsync(url).ContinueWith(t =>
			{
				var dataStream = new GZipStream(t.Result, CompressionMode.Compress);
				using (dataStream)
				{
					dataStream.Write(byteArray, 0, byteArray.Length);
				}
			})).Unwrap();
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
			return WaitForTask.ContinueWith(__ =>
			{
				httpClient.DefaultRequestHeaders.Add("Requires-Big-Initial-Download", "True");
				return httpClient.GetAsync(url)
				   .ContinueWith(task =>
				   {
					   var stream = task.Result.Content.ReadAsStreamAsync().Result;
					   var observableLineStream = new ObservableLineStream(stream, () =>
					   {
						   httpClient.CancelPendingRequests();
					   });
					   observableLineStream.Start();
					   return (IObservable<string>)observableLineStream;
				   })
				   .ContinueWith(task =>
				   {
					   var webException = task.Exception.ExtractSingleInnerException() as WebException;
					   if (webException == null || retries >= 3)
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
				.Unwrap();
		}

		public Task ExecuteWriteAsync(string data)
		{
			return WriteAsync(data)
				.ContinueWith(task =>
				{
					if (task.IsFaulted)
						return task;
					return ExecuteRequestAsync();
				})
				.Unwrap();
		}

		public Task ExecuteWriteAsync(byte[] data)
		{
			return WriteAsync(data)
				.ContinueWith(task =>
				{
					if (task.IsFaulted)
						return task;
					return ExecuteRequestAsync();
				})
				.Unwrap();
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
			httpClient.DefaultRequestHeaders.Add(Constants.RavenClientPrimaryServerUrl, ToRemoteUrl(thePrimaryUrl));
			httpClient.DefaultRequestHeaders.Add(Constants.RavenClientPrimaryServerLastCheck,  lastPrimaryCheck.ToString("s"));

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
	}
}