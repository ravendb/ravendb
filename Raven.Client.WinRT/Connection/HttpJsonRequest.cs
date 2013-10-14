//-----------------------------------------------------------------------
// <copyright file="HttpJsonRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Client.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;


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

		public Action<string, string, string> HandleReplicationStatusChanges = delegate { };

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
			return WriteAsync(postedData);
		}

		public Uri Url
		{
			get { return url; }
		}

		public string Method
		{
			get { return method.ToString(); }
		}

		public HttpResponseMessage Response { get; private set; }

		private HttpJsonRequestFactory factory;
		private bool writeCalled;

		internal HttpJsonRequest(string url, string method, RavenJObject metadata, DocumentConvention conventions, HttpJsonRequestFactory factory)
		{
			this.url = new Uri(url);
			this.conventions = conventions;
			this.factory = factory;
			this.method = new HttpMethod(method);

			var handler = new HttpClientHandler();
			httpClient = new HttpClient(handler);
			httpClient.DefaultRequestHeaders.Add("Raven-Client-Version", ClientVersion);

			WriteMetadata(metadata);
			if (method != "GET")
				httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json") {CharSet = "utf-8"});

			if (factory.DisableRequestCompression == false)
			if (method == "POST" || method == "PUT" || method == "PATCH" || method == "EVAL")
				httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Content-Encoding", "gzip");
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
			if (writeCalled == false)
			{
				Response = await httpClient.SendAsync(new HttpRequestMessage(method, url))
				                           .ConvertSecurityExceptionToServerNotFound()
				                           .AddUrlIfFaulting(url);
			}

			if (Response.IsSuccessStatusCode == false)
				throw new ErrorResponseException(Response);

			return await ReadStringInternal()
				            /* .ContinueWith(task => RetryIfNeedTo(task, ReadResponseStringAsync))
				             .Unwrap()*/
							 ;
		}

		/*private Task<T> RetryIfNeedTo<T>(HttpResponseMessage response, Func<Task<T>> generator)
		{
			if (retries >= 3)
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
		}*/

		private void HandleForbiddenResponseAsync(HttpResponseMessage forbiddenResponse)
		{
			if (conventions.HandleForbiddenResponseAsync == null)
				return;

			conventions.HandleForbiddenResponseAsync(forbiddenResponse);
		}

		public Task HandleUnauthorizedResponseAsync(HttpResponseMessage unauthorizedResponse)
		{
			if (conventions.HandleUnauthorizedResponseAsync == null)
				return null;

			var unauthorizedResponseAsync = conventions.HandleUnauthorizedResponseAsync(unauthorizedResponse);

			if (unauthorizedResponseAsync == null)
				return null;

			return unauthorizedResponseAsync.ContinueWith(task => RecreateWebRequest(task.Result)).Unwrap();
		}

		public async Task<byte[]> ReadResponseBytesAsync()
		{
			Response = await httpClient.SendAsync(new HttpRequestMessage(method, url))
			                           .ConvertSecurityExceptionToServerNotFound()
			                           .AddUrlIfFaulting(url);

			if (Response.IsSuccessStatusCode == false)
				throw new ErrorResponseException(Response);

			// TODO: Use RetryIfNeedTo(task, ReadResponseBytesAsync)
			return ConvertStreamToBytes(await Response.GetResponseStreamWithHttpDecompression());
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

		private async Task<string> ReadStringInternal()
		{
			var responseStream = await Response.GetResponseStreamWithHttpDecompression();
			var reader = new StreamReader(responseStream);
			var text = reader.ReadToEnd();
			return text;
		}

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
				{
					headerName = "If-None-Match";
					value = "\"" + value + "\"";
				}
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
		public async Task WriteAsync(string data)
		{
			writeCalled = true;
			Response = await httpClient.SendAsync(new HttpRequestMessage(method, url)
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

			Response = await httpClient.SendAsync(new HttpRequestMessage(method, url)
			{
				Content = new CompressedStreamContent(byteArray)
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

		public async Task<IObservable<string>> ServerPullAsync(int retries = 0)
		{
			httpClient.DefaultRequestHeaders.Add("Requires-Big-Initial-Download", "True");
			var result = await httpClient.GetAsync(url);
			var stream = await result.Content.ReadAsStreamAsync();
			var observableLineStream = new ObservableLineStream(stream, () =>
			{
				httpClient.CancelPendingRequests();
			});
			observableLineStream.Start();
			return (IObservable<string>) observableLineStream;

			/*try
			{

			}
			catch (HttpWebResponse)
			{
				if (retries >= 3)
					return task; // effectively throw
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
				throw;
			}*/
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

		public HttpJsonRequest AddReplicationStatusHeaders(string thePrimaryUrl, string currentUrl, ReplicationInformer replicationInformer, FailoverBehavior failoverBehavior, Action<string, string, string> handleReplicationStatusChanges)
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