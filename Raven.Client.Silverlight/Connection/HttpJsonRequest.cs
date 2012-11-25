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
using System.Reflection;
using System.Text;
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
		private readonly string url;
		private readonly DocumentConvention conventions;
		internal HttpWebRequest webRequest;
		private byte[] postedData;
		private int retries;
		public static readonly string ClientVersion = new AssemblyName(typeof(HttpJsonRequest).Assembly.FullName).Version.ToString();

		private string primaryUrl;

		private string operationUrl;

		public Action<NameValueCollection, string, string> HandleReplicationStatusChanges = delegate { };


		private Task RecreateWebRequest(Action<HttpWebRequest> result)
		{
			retries++;
			// we now need to clone the request, since just calling GetRequest again wouldn't do anything
			var newWebRequest = (HttpWebRequest)WebRequestCreator.ClientHttp.Create(new Uri(url));
			newWebRequest.Method = webRequest.Method;
			HttpJsonRequestHelper.CopyHeaders(webRequest, newWebRequest);
			newWebRequest.Credentials = webRequest.Credentials;
			result(newWebRequest);
			webRequest = newWebRequest;

			if (postedData == null)
			{
				var taskCompletionSource = new TaskCompletionSource<object>();
				taskCompletionSource.SetResult(null);
				return taskCompletionSource.Task;
			}
			else return WriteAsync(postedData);
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
			this.url = url;
			this.conventions = conventions;
			this.factory = factory;
			webRequest = (HttpWebRequest)WebRequestCreator.ClientHttp.Create(new Uri(url));
			noopWaitForTask = new CompletedTask();
			WaitForTask = noopWaitForTask;

			webRequest.Headers["Raven-Client-Version"] = ClientVersion;

			WriteMetadata(metadata);
			webRequest.Method = method;
			if (method != "GET")
				webRequest.ContentType = "application/json; charset=utf-8";
		
			if(factory.DisableRequestCompression)
				return;

			if (method == "POST" || method == "PUT" || method == "PATCH" || method == "EVAL")
				webRequest.Headers["Content-Encoding"] = "gzip";
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
			return WaitForTask.ContinueWith(_ => webRequest
													.GetResponseAsync()
													.ConvertSecurityExceptionToServerNotFound()
													.AddUrlIfFaulting(webRequest.RequestUri)
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
			if (webResponse == null || (webResponse.StatusCode != HttpStatusCode.Unauthorized && webResponse.StatusCode != HttpStatusCode.Forbidden))
				task.AssertNotFailed();

			if(webResponse.StatusCode == HttpStatusCode.Forbidden)
			{
				HandleForbbidenResponseAsync(webResponse);
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

		private void HandleForbbidenResponseAsync(HttpWebResponse forbbidenResponse)
		{
			if (conventions.HandleForbiddenResponseAsync == null)
				return;

			conventions.HandleForbiddenResponseAsync(forbbidenResponse);
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
			return WaitForTask.ContinueWith(_ => webRequest
													.GetResponseAsync()
													.ConvertSecurityExceptionToServerNotFound()
													.AddUrlIfFaulting(webRequest.RequestUri)
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

		private string ReadStringInternal(Func<WebResponse> getResponse)
		{
			return ReadResponse(getResponse, responseStream =>
				{
					var reader = new StreamReader(responseStream);
					var text = reader.ReadToEnd();
					return text;
				}
			);

		}

		private T ReadResponse<T>(Func<WebResponse> getResponse, Func<Stream, T> handleResponse)
		{
			WebResponse response;
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
			foreach (var key in response.Headers.AllKeys)
			{
				ResponseHeaders[key] = response.Headers[key];
			}
			
			ResponseStatusCode = ((HttpWebResponse)response).StatusCode;

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
					headerName == Constants.LastModified)
					continue;
				switch (headerName)
				{
					case "Content-Length":
						break;
					case "Content-Type":
						webRequest.ContentType = value;
						break;
					default:
						webRequest.Headers[headerName] = value;
						break;
				}
			}
		}

		/// <summary>
		/// Begins the write operation
		/// </summary>
		public Task WriteAsync(string data)
		{
			return WaitForTask.ContinueWith(_ =>
											webRequest.GetRequestStreamAsync()
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
			return WaitForTask.ContinueWith(_ => webRequest.GetRequestStreamAsync().ContinueWith(t =>
			{
				var dataStream = new GZipStream(t.Result, CompressionMode.Compress);
				using (dataStream)
				{
					dataStream.Write(byteArray, 0, byteArray.Length);
					dataStream.Close();
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
				webRequest.Headers[header.Key] = header.Value;
			}
			return this;
		}

		/// <summary>
		/// Adds the operation header
		/// </summary>
		public HttpJsonRequest AddOperationHeader(string key, string value)
		{
			webRequest.Headers[key] = value;
			return this;
		}

		public Task<IObservable<string>> ServerPullAsync(int retries = 0)
		{
			return WaitForTask.ContinueWith(__ =>
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
																						   task.Result.Close();
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
					   if (httpWebResponse == null || (httpWebResponse.StatusCode != HttpStatusCode.Unauthorized && httpWebResponse.StatusCode != HttpStatusCode.Forbidden))
						   return task; // effectively throw

					   if(httpWebResponse.StatusCode == HttpStatusCode.Forbidden)
					   {
						   HandleForbbidenResponseAsync(httpWebResponse);
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
			if (thePrimaryUrl.Equals(currentUrl, StringComparison.InvariantCultureIgnoreCase))
				return this;
			if (replicationInformer.GetFailureCount(thePrimaryUrl) <= 0)
				return this; // not because of failover, no need to do this.

			var lastPrimaryCheck = replicationInformer.GetFailureLastCheck(thePrimaryUrl);
			webRequest.Headers[Constants.RavenClientPrimaryServerUrl] = ToRemoteUrl(thePrimaryUrl);
			webRequest.Headers[Constants.RavenClientPrimaryServerLastCheck] = lastPrimaryCheck.ToString("s");

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