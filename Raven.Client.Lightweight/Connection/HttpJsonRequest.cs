//-----------------------------------------------------------------------
// <copyright file="HttpJsonRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;
using System.Threading;
#if !NET35
using System.Threading.Tasks;
#endif
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	/// <summary>
	/// A representation of an HTTP json request to the RavenDB server
	/// </summary>
	public class HttpJsonRequest
	{
		internal readonly string Url;
		internal readonly string Method;

		internal volatile HttpWebRequest webRequest;
		// temporary create a strong reference to the cached data for this request
		// avoid the potential for clearing the cache from a cached item
		internal CachedRequest CachedRequestDetails;
		private readonly HttpJsonRequestFactory factory;
		private readonly IHoldProfilingInformation owner;
		private readonly DocumentConvention conventions;
		private string postedData;
		private Stopwatch sp = Stopwatch.StartNew();
		internal bool ShouldCacheRequest;
		public object Headers;
		private Stream postedStream;
		private bool writeCalled;
		public static readonly string ClientVersion = typeof (HttpJsonRequest).Assembly.GetName().Version.ToString();

		/// <summary>
		/// Gets or sets the response headers.
		/// </summary>
		/// <value>The response headers.</value>
		public NameValueCollection ResponseHeaders { get; set; }

		internal HttpJsonRequest(
			CreateHttpJsonRequestParams requestParams,
			HttpJsonRequestFactory factory)
		{
			Url = requestParams.Url;
			this.factory = factory;
			owner = requestParams.Owner;
			conventions = requestParams.Convention;
			Method = requestParams.Method;
			webRequest = (HttpWebRequest)WebRequest.Create(requestParams.Url);
			webRequest.UseDefaultCredentials = true;
			webRequest.Credentials = requestParams.Credentials;
			webRequest.Method = requestParams.Method;
			if (factory.DisableRequestCompression == false &&
				(requestParams.Method == "POST" || requestParams.Method == "PUT" || 
				 requestParams.Method == "PATCH" || requestParams.Method == "EVAL"))
				webRequest.Headers["Content-Encoding"] = "gzip";
			webRequest.ContentType = "application/json; charset=utf-8";
			webRequest.Headers.Add("Raven-Client-Version", ClientVersion);
			WriteMetadata(requestParams.Metadata);
			requestParams.UpdateHeaders(webRequest);
		}

#if !NET35

		public Task ExecuteRequestAsync()
		{
			return ReadResponseJsonAsync();
		}

		/// <summary>
		/// Begins the read response string.
		/// </summary>
		public Task<RavenJToken> ReadResponseJsonAsync()
		{
			if (SkipServerCheck)
			{
				var tcs = new TaskCompletionSource<RavenJToken>();
				var cachedResponse = factory.GetCachedResponse(this);
				factory.InvokeLogRequest(owner, ()=> new RequestResultArgs
				{
					DurationMilliseconds = CalculateDuration(),
					Method = webRequest.Method,
					HttpResult = (int)ResponseStatusCode,
					Status = RequestStatus.AggresivelyCached,
					Result = cachedResponse.ToString(),
					Url = webRequest.RequestUri.PathAndQuery,
					PostedData = postedData
				});
				tcs.SetResult(cachedResponse);
				return tcs.Task;
			}

			return InternalReadResponseStringAsync(retries: 0);
		}

		private Task<RavenJToken> InternalReadResponseStringAsync(int retries)
		{
			return Task.Factory.FromAsync<WebResponse>(webRequest.BeginGetResponse, webRequest.EndGetResponse, null)
				.ContinueWith(task => ReadJsonInternal(() => task.Result))
				.ContinueWith(task =>
				{
					var webException = task.Exception.ExtractSingleInnerException() as WebException;
					if (webException == null || retries >= 3)
						return task;// effectively throw

					var httpWebResponse = webException.Response as HttpWebResponse;
					if (httpWebResponse == null ||
						httpWebResponse.StatusCode != HttpStatusCode.Unauthorized)
						return task; // effectively throw

					var authorizeResponse = HandleUnauthorizedResponseAsync(httpWebResponse);

					if (authorizeResponse == null)
						return task; // effectively throw

					return authorizeResponse
						.ContinueWith(_ =>
						{
							_.Wait(); //throw on error
							return InternalReadResponseStringAsync(retries + 1);
						})
						.Unwrap();
				}).Unwrap();
		}

		public Task<byte[]> ReadResponseBytesAsync()
		{
			if (!writeCalled)
				webRequest.ContentLength = 0;
			return Task.Factory.FromAsync<WebResponse>(webRequest.BeginGetResponse, webRequest.EndGetResponse, null)
				.ContinueWith(task =>
				{
					using (var stream = task.Result.GetResponseStreamWithHttpDecompression())
					{
						ResponseHeaders = new NameValueCollection(task.Result.Headers);
						return stream.ReadData();
					}
				});
		}
#endif
		public void ExecuteRequest()
		{
			ReadResponseJson();
		}

		public byte[] ReadResponseBytes()
		{
			if (writeCalled == false)
				webRequest.ContentLength = 0;
			using (var webResponse = webRequest.GetResponse())
			using (var stream = webResponse.GetResponseStreamWithHttpDecompression())
			{
				ResponseHeaders = new NameValueCollection(webResponse.Headers);
				return stream.ReadData();
			}
		}

		/// <summary>
		/// Reads the response string.
		/// </summary>
		/// <returns></returns>
		public RavenJToken ReadResponseJson()
		{
			if (SkipServerCheck)
			{
				var result = factory.GetCachedResponse(this);
				factory.InvokeLogRequest(owner, () => new RequestResultArgs
				{
					DurationMilliseconds = CalculateDuration(),
					Method = webRequest.Method,
					HttpResult = (int)ResponseStatusCode,
					Status = RequestStatus.AggresivelyCached,
					Result = result.ToString(),
					Url = webRequest.RequestUri.PathAndQuery,
					PostedData = postedData
				});
				return result;
			}

			int retries = 0;
			while (true)
			{
				try
				{
					if (writeCalled == false)
						webRequest.ContentLength = 0;
					return ReadJsonInternal(webRequest.GetResponse);
				}
				catch (WebException e)
				{
					if (++retries >= 3)
						throw;

					var httpWebResponse = e.Response as HttpWebResponse;
					if (httpWebResponse == null ||
						httpWebResponse.StatusCode != HttpStatusCode.Unauthorized)
						throw;

					if (HandleUnauthorizedResponse(httpWebResponse) == false)
						throw;
				}
			}
		}

		public bool HandleUnauthorizedResponse(HttpWebResponse unauthorizedResponse)
		{
			if (conventions.HandleUnauthorizedResponse == null)
				return false;

			var handleUnauthorizedResponse = conventions.HandleUnauthorizedResponse(unauthorizedResponse);
			if (handleUnauthorizedResponse == null)
				return false;

			RecreateWebRequest(handleUnauthorizedResponse);
			return true;
		}
#if !NET35
		public Task HandleUnauthorizedResponseAsync(HttpWebResponse unauthorizedResponse)
		{
			if (conventions.HandleUnauthorizedResponseAsync == null)
				return null;

			var unauthorizedResponseAsync = conventions.HandleUnauthorizedResponseAsync(unauthorizedResponse);

			if (unauthorizedResponseAsync == null)
				return null;

			return unauthorizedResponseAsync.ContinueWith(task => RecreateWebRequest(unauthorizedResponseAsync.Result));
		}
#endif

		private void RecreateWebRequest(Action<HttpWebRequest> action)
		{
			// we now need to clone the request, since just calling GetRequest again wouldn't do anything

			var newWebRequest = (HttpWebRequest)WebRequest.Create(Url);
			newWebRequest.Method = webRequest.Method;
			HttpRequestHelper.CopyHeaders(webRequest, newWebRequest);
			newWebRequest.Credentials = webRequest.Credentials;
			action(newWebRequest);

			if (postedData != null)
			{
				HttpRequestHelper.WriteDataToRequest(newWebRequest, postedData, factory.DisableRequestCompression);
			}
			if (postedStream != null)
			{
				postedStream.Position = 0;
				using (var stream = newWebRequest.GetRequestStream())
				using (var commpressedData = new GZipStream(stream, CompressionMode.Compress))
				{
					if (factory.DisableRequestCompression == false)
						postedStream.CopyTo(commpressedData);
					else
						postedStream.CopyTo(stream);

					commpressedData.Flush();
					stream.Flush();
				}
			}
			webRequest = newWebRequest;
		}



		private RavenJToken ReadJsonInternal(Func<WebResponse> getResponse)
		{
			WebResponse response;
			try
			{
				response = getResponse();
				sp.Stop();
			}
			catch (WebException e)
			{
				sp.Stop();
				var result = HandleErrors(e);
				if (result == null)
					throw;
				return result;
			}
#if !NET35
			catch (AggregateException e)
			{
				sp.Stop();
				var we = e.ExtractSingleInnerException() as WebException;
				if (we == null)
					throw;
				var result = HandleErrors(we);
				if (result == null)
					throw;
				return result;
			}
#endif

			ResponseHeaders = new NameValueCollection(response.Headers);
			ResponseStatusCode = ((HttpWebResponse)response).StatusCode;
			using (var responseStream = response.GetResponseStreamWithHttpDecompression())
			{
				var data = RavenJToken.TryLoad(responseStream);

				if (Method == "GET" && ShouldCacheRequest)
				{
					factory.CacheResponse(Url, data, ResponseHeaders);
				}

				factory.InvokeLogRequest(owner, () => new RequestResultArgs
				{
					DurationMilliseconds = CalculateDuration(),
					Method = webRequest.Method,
					HttpResult = (int) ResponseStatusCode,
					Status = RequestStatus.SentToServer,
					Result = (data ?? "").ToString(),
					Url = webRequest.RequestUri.PathAndQuery,
					PostedData = postedData
				});

				return data;
			}
		}

		private RavenJToken HandleErrors(WebException e)
		{
			var httpWebResponse = e.Response as HttpWebResponse;
			if (httpWebResponse == null ||
				httpWebResponse.StatusCode == HttpStatusCode.Unauthorized ||
				httpWebResponse.StatusCode == HttpStatusCode.NotFound ||
				httpWebResponse.StatusCode == HttpStatusCode.Conflict)
			{
				int httpResult = -1;
				if (httpWebResponse != null)
					httpResult = (int)httpWebResponse.StatusCode;

				factory.InvokeLogRequest(owner, () => new RequestResultArgs
				{
					DurationMilliseconds = CalculateDuration(),
					Method = webRequest.Method,
					HttpResult = httpResult,
					Status = RequestStatus.ErrorOnServer,
					Result = e.Message,
					Url = webRequest.RequestUri.PathAndQuery,
					PostedData = postedData
				});

				return null;//throws
			}

			if (httpWebResponse.StatusCode == HttpStatusCode.NotModified
				&& CachedRequestDetails != null)
			{
				factory.UpdateCacheTime(this);
				var result = factory.GetCachedResponse(this);

				factory.InvokeLogRequest(owner, () => new RequestResultArgs
				{
					DurationMilliseconds = CalculateDuration(),
					Method = webRequest.Method,
					HttpResult = (int)httpWebResponse.StatusCode,
					Status = RequestStatus.Cached,
					Result = result.ToString(),
					Url = webRequest.RequestUri.PathAndQuery,
					PostedData = postedData
				});

				return result;
			}

			using (var sr = new StreamReader(e.Response.GetResponseStreamWithHttpDecompression()))
			{
				var readToEnd = sr.ReadToEnd();

				factory.InvokeLogRequest(owner, () => new RequestResultArgs
				{
					DurationMilliseconds = CalculateDuration(),
					Method = webRequest.Method,
					HttpResult = (int)httpWebResponse.StatusCode,
					Status = RequestStatus.Cached,
					Result = readToEnd,
					Url = webRequest.RequestUri.PathAndQuery,
					PostedData = postedData
				});

				RavenJObject ravenJObject;
				try
				{
					ravenJObject = RavenJObject.Parse(readToEnd);
				}
				catch (Exception)
				{
					throw new InvalidOperationException(readToEnd, e);
				}

				if (ravenJObject.ContainsKey("Error"))
				{
					var sb = new StringBuilder();
					foreach (var prop in ravenJObject)
					{
						if (prop.Key == "Error")
							continue;

						sb.Append(prop.Key).Append(": ").AppendLine(prop.Value.ToString(Formatting.Indented));
					}

					sb.AppendLine()
						.AppendLine(ravenJObject.Value<string>("Error"));

					throw new InvalidOperationException(sb.ToString());
				}
				throw new InvalidOperationException(readToEnd, e);
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
				webRequest.Headers[header.Key] = header.Value;
			}
			return this;
		}

		/// <summary>
		/// The request duration
		/// </summary>
		public double CalculateDuration()
		{
			return sp.ElapsedMilliseconds;
		}

		/// <summary>
		/// Gets or sets the response status code.
		/// </summary>
		/// <value>The response status code.</value>
		public HttpStatusCode ResponseStatusCode { get; set; }

		///<summary>
		/// Whatever we can skip the server check and directly return the cached result
		///</summary>
		public bool SkipServerCheck { get; set; }

		/// <summary>
		/// The underlying request content type
		/// </summary>
		public string ContentType
		{
			get { return webRequest.ContentType; }
			set { webRequest.ContentType = value; }
		}

		private void WriteMetadata(RavenJObject metadata)
		{
			if (metadata == null || metadata.Count == 0)
			{
				return;
			}

			foreach (var prop in metadata)
			{
				if (prop.Value == null)
					continue;

				if (prop.Value.Type == JTokenType.Object ||
					prop.Value.Type == JTokenType.Array)
					continue;

				var headerName = prop.Key;
				if (headerName == "ETag")
					headerName = "If-None-Match";
				var value = prop.Value.Value<object>().ToString();

				bool isRestricted;
				try
				{
					isRestricted = WebHeaderCollection.IsRestricted(headerName);
				}
				catch (Exception e)
				{
					throw new InvalidOperationException("Could not figure out how to treat header: " + headerName, e);
				}
				// Restricted headers require their own special treatment, otherwise an exception will
				// be thrown.
				// See http://msdn.microsoft.com/en-us/library/78h415ay.aspx
				if (isRestricted)
				{
					switch (headerName)
					{
						/*case "Date":
						case "Referer":
						case "Content-Length":
						case "Expect":
						case "Range":
						case "Transfer-Encoding":
						case "User-Agent":
						case "Proxy-Connection":
						case "Host": // Host property is not supported by 3.5
							break;*/
						case "Content-Type":
							webRequest.ContentType = value;
							break;
						case "If-Modified-Since":
							DateTime tmp;
							DateTime.TryParse(value, out tmp);
							webRequest.IfModifiedSince = tmp;
							break;
						case "Accept":
							webRequest.Accept = value;
							break;
						case "Connection":
							webRequest.Connection = value;
							break;
					}
				}
				else
				{
					webRequest.Headers[headerName] = value;
				}
			}
		}

		/// <summary>
		/// Writes the specified data.
		/// </summary>
		/// <param name="data">The data.</param>
		public void Write(string data)
		{
			writeCalled = true;
			postedData = data;

			HttpRequestHelper.WriteDataToRequest(webRequest, data, factory.DisableRequestCompression);
		}


		/// <summary>
		/// Begins the write operation
		/// </summary>
		/// <param name="dataToWrite">The byte array.</param>
		/// <param name="callback">The callback.</param>
		/// <param name="state">The state.</param>
		/// <returns></returns>
		public IAsyncResult BeginWrite(string dataToWrite, AsyncCallback callback, object state)
		{
			writeCalled = true;
			postedData = dataToWrite;

			return webRequest.BeginGetRequestStream(callback, state);
		}

		/// <summary>
		/// Ends the write operation.
		/// </summary>
		/// <param name="result">The result.</param>
		public void EndWrite(IAsyncResult result)
		{
			using (var dataStream = webRequest.EndGetRequestStream(result))
			using (var compressed = new GZipStream(dataStream, CompressionMode.Compress))
			using (var writer = new StreamWriter(compressed, Encoding.UTF8))
			{
				writer.Write(postedData);
				writer.Flush();

				compressed.Flush();
				dataStream.Flush();
			}
		}


		private class ImmediateCompletionResult : IAsyncResult, IDisposable
		{
			private ManualResetEvent manualResetEvent;

			public bool IsCompleted
			{
				get { return true; }
			}

			public WaitHandle AsyncWaitHandle
			{
				get
				{
					if (manualResetEvent == null)
					{
						lock (this)
						{
							if (manualResetEvent == null)
								manualResetEvent = new ManualResetEvent(true);
						}
					}
					return manualResetEvent;
				}
			}

			public object AsyncState
			{
				get { return null; }
			}

			public bool CompletedSynchronously
			{
				get { return true; }
			}

			public void Dispose()
			{
				if (manualResetEvent != null)
					manualResetEvent.Close();
			}
		}

		public void Write(Stream streamToWrite)
		{
			writeCalled = true;
			postedStream = streamToWrite;
			webRequest.SendChunked = true;
			using (var stream = webRequest.GetRequestStream())
			using (var commpressedData = new GZipStream(stream, CompressionMode.Compress))
			{
				streamToWrite.CopyTo(commpressedData);

				commpressedData.Flush();
				stream.Flush();
			}
		}

		public Task<IObservable<string>>  ServerPullAsync(int retries = 0)
		{
			return Task.Factory.FromAsync<WebResponse>(webRequest.BeginGetResponse, webRequest.EndGetResponse, null)
				.ContinueWith(task =>
				              	{
				              		var stream = task.Result.GetResponseStreamWithHttpDecompression();
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
					if (httpWebResponse == null ||
						httpWebResponse.StatusCode != HttpStatusCode.Unauthorized)
						return task; // effectively throw

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
		}

		public Task ExecuteWriteAsync(string data)
		{
			try
			{
				Write(data);
				return ExecuteRequestAsync();
			}
			catch (Exception e)
			{
				return new CompletedTask(e);
			}
		}
	}
}
