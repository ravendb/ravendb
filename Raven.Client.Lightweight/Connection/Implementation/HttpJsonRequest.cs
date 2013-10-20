#if !NETFX_CORE && !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="HttpJsonRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
#if SILVERLIGHT || NETFX_CORE
using Raven.Client.Silverlight.MissingFromSilverlight;
#else
using System.Collections.Specialized;
#endif
using System.Diagnostics;
using System.IO;
#if NETFX_CORE
using Raven.Client.WinRT.Connection;
#endif
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	using Raven.Client.Extensions;

	/// <summary>
	/// A representation of an HTTP json request to the RavenDB server
	/// </summary>
	public class HttpJsonRequest
	{
		internal readonly string Url;
		internal readonly string Method;

		private volatile HttpClient httpClient;
        private readonly NameValueCollection headers = new NameValueCollection();

		// temporary create a strong reference to the cached data for this request
		// avoid the potential for clearing the cache from a cached item
		internal CachedRequest CachedRequestDetails;
		private readonly HttpJsonRequestFactory factory;
		private readonly IHoldProfilingInformation owner;
		private readonly DocumentConvention conventions;
		private string postedData = null;
		private readonly Stopwatch sp = Stopwatch.StartNew();
		internal bool ShouldCacheRequest;
		private Stream postedStream;
		private bool writeCalled;
		public static readonly string ClientVersion = typeof(HttpJsonRequest).Assembly.GetName().Version.ToString();
		private bool disabledAuthRetries;
		private string primaryUrl;

		private string operationUrl;

		public Action<string, string, string> HandleReplicationStatusChanges = delegate { };
		private readonly WebRequestHandler handler;

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
			Method = requestParams.Method;

			this.factory = factory;
			owner = requestParams.Owner;
			conventions = requestParams.Convention;

			handler = new WebRequestHandler
			{
				UseDefaultCredentials = true,
				Credentials = requestParams.Credentials,
			};
			httpClient = new HttpClient(handler);

			if (factory.DisableRequestCompression == false && requestParams.DisableRequestCompression == false)
			{
				if (requestParams.Method == "POST" || requestParams.Method == "PUT" ||
				    requestParams.Method == "PATCH" || requestParams.Method == "EVAL")
				{
                    httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Content-Encoding", "gzip");
				}

                httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Accept-Encoding", "gzip");
			}

			httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json") { CharSet = "utf-8" });
			headers.Add("Raven-Client-Version", ClientVersion);
			requestParams.UpdateHeaders(httpClient.DefaultRequestHeaders);
		}

		public HttpRequestHeaders DefaultRequestHeaders
		{
			get { return httpClient.DefaultRequestHeaders; }
		}

		public void DisableAuthentication()
		{
			handler.Credentials = null;
			handler.UseDefaultCredentials = false;
			disabledAuthRetries = true;
		}

		public Task ExecuteRequestAsync()
		{
			return ReadResponseJsonAsync();
		}

		/// <summary>
		/// Begins the read response string.
		/// </summary>
		public async Task<RavenJToken> ReadResponseJsonAsync()
		{
			if (SkipServerCheck)
			{
				var result = factory.GetCachedResponse(this);
				factory.InvokeLogRequest(owner, () => new RequestResultArgs
				{
					DurationMilliseconds = CalculateDuration(),
					Method = Method,
					HttpResult = (int)ResponseStatusCode,
					Status = RequestStatus.AggressivelyCached,
					Result = result.ToString(),
					Url = Url,
					PostedData = postedData
				});
				return result;
			}

			int retries = 0;
			while (true)
			{
				ErrorResponseException webException;
				try
				{
					if (writeCalled == false)
					{
						try
						{
						    var httpRequestMessage = new HttpRequestMessage(new HttpMethod(Method), Url);
						    CopyHeadersToHttpRequestMessage(httpRequestMessage);
						    HttpResponseMessage httpResponseMessage = await httpClient.SendAsync(httpRequestMessage);
						    Response = httpResponseMessage;
							SetResponseHeaders(Response);

							ResponseStatusCode = Response.StatusCode;
						}
						finally
						{
							sp.Stop();
						}
						var result = await CheckForErrorsAndReturnCachedResultIfAnyAsync();
					    if (result != null)
					        return result;
					}
					return await ReadJsonInternalAsync();
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

				if (Response.StatusCode == HttpStatusCode.Forbidden)
				{
					await HandleForbiddenResponseAsync(Response);
					await new CompletedTask(webException).Task; // Throws, preserving original stack
				}

				if (!HandleUnauthorizedResponse(Response))
					await new CompletedTask(webException).Task; // Throws, preserving original stack
			}
		}

	    private void CopyHeadersToHttpRequestMessage(HttpRequestMessage httpRequestMessage)
	    {
	        for (int i = 0; i < headers.Count; i++)
	        {
	            var key = headers.GetKey(i);
	            var values = headers.GetValues(i);
	            Debug.Assert(values != null);
	            httpRequestMessage.Headers.Add(key, values);
	        }
	    }

	    private void SetResponseHeaders(HttpResponseMessage response)
		{
			ResponseHeaders = new NameValueCollection();
			foreach (var header in response.Headers)
			{
				foreach (var val in header.Value)
				{
					ResponseHeaders.Add(header.Key, val);
				}
			}
		}

		private async Task<RavenJToken> CheckForErrorsAndReturnCachedResultIfAnyAsync()
		{
			if (Response.IsSuccessStatusCode == false)
			{
				if (Response.StatusCode == HttpStatusCode.Unauthorized ||
					Response.StatusCode == HttpStatusCode.NotFound ||
					Response.StatusCode == HttpStatusCode.Conflict)
				{
					factory.InvokeLogRequest(owner, () => new RequestResultArgs
					{
						DurationMilliseconds = CalculateDuration(),
						Method = Method,
						HttpResult = (int)Response.StatusCode,
						Status = RequestStatus.ErrorOnServer,
						Result = Response.StatusCode.ToString(),
						Url = Url,
						PostedData = postedData
					});

					throw new ErrorResponseException(Response);
				}

				if (Response.StatusCode == HttpStatusCode.NotModified
					&& CachedRequestDetails != null)
				{
					factory.UpdateCacheTime(this);
					var result = factory.GetCachedResponse(this, ResponseHeaders);

                    // here we explicitly need to get Response.Headers, and NOT ResponseHeaders because we are 
                    // getting the value _right now_ from the secondary, and don't care about the 304, the force check
                    // is still valid
					HandleReplicationStatusChanges(Response.Headers.GetFirstValue(Constants.RavenForcePrimaryServerCheck), primaryUrl, operationUrl);

					factory.InvokeLogRequest(owner, () => new RequestResultArgs
					{
						DurationMilliseconds = CalculateDuration(),
						Method = Method,
						HttpResult = (int)Response.StatusCode,
						Status = RequestStatus.Cached,
						Result = result.ToString(),
						Url = Url,
						PostedData = postedData
					});

					return result;
				}


				using (var sr = new StreamReader(await Response.GetResponseStreamWithHttpDecompression()))
				{
					var readToEnd = sr.ReadToEnd();

					factory.InvokeLogRequest(owner, () => new RequestResultArgs
					{
						DurationMilliseconds = CalculateDuration(),
						Method = Method,
						HttpResult = (int)Response.StatusCode,
						Status = RequestStatus.Cached,
						Result = readToEnd,
						Url = Url,
						PostedData = postedData
					});

					if (string.IsNullOrWhiteSpace(readToEnd))
						throw new ErrorResponseException(Response);

					RavenJObject ravenJObject;
					try
					{
						ravenJObject = RavenJObject.Parse(readToEnd);
					}
					catch (Exception e)
					{
                        throw new ErrorResponseException(Response, readToEnd, e);
					}
					if (ravenJObject.ContainsKey("IndexDefinitionProperty"))
					{
						throw new IndexCompilationException(ravenJObject.Value<string>("Message"))
						{
							IndexDefinitionProperty = ravenJObject.Value<string>("IndexDefinitionProperty"),
							ProblematicText = ravenJObject.Value<string>("ProblematicText")
						};
					}
					if (Response.StatusCode == HttpStatusCode.BadRequest && ravenJObject.ContainsKey("Message"))
					{
						throw new BadRequestException(ravenJObject.Value<string>("Message"), new ErrorResponseException(Response));
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

						if (sb.Length > 0)
							sb.AppendLine();
						sb.Append(ravenJObject.Value<string>("Error"));

						throw new ErrorResponseException(Response, sb.ToString());
					}
					throw new ErrorResponseException(Response, readToEnd);
				}
			}
		    return null;
		}

		public async Task<byte[]> ReadResponseBytesAsync()
		{
			var httpRequestMessage = new HttpRequestMessage(new HttpMethod(Method), Url);
			CopyHeadersToHttpRequestMessage(httpRequestMessage);
			Response = await httpClient.SendAsync(httpRequestMessage);
		    await CheckForErrorsAndReturnCachedResultIfAnyAsync();
			using (var stream = await Response.GetResponseStreamWithHttpDecompression())
			{
				SetResponseHeaders(Response);
				return await stream.ReadDataAsync();
			}
		}

		public void ExecuteRequest()
		{
			ReadResponseJsonAsync().WaitUnwrap();
		}

		/// <summary>
		/// Reads the response string.
		/// </summary>
		/// <returns></returns>
		public RavenJToken ReadResponseJson()
		{
		    return ReadResponseJsonAsync().ResultUnwrap();
		}

	    private bool HandleUnauthorizedResponse(HttpResponseMessage unauthorizedResponse)
		{
			if (conventions.HandleUnauthorizedResponseAsync == null)
				return false;

			var unauthorizedResponseAsync = conventions.HandleUnauthorizedResponseAsync(unauthorizedResponse);
			if (unauthorizedResponseAsync == null)
				return false;

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


		private async Task<RavenJToken> ReadJsonInternalAsync()
		{
			HandleReplicationStatusChanges(ResponseHeaders.Get(Constants.RavenForcePrimaryServerCheck), primaryUrl, operationUrl);

			using (var responseStream = await Response.GetResponseStreamWithHttpDecompression())
			{
				var data = await RavenJToken.TryLoadAsync(responseStream);

				if (Method == "GET" && ShouldCacheRequest)
				{
					factory.CacheResponse(Url, data, ResponseHeaders);
				}

				factory.InvokeLogRequest(owner, () => new RequestResultArgs
				{
					DurationMilliseconds = CalculateDuration(),
					Method = Method,
					HttpResult = (int) ResponseStatusCode,
					Status = RequestStatus.SentToServer,
					Result = (data ?? "").ToString(),
					Url = Url,
					PostedData = postedData
				});

				return data;
			}
		}

	    /// <summary>
	    /// Adds the operation headers.
	    /// </summary>
	    /// <param name="operationsHeaders">The operations headers.</param>
	    public void AddOperationHeaders(NameValueCollection operationsHeaders)
	    {
	        headers.Add(operationsHeaders);
	    }

	    /// <summary>
		/// Adds the operation header.
		/// </summary>
		public void AddOperationHeader(string key, string value)
	    {
	        headers[key] = value;
	    }

	    public HttpJsonRequest AddReplicationStatusHeaders(string thePrimaryUrl, string currentUrl,
            ReplicationInformer replicationInformer, FailoverBehavior failoverBehavior,
            Action<string, string, string> handleReplicationStatusChanges)
		{
			if (thePrimaryUrl.Equals(currentUrl, StringComparison.OrdinalIgnoreCase))
				return this;
			if (replicationInformer.GetFailureCount(thePrimaryUrl) <= 0)
				return this; // not because of failover, no need to do this.

			var lastPrimaryCheck = replicationInformer.GetFailureLastCheck(thePrimaryUrl);
			headers.Set(Constants.RavenClientPrimaryServerUrl, ToRemoteUrl(thePrimaryUrl));
			headers.Set(Constants.RavenClientPrimaryServerLastCheck, lastPrimaryCheck.ToString("s"));

			primaryUrl = thePrimaryUrl;
			operationUrl = currentUrl;

			HandleReplicationStatusChanges = handleReplicationStatusChanges;

			return this;
		}

		private static string ToRemoteUrl(string primaryUrl)
		{
			var uriBuilder = new UriBuilder(primaryUrl);
			if (uriBuilder.Host == "localhost" || uriBuilder.Host == "127.0.0.1")
				uriBuilder.Host = Environment.MachineName;
			return uriBuilder.Uri.ToString();
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

        public TimeSpan Timeout
		{
			set { handler.ReadWriteTimeout = (int) value.TotalMilliseconds; }
		}

		public HttpResponseMessage Response { get; private set; }

		/// <summary>
		/// Writes the specified data.
		/// </summary>
		/// <param name="data">The data.</param>
		public void Write(string data)
		{
			throw new NotSupportedException("TO BE REMOVED");
			/*writeCalled = true;
			postedData = data;
			HttpRequestHelper.WriteDataToRequest(webRequest, data, factory.DisableRequestCompression);*/
		}

		public async Task<IObservable<string>> ServerPullAsync()
		{
			int retries = 0;
			while (true)
			{
				ErrorResponseException webException;

				try
				{
					Response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(Method), Url), HttpCompletionOption.ResponseHeadersRead);
					await CheckForErrorsAndReturnCachedResultIfAnyAsync();

					var stream = await Response.GetResponseStreamWithHttpDecompression();
					var observableLineStream = new ObservableLineStream(stream, () => Response.Dispose());
					SetResponseHeaders(Response);
					observableLineStream.Start();
					return (IObservable<string>) observableLineStream;
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

				if (!HandleUnauthorizedResponse(webException.Response))
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

		private async Task WriteAsync(Stream streamToWrite)
		{
			postedStream = streamToWrite;

				Response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(Method), Url)
				{
				Content = new CompressedStreamContent(postedStream)
				});

				if (Response.IsSuccessStatusCode == false)
					throw new ErrorResponseException(Response);

				SetResponseHeaders(Response);
			}

		public async Task WriteAsync(string data)
		{
			writeCalled = true;
			Response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod(Method), Url)
			{
				Content = new CompressedStringContent(data, factory.DisableRequestCompression),
			});

			if (Response.IsSuccessStatusCode == false)
				throw new ErrorResponseException(Response);

			SetResponseHeaders(Response);
		}

		public async Task<Stream> GetRawRequestStream()
		{
			var httpRequestMessage = new HttpRequestMessage(new HttpMethod(Method), Url);
			CopyHeadersToHttpRequestMessage(httpRequestMessage);
			HttpResponseMessage response = await httpClient.SendAsync(httpRequestMessage);
			return await response.Content.ReadAsStreamAsync();

			//TODO DH no comparable property on httpclient
			// webRequest.SendChunked = true;
		}

		public async Task<HttpResponseMessage> RawExecuteRequestAsync()
		{
			/*try
			{*/
			var httpRequestMessage = new HttpRequestMessage(new HttpMethod(Method), Url);
			CopyHeadersToHttpRequestMessage(httpRequestMessage);
			return await httpClient.SendAsync(httpRequestMessage);
			// TODO DH do we need this catch?
			/*}
			catch (WebException we)
			{
				var httpWebResponse = we.Response as HttpWebResponse;
				if (httpWebResponse == null)
					throw;
				var sb = new StringBuilder()
					.Append(httpWebResponse.StatusCode)
					.Append(" ")
					.Append(httpWebResponse.StatusDescription)
					.AppendLine();

				using (var reader = new StreamReader(httpWebResponse.GetResponseStreamWithHttpDecompression()))
				{
					string line;
					while ((line = reader.ReadLine()) != null)
					{
						sb.AppendLine(line);
					}
				}
				throw new InvalidOperationException(sb.ToString(), we);
			}*/
		}

		public void PrepareForLongRequest()
		{
			Timeout = TimeSpan.FromHours(6);
			//TODO DH no comparable property on httpclient
			//webRequest.AllowWriteStreamBuffering = false;
		}

	    public void AddHeader(string key, string val)
	    {
	        headers.Set(key, val);
	    }
	}
}
#endif
