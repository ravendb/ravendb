using System;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Runtime.Caching;
using System.Text;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;

namespace Raven.Client.Client
{
	/// <summary>
	/// A representation of an HTTP json request to the RavenDB server
	/// </summary>
	public class HttpJsonRequest
	{
		private readonly string url;
		private readonly string method;
	    private readonly bool cacheRequest;

	    /// <summary>
		/// Occurs when a json request is created
		/// </summary>
		public static event EventHandler<WebRequestEventArgs> ConfigureRequest = delegate {  };

		private static ObjectCache cache = new MemoryCache(typeof(HttpJsonRequest).FullName + ".Cache");

	    private static int numOfCachedRequests;

        /// <summary>
        /// The number of requests that we got 304 for 
        /// and were able to handle purely from the cache
        /// </summary>
	    public static int NumberOfCachedRequests
	    {
	        get { return numOfCachedRequests; }
	    }

	    private class CachedRequest
		{
			public string Etag;
			public string Data;
			public string LastModified;
		}

		private byte[] bytesForNextWrite;

	    /// <summary>
	    /// Creates the HTTP json request.
	    /// </summary>
	    /// <param name="self">The self.</param>
	    /// <param name="url">The URL.</param>
	    /// <param name="method">The method.</param>
	    /// <param name="credentials">The credentials.</param>
	    /// <param name="convention">The document conventions governing this request</param>
	    /// <returns></returns>
	    public static HttpJsonRequest CreateHttpJsonRequest(object self, string url, string method, ICredentials credentials, DocumentConvention convention)
		{
			var request = new HttpJsonRequest(url, method, credentials, convention.ShouldCacheRequest(url));
			ConfigureRequest(self, new WebRequestEventArgs { Request = request.webRequest });
			return request;
		}

		/// <summary>
		/// Creates the HTTP json request.
		/// </summary>
		/// <param name="self">The self.</param>
		/// <param name="url">The URL.</param>
		/// <param name="method">The method.</param>
		/// <param name="metadata">The metadata.</param>
		/// <param name="credentials">The credentials.</param>
        /// <param name="convention">The document conventions governing this request</param>
        /// <returns></returns>
		public static HttpJsonRequest CreateHttpJsonRequest(object self, string url, string method, JObject metadata, ICredentials credentials, DocumentConvention convention)
		{
			var request = new HttpJsonRequest(url, method, metadata, credentials, convention.ShouldCacheRequest(url));
			ConfigureRequest(self, new WebRequestEventArgs { Request = request.webRequest });
			return request;
		}

		private readonly WebRequest webRequest;
		// temporary create a strong reference to the cached data for this request
		// avoid the potential for clearing the cache from a cached item
		private readonly CachedRequest cachedRequest;

		/// <summary>
		/// Gets or sets the response headers.
		/// </summary>
		/// <value>The response headers.</value>
		public NameValueCollection ResponseHeaders { get; set; }

		private HttpJsonRequest(string url, string method, ICredentials credentials, bool cacheRequest)
			: this(url, method, new JObject(), credentials,cacheRequest)
		{
		}

		private HttpJsonRequest(string url, string method, JObject metadata, ICredentials credentials, bool cacheRequest)
		{
			this.url = url;
			this.method = method;
		    this.cacheRequest = cacheRequest;
		    webRequest = WebRequest.Create(url);
			webRequest.Credentials = credentials;
			WriteMetadata(metadata);
			webRequest.Method = method;
			webRequest.Headers["Accept-Encoding"] = "deflate,gzip";
			webRequest.ContentType = "application/json; charset=utf-8";

			if (cacheRequest == false ||
                method != "GET")
				return;

			cachedRequest = (CachedRequest)cache.Get(url);
			if (cachedRequest == null)
				return;

			webRequest.Headers["If-None-Match"] = cachedRequest.Etag;
		}

		/// <summary>
		/// Begins the read response string.
		/// </summary>
		/// <param name="callback">The callback.</param>
		/// <param name="state">The state.</param>
		/// <returns></returns>
		public IAsyncResult BeginReadResponseString(AsyncCallback callback, object state)
		{
			return webRequest.BeginGetResponse(callback, state);
		}

		/// <summary>
		/// Ends the reading of the response string.
		/// </summary>
		/// <param name="result">The result.</param>
		/// <returns></returns>
		public string EndReadResponseString(IAsyncResult result)
		{
			return ReadStringInternal(() => webRequest.EndGetResponse(result));
		}

		/// <summary>
		/// Reads the response string.
		/// </summary>
		/// <returns></returns>
		public string ReadResponseString()
		{
			return ReadStringInternal(webRequest.GetResponse);
		}

		private string ReadStringInternal(Func<WebResponse> getResponse)
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

				if (httpWebResponse.StatusCode == HttpStatusCode.NotModified 
					&& cachedRequest != null)
				{
					ResponseStatusCode = HttpStatusCode.NotModified;
					ResponseHeaders = new NameValueCollection
					{
						{"ETag", cachedRequest.Etag},
						{"Last-Modified", cachedRequest.LastModified}
					};
				    Interlocked.Increment(ref numOfCachedRequests);
					return cachedRequest.Data;
				}

				using (var sr = new StreamReader(e.Response.GetResponseStreamWithHttpDecompression()))
				{
					throw new InvalidOperationException(sr.ReadToEnd(), e);
				}
			}
			
			ResponseHeaders = response.Headers;
			ResponseStatusCode = ((HttpWebResponse) response).StatusCode;
			using (var responseStream = response.GetResponseStreamWithHttpDecompression())
			{
				var reader = new StreamReader(responseStream);
				var text = reader.ReadToEnd();
				reader.Close();
				if (method == "GET" && cacheRequest)
				{
					cache.Add(url, new CachedRequest
					{
						Data = text,
						LastModified = response.Headers["Last-Modified"],
						Etag = response.Headers["ETag"]
					}, new CacheItemPolicy() );// cache as much as possible, for as long as possible, using the default cache limits
				}
				return text;
			}
		}


		/// <summary>
		/// Gets or sets the response status code.
		/// </summary>
		/// <value>The response status code.</value>
		public HttpStatusCode ResponseStatusCode { get; set; }

		private void WriteMetadata(JObject metadata)
		{
			if (metadata == null || metadata.Count == 0)
			{
				webRequest.ContentLength = 0;
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
					headerName = "If-Match";
				var value = prop.Value.Value<object>().ToString();
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
		/// Writes the specified data.
		/// </summary>
		/// <param name="data">The data.</param>
		public void Write(string data)
		{
			var byteArray = Encoding.UTF8.GetBytes(data);

			Write(byteArray);
		}

		/// <summary>
		/// Writes the specified byte array.
		/// </summary>
		/// <param name="byteArray">The byte array.</param>
		public void Write(byte[] byteArray)
		{
			webRequest.ContentLength = byteArray.Length;

			using (var dataStream = webRequest.GetRequestStream())
			{
				dataStream.Write(byteArray, 0, byteArray.Length);
				dataStream.Close();
			}
		}

		/// <summary>
		/// Begins the write operation
		/// </summary>
		/// <param name="byteArray">The byte array.</param>
		/// <param name="callback">The callback.</param>
		/// <param name="state">The state.</param>
		/// <returns></returns>
		public IAsyncResult BeginWrite(byte[] byteArray, AsyncCallback callback, object state)
		{
			bytesForNextWrite = byteArray;
			webRequest.ContentLength = byteArray.Length;
			return webRequest.BeginGetRequestStream(callback, state);
		}

		/// <summary>
		/// Ends the write operation.
		/// </summary>
		/// <param name="result">The result.</param>
		public void EndWrite(IAsyncResult result)
		{
			using (var dataStream = webRequest.EndGetRequestStream(result))
			{
				dataStream.Write(bytesForNextWrite, 0, bytesForNextWrite.Length);
				dataStream.Close();
			}
			bytesForNextWrite = null;
		}

		/// <summary>
		/// Adds the operation headers.
		/// </summary>
		/// <param name="operationsHeaders">The operations headers.</param>
		public void AddOperationHeaders(NameValueCollection operationsHeaders)
		{
			foreach (string header in operationsHeaders)
			{
				webRequest.Headers[header] = operationsHeaders[header];
			}
		}

        /// <summary>
        /// Reset the number of cached requests and clear the entire cache
        /// Mostly used for testing
        /// </summary>
	    public static void ResetCache()
	    {
            cache = new MemoryCache(typeof(HttpJsonRequest).FullName + ".Cache");
	        numOfCachedRequests = 0;
	    }
	}
}
