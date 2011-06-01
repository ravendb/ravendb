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
using System.Text;
using Newtonsoft.Json.Linq;
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

		private byte[] bytesForNextWrite;


		internal readonly HttpWebRequest webRequest;
		// temporary create a strong reference to the cached data for this request
		// avoid the potential for clearing the cache from a cached item
		internal CachedRequest CachedRequestDetails;
		private readonly HttpJsonRequestFactory factory;
		internal bool ShouldCacheRequest;

		/// <summary>
		/// Gets or sets the response headers.
		/// </summary>
		/// <value>The response headers.</value>
		public NameValueCollection ResponseHeaders { get; set; }

		internal HttpJsonRequest(string url, string method, RavenJObject metadata, ICredentials credentials, HttpJsonRequestFactory factory)
		{
			this.Url = url;
			this.factory = factory;
			this.Method = method;
			webRequest = (HttpWebRequest)WebRequest.Create(url);
			webRequest.Credentials = credentials;
			WriteMetadata(metadata);
			webRequest.Method = method;
			webRequest.Headers["Accept-Encoding"] = "deflate,gzip";
			webRequest.ContentType = "application/json; charset=utf-8";
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
					&& CachedRequestDetails != null)
				{
					return factory.GetCachedResponse(this);
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
				factory.CacheResponse(response, text, this);
				return text;
			}
		}

		/// <summary>
		/// Gets or sets the response status code.
		/// </summary>
		/// <value>The response status code.</value>
		public HttpStatusCode ResponseStatusCode { get; set; }

		private void WriteMetadata(RavenJObject metadata)
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

				// Restricted headers require their own special treatment, otherwise an exception will
				// be thrown.
				// See http://msdn.microsoft.com/en-us/library/78h415ay.aspx
				if (WebHeaderCollection.IsRestricted(headerName))
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
				} else {
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
		/// Adds the operation headers.
		/// </summary>
		/// <param name="operationsHeaders">The operations headers.</param>
		public void AddOperationHeaders(IDictionary<string, string> operationsHeaders)
		{
			foreach (var kvp in operationsHeaders)
			{
				webRequest.Headers[kvp.Key] = operationsHeaders[kvp.Value];
			}
		}
	}
}
