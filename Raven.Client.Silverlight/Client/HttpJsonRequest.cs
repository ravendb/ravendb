//-----------------------------------------------------------------------
// <copyright file="HttpJsonRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Browser;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;

namespace Raven.Client.Client
{
	/// <summary>
	/// A representation of an HTTP json request to the RavenDB server
	/// Since we are using the ClientHttp stack for Silverlight, we don't need to implement
	/// caching, it is already implemented for us.
	/// Note: that the RavenDB server generate both an ETag and an Expires header to ensure proper
	/// Note: behavior from the silverlight http stack
	/// </summary>
	public class HttpJsonRequest
	{
		/// <summary>
		/// Occurs when a json request is created
		/// </summary>
		public static event EventHandler<WebRequestEventArgs> ConfigureRequest = delegate {  };

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
			var request = new HttpJsonRequest(url, method, new JObject());
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
			var request = new HttpJsonRequest(url, method, metadata);
			ConfigureRequest(self, new WebRequestEventArgs { Request = request.webRequest });
			return request;
		}

		private readonly WebRequest webRequest;
		/// <summary>
		/// Gets or sets the response headers.
		/// </summary>
		/// <value>The response headers.</value>
		public IDictionary<string, IList<string>> ResponseHeaders { get; set; }

	

		private HttpJsonRequest(string url, string method, JObject metadata)
		{
			webRequest = WebRequestCreator.ClientHttp.Create(new Uri(url));

			WriteMetadata(metadata);
			webRequest.Method = method;
			if(method != "GET")
				webRequest.ContentType = "application/json; charset=utf-8";
		}

		/// <summary>
		/// Begins the read response string.
		/// </summary>
		/// <param name="callback">The callback.</param>
		/// <param name="state">The state.</param>
		/// <returns></returns>
		public Task<string> ReadResponseStringAsync()
		{
			return webRequest
				.GetResponseAsync()
				.ContinueWith(t => ReadStringInternal(() => t.Result));
		}

		public Task<byte[]> ReadResponseBytesAsync()
		{
			return webRequest
				.GetResponseAsync()
				.ContinueWith(t => ReadResponse(() => t.Result,ConvertStreamToBytes));
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

		/// <summary>
		/// Ends the reading of the response string.
		/// </summary>
		/// <param name="result">The result.</param>
		/// <returns></returns>
		public string EndReadResponseString(IAsyncResult result)
		{
			return ReadStringInternal(() => webRequest.EndGetResponse(result));
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

				using (var sr = new StreamReader(e.Response.GetResponseStream()))
				{
					throw new InvalidOperationException(sr.ReadToEnd(), e);
				}
			}
			
			ResponseHeaders = response.Headers.AllKeys.ToDictionary(key => key, key => (IList<string>)new List<string> { response.Headers[key] });
			ResponseStatusCode = ((HttpWebResponse) response).StatusCode;

			using (var responseStream = response.GetResponseStream())
			{
				var reader = new StreamReader(responseStream);
				var text = reader.ReadToEnd();
				return text;
			}
		}

		private T ReadResponse<T>(Func<WebResponse> getResponse, Func<Stream,T> handleResponse)
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

			ResponseHeaders = response.Headers.AllKeys
					.ToDictionary(key => key, key => (IList<string>)new List<string> { response.Headers[key] });
			ResponseStatusCode = ((HttpWebResponse)response).StatusCode;

			using (var responseStream = response.GetResponseStream())
			{
				return handleResponse(responseStream);
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
		/// Begins the write operation
		/// </summary>
		/// <param name="byteArray">The byte array.</param>
		/// <param name="callback">The callback.</param>
		/// <param name="state">The state.</param>
		/// <returns></returns>
		public Task WriteAsync(byte[] byteArray)
		{
			return webRequest.GetRequestStreamAsync().ContinueWith(t =>
																	   {
																		   var dataStream = t.Result;
																		   using (dataStream)
																		   {
                                                                               dataStream.Write(byteArray, 0, byteArray.Length);
																			   dataStream.Close();
																		   }
																	   });
		}

		/// <summary>
		/// Adds the operation headers.
		/// </summary>
		/// <param name="operationsHeaders">The operations headers.</param>
		public void AddOperationHeaders(IDictionary<string, string> operationsHeaders)
		{
			foreach (var header in operationsHeaders)
			{
				webRequest.Headers[header.Key] = header.Value;
			}
		}
	}
}
