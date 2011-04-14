using System;
using System.Net;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Client.Silverlight.Connection
{
	///<summary>
	/// Create the HTTP Json Requests to the RavenDB Server
	/// and manages the http cache
	///</summary>
	public class HttpJsonRequestFactory : IDisposable
	{
		/// <summary>
		/// Occurs when a json request is created
		/// </summary>
		public event EventHandler<WebRequestEventArgs> ConfigureRequest = delegate { };

		/// <summary>
		/// Creates the HTTP json request.
		/// </summary>
		/// <param name="self">The self.</param>
		/// <param name="url">The URL.</param>
		/// <param name="method">The method.</param>
		/// <param name="credentials">The credentials.</param>
		/// <param name="convention">The document conventions governing this request</param>
		/// <returns></returns>
		public HttpJsonRequest CreateHttpJsonRequest(object self, string url, string method, ICredentials credentials, DocumentConvention convention)
		{
			return CreateHttpJsonRequest(self, url, method, new RavenJObject(), credentials, convention);
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
		public HttpJsonRequest CreateHttpJsonRequest(object self, string url, string method, RavenJObject metadata, ICredentials credentials, DocumentConvention convention)
		{
			var request = new HttpJsonRequest(url, method, metadata);
			ConfigureRequest(self, new WebRequestEventArgs { Request = request.WebRequest });
			return request;
		}

		public void Dispose()
		{
		}
	}
}