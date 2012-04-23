using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
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
		/// Advanced: Don't set this unless you know what you are doing!
		/// 
		/// Enable using basic authentication using http
		/// By default, RavenDB only allows basic authentication over HTTPS, setting this property to true
		/// will instruct RavenDB to make unsecure calls (usually only good for testing / internal networks).
		/// </summary>
		public bool EnableBasicAuthenticationOverUnsecureHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers { get; set; }

		/// <summary>
		/// Occurs when a json request is created
		/// </summary>
		public event EventHandler<WebRequestEventArgs> ConfigureRequest = delegate { };

		/// <summary>
		/// Occurs when a json request is completed
		/// </summary>
		public event EventHandler<RequestResultArgs> LogRequest = delegate { };

		/// <summary>
		/// Invoke the LogRequest event
		/// </summary>
		internal void InvokeLogRequest(IHoldProfilingInformation sender, RequestResultArgs e)
		{
			var handler = LogRequest;
			if (handler != null)
				handler(sender, e);
		}

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
			var request = new HttpJsonRequest(url, method, metadata, convention, this);
			ConfigureRequest(self, new WebRequestEventArgs { Request = request.webRequest, JsonRequest = request });
			return request;
		}

		/// <summary>
		/// Determine whether to use compression or not 
		/// </summary>
		public bool DisableRequestCompression { get; set; }

		public void Dispose()
		{
		}
	}
}