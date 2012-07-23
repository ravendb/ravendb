using System;
using System.Net;
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
		/// <param name="metadata">The metadata.</param>
		/// <param name="credentials">The credentials.</param>
		/// <param name="convention">The document conventions governing this request</param>
		/// <returns></returns>
		public HttpJsonRequest CreateHttpJsonRequest(CreateHttpJsonRequestParams createHttpJsonRequestParams)
		{
			var request = new HttpJsonRequest(createHttpJsonRequestParams.Url, createHttpJsonRequestParams.Method, createHttpJsonRequestParams.Metadata, createHttpJsonRequestParams.Convention, this);
			ConfigureRequest(createHttpJsonRequestParams.Self, new WebRequestEventArgs { Request = request.webRequest, JsonRequest = request });
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

	public class CreateHttpJsonRequestParams
	{
		public CreateHttpJsonRequestParams(object self, string url, string method, ICredentials credentials, DocumentConvention convention)
		{
			Self = self;
			Url = url;
			Method = method;
			Credentials = credentials;
			Convention = convention;
		}

		public CreateHttpJsonRequestParams(object self, string url, string method, RavenJObject metadata, ICredentials credentials, DocumentConvention convention)
		{
			Self = self;
			Url = url;
			Method = method;
			Metadata = metadata;
			Credentials = credentials;
			Convention = convention;
		}

		public object Self { get; private set; }
		public string Url { get; private set; }
		public string Method { get; private set; }
		public RavenJObject Metadata { get; private set; }
		public ICredentials Credentials { get; private set; }
		public DocumentConvention Convention { get; private set; }
		public bool AvoidCachingRequest { get; set; }
	}

}
