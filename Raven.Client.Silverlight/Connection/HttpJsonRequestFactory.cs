using System;
using System.Collections.Specialized;
using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
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
		/// will instruct RavenDB to make unsecured calls (usually only good for testing / internal networks).
		/// </summary>
		public bool EnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers { get; set; }

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
		internal void InvokeLogRequest(IHoldProfilingInformation sender, Func<RequestResultArgs> generateRequestResult)
		{
			var handler = LogRequest;
			if (handler != null)
				handler(sender, generateRequestResult.Invoke());
		}

		/// <summary>
		/// Determine whether to use compression or not 
		/// </summary>
		public bool DisableRequestCompression { get; set; }
		public bool DisableHttpCaching
		{
			get { return false; }
		}

		public void Dispose()
		{
		}

		public HttpJsonRequest CreateHttpJsonRequest(CreateHttpJsonRequestParams createHttpJsonRequestParams)
		{
			var request = new HttpJsonRequest(createHttpJsonRequestParams, this);
			ConfigureRequest(createHttpJsonRequestParams.Owner, new WebRequestEventArgs { Client = request.httpClient, JsonRequest = request });
			return request;
		}

		public IDisposable DisableAllCaching()
		{
			return null;
		}

		internal CachedRequestOp ConfigureCaching(string url, Action<string, string> setHeader)
		{
			return new CachedRequestOp();
		}

		public void IncrementCachedRequests()
		{
		}

		public void CacheResponse(string s, RavenJToken result, NameValueCollection nameValueCollection)
		{

		}
	}
}